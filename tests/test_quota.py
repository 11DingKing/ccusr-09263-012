"""机构月度名额配额：按月/机构计算、释放补位、并发不超卖。

并发用例对“仅剩一个名额”的同一配额同时发起两个领取请求，
SQLite 后端以 ``BEGIN IMMEDIATE`` 原子事务串行化，断言恰好一个成功、
另一个进入候补且绝不超卖。
"""
from __future__ import annotations

import tempfile
import threading
import unittest
import urllib.error
import urllib.request
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from urllib.parse import quote

from service_09252_008.application.ports import ManualClock, SequentialIdGenerator, UuidIdGenerator
from service_09252_008.application.quota_service import QuotaService
from service_09252_008.domain.errors import BusinessRuleError, ConflictError, NotFoundError, StateError, ValidationError
from service_09252_008.interfaces.http_api import create_server
from service_09252_008.persistence.sqlite_store import SQLiteStore
from service_09252_008.persistence.store import InMemoryStore
from tests.helpers import make_services, seed_catalog

MONTH = "2026-10"
INST = "城南大学"


def make_quota_service(store=None, *, now=None):
    store = store or InMemoryStore()
    clock = ManualClock(now or datetime(2026, 9, 25, tzinfo=timezone.utc))
    ids = SequentialIdGenerator()
    return QuotaService(store, clock, ids), store, clock, ids


class QuotaBasicTests(unittest.TestCase):
    def setUp(self) -> None:
        self.quotas, self.store, _, _ = make_quota_service()
        self.quotas.set_quota({"institution": INST, "month": MONTH, "total": 2})

    def test_quota_is_keyed_by_month_and_institution(self) -> None:
        # 不同机构、不同月份各自独立计算
        self.quotas.set_quota({"institution": "城北学院", "month": MONTH, "total": 5})
        self.quotas.set_quota({"institution": INST, "month": "2026-11", "total": 1})

        first = self.quotas.claim({"institution": INST, "month": MONTH, "applicant_id": "a1"})
        self.assertEqual(first["status"], "HELD")
        self.assertEqual(first["remaining"], 1)  # Python API 返回扣减后的剩余名额

        view = self.quotas.get_quota(INST, MONTH)
        self.assertEqual((view["total"], view["used"], view["remaining"]), (2, 1, 1))

        other_inst = self.quotas.get_quota("城北学院", MONTH)
        self.assertEqual(other_inst["remaining"], 5)
        other_month = self.quotas.get_quota(INST, "2026-11")
        self.assertEqual(other_month["remaining"], 1)

    def test_exhaustion_waitlists_then_release_promotes_in_original_order(self) -> None:
        c1 = self.quotas.claim({"institution": INST, "month": MONTH, "applicant_id": "a1"})
        c2 = self.quotas.claim({"institution": INST, "month": MONTH, "applicant_id": "a2"})
        self.assertEqual([c1["status"], c2["status"]], ["HELD", "HELD"])

        # 名额已满：后到者进入候补并保留原排序
        w3 = self.quotas.claim({"institution": INST, "month": MONTH, "applicant_id": "a3"})
        w4 = self.quotas.claim({"institution": INST, "month": MONTH, "applicant_id": "a4"})
        self.assertEqual([w3["status"], w4["status"]], ["WAITING", "WAITING"])
        self.assertEqual([w3["position"], w4["position"]], [3, 4])
        self.assertEqual(self.quotas.get_quota(INST, MONTH)["remaining"], 0)

        # 释放 position=2 的名额：候补者按原排序，a3 先补位，a4 继续候补
        result = self.quotas.release(c2["claim_id"], {"reason": "取消"})
        self.assertEqual(result["remaining"], 0)
        self.assertEqual(result["promoted_claim_ids"], [w3["claim_id"]])
        self.assertEqual(self.quotas.get_claim(w3["claim_id"])["status"], "HELD")
        self.assertEqual(self.quotas.get_claim(w4["claim_id"])["status"], "WAITING")

        # 再释放一个：a4 补位
        result = self.quotas.release(c1["claim_id"])
        self.assertEqual(result["promoted_claim_ids"], [w4["claim_id"]])
        view = self.quotas.get_quota(INST, MONTH)
        self.assertEqual((view["used"], view["remaining"], view["waiting_claims"]), (2, 0, 0))

    def test_releasing_waiting_claim_does_not_free_a_slot(self) -> None:
        c1 = self.quotas.claim({"institution": INST, "month": MONTH, "applicant_id": "a1"})
        self.quotas.claim({"institution": INST, "month": MONTH, "applicant_id": "a2"})
        w3 = self.quotas.claim({"institution": INST, "month": MONTH, "applicant_id": "a3"})
        self.assertEqual(w3["status"], "WAITING")

        result = self.quotas.release(w3["claim_id"])
        self.assertEqual(result["promoted_claim_ids"], [])
        self.assertEqual(result["remaining"], 0)
        # 候补者退出后，其原排序位置不再占用
        self.assertEqual(self.quotas.get_quota(INST, MONTH)["waiting_claims"], 0)
        self.assertEqual(c1["status"], "HELD")

    def test_duplicate_claim_rejected(self) -> None:
        self.quotas.claim({"institution": INST, "month": MONTH, "applicant_id": "dup"})
        with self.assertRaises(ConflictError):
            self.quotas.claim({"institution": INST, "month": MONTH, "applicant_id": "dup"})

    def test_double_release_rejected(self) -> None:
        c1 = self.quotas.claim({"institution": INST, "month": MONTH, "applicant_id": "a1"})
        self.quotas.release(c1["claim_id"])
        with self.assertRaises(StateError):
            self.quotas.release(c1["claim_id"])

    def test_validation_and_missing_quota(self) -> None:
        with self.assertRaises(ValidationError):
            self.quotas.set_quota({"institution": INST, "month": "2026-13", "total": 1})
        with self.assertRaises(ValidationError):
            self.quotas.set_quota({"institution": INST, "month": MONTH, "total": -1})
        with self.assertRaises(NotFoundError):
            self.quotas.claim({"institution": "未配置机构", "month": MONTH, "applicant_id": "x"})

        held = self.quotas.claim({"institution": INST, "month": MONTH, "applicant_id": "a1"})
        # 已占用 1 个时不得把上限调到低于已占用数
        with self.assertRaises(BusinessRuleError):
            self.quotas.set_quota({"institution": INST, "month": MONTH, "total": 0})
        self.assertEqual(self.quotas.get_quota(INST, MONTH)["used"], 1)
        self.assertEqual(held["claim_id"][:4], "clm_")

    def test_increasing_quota_promotes_waitlist(self) -> None:
        self.quotas.claim({"institution": INST, "month": MONTH, "applicant_id": "a1"})
        self.quotas.claim({"institution": INST, "month": MONTH, "applicant_id": "a2"})
        w3 = self.quotas.claim({"institution": INST, "month": MONTH, "applicant_id": "a3"})
        self.assertEqual(w3["status"], "WAITING")

        view = self.quotas.set_quota({"institution": INST, "month": MONTH, "total": 3})
        self.assertEqual(view["promoted_claim_ids"], [w3["claim_id"]])
        self.assertEqual((view["used"], view["remaining"]), (3, 0))


class QuotaConcurrentSqliteTests(unittest.TestCase):
    """两个请求同时领取仅剩的一个名额：SQLite 原子事务保证不超卖。"""

    def test_two_concurrent_claims_for_one_slot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = SQLiteStore(f"{tmp}/quota.db")
            clock = ManualClock(datetime(2026, 9, 25, tzinfo=timezone.utc))
            ids = UuidIdGenerator()
            quotas = QuotaService(store, clock, ids)
            # 重复多轮以提高竞态暴露概率，每轮使用独立月份
            for round_no, month in enumerate(["2026-10", "2026-11", "2026-12", "2027-01", "2027-02"]):
                quotas.set_quota({"institution": INST, "month": month, "total": 1})

                outcomes: list[dict] = []
                barrier = threading.Barrier(2)

                def grab(applicant: str) -> dict:
                    barrier.wait()  # 尽量让两个请求同时进入事务
                    return quotas.claim(
                        {"institution": INST, "month": month, "applicant_id": f"{applicant}-{round_no}"}
                    )

                with ThreadPoolExecutor(max_workers=2) as pool:
                    for view in pool.map(grab, ["concurrent-a", "concurrent-b"]):
                        outcomes.append(view)

                statuses = sorted(o["status"] for o in outcomes)
                self.assertEqual(statuses, ["HELD", "WAITING"])  # 恰好一个成功
                view = quotas.get_quota(INST, month)
                self.assertEqual(view["total"], 1)
                self.assertEqual(view["used"], 1)  # 未超卖
                self.assertEqual(view["remaining"], 0)
                self.assertEqual(view["held_claims"], 1)
                self.assertEqual(view["waiting_claims"], 1)

                # 释放后唯一候补者补位
                held = next(o for o in outcomes if o["status"] == "HELD")
                result = quotas.release(held["claim_id"])
                self.assertEqual(len(result["promoted_claim_ids"]), 1)
                self.assertEqual(quotas.get_quota(INST, month)["used"], 1)
            store.close()


class QuotaHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        catalog, bookings, clock, store = make_services()
        seed_catalog(catalog)
        cls.quotas = QuotaService(store, clock, SequentialIdGenerator())
        cls.server = create_server("127.0.0.1", 0, catalog, bookings, cls.quotas)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=5)

    def _request(self, method: str, path: str, body=None):
        data = json.dumps(body).encode("utf-8") if body is not None else None
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}", data=data, method=method
        )
        if data is not None:
            request.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                return response.status, json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode("utf-8"))

    def test_quota_endpoints_and_concurrent_last_slot(self) -> None:
        status, view = self._request("POST", "/quotas", {"institution": INST, "month": MONTH, "total": 1})
        self.assertEqual(status, 200)
        self.assertEqual(view["remaining"], 1)

        results = []
        barrier = threading.Barrier(2)

        def grab(applicant):
            barrier.wait()
            return self._request(
                "POST", "/quota-claims", {"institution": INST, "month": MONTH, "applicant_id": applicant}
            )

        with ThreadPoolExecutor(max_workers=2) as pool:
            for outcome in pool.map(grab, ["http-a", "http-b"]):
                results.append(outcome)

        statuses = sorted(body["status"] for _, body in results)
        self.assertTrue(all(code == 200 for code, _ in results))
        self.assertEqual(statuses, ["HELD", "WAITING"])

        status, view = self._request("GET", f"/quotas/{MONTH}/{quote(INST)}")
        self.assertEqual(status, 200)
        self.assertEqual((view["used"], view["remaining"]), (1, 0))  # 不超卖

        status, month_view = self._request("GET", f"/quotas/{MONTH}")
        self.assertEqual(status, 200)
        self.assertEqual(len(month_view["items"]), 1)


if __name__ == "__main__":
    unittest.main()
