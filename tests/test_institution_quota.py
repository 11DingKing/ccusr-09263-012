"""机构月度名额配额：按月/机构计算、释放候补补位、并发不超卖。"""
from __future__ import annotations

import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

from service_09252_008.application.ports import ManualClock, SequentialIdGenerator, UuidIdGenerator
from service_09252_008.application.quota_service import QuotaService
from service_09252_008.domain.errors import ConflictError, NotFoundError, StateError, ValidationError
from service_09252_008.interfaces.http_api import create_server
from service_09252_008.persistence.sqlite_store import SQLiteStore
from service_09252_008.persistence.store import InMemoryStore
from tests.helpers import NOW


def make_quota_service(store=None, clock=None):
    store = store or InMemoryStore()
    clock = clock or ManualClock(NOW)
    return QuotaService(store, clock, SequentialIdGenerator()), store, clock


class QuotaBasicTests(unittest.TestCase):
    def test_claim_deducts_and_returns_remaining(self) -> None:
        quotas, _, _ = make_quota_service()
        view = quotas.set_quota({"institution": "城南大学", "month": "2026-09", "total": 3})
        self.assertEqual((view["total"], view["remaining"]), (3, 3))

        claim = quotas.claim({"institution": "城南大学", "month": "2026-09", "ref": "req-1"})
        self.assertEqual(claim["state"], "ACTIVE")
        self.assertEqual(claim["remaining"], 2)  # API 返回剩余名额

        claim2 = quotas.claim({"institution": "城南大学", "month": "2026-09", "ref": "req-2", "quantity": 2})
        self.assertEqual(claim2["state"], "ACTIVE")
        self.assertEqual(claim2["remaining"], 0)

        view = quotas.get_quota("城南大学", "2026-09")
        self.assertEqual((view["claimed"], view["remaining"], view["waiting_count"]), (3, 0, 0))

    def test_quotas_are_isolated_by_month_and_institution(self) -> None:
        quotas, _, _ = make_quota_service()
        quotas.set_quota({"institution": "甲校", "month": "2026-09", "total": 1})
        quotas.set_quota({"institution": "甲校", "month": "2026-10", "total": 1})
        quotas.set_quota({"institution": "乙校", "month": "2026-09", "total": 1})

        for institution, month in (("甲校", "2026-09"), ("甲校", "2026-10"), ("乙校", "2026-09")):
            claim = quotas.claim({"institution": institution, "month": month, "ref": f"{institution}-{month}"})
            self.assertEqual(claim["remaining"], 0)

        # 各自的余量互不影响
        self.assertEqual(quotas.get_quota("甲校", "2026-10")["remaining"], 0)
        self.assertEqual(quotas.get_quota("乙校", "2026-09")["remaining"], 0)

    def test_invalid_month_and_unknown_quota(self) -> None:
        quotas, _, _ = make_quota_service()
        with self.assertRaises(ValidationError):
            quotas.set_quota({"institution": "甲校", "month": "2026-9", "total": 1})
        with self.assertRaises(NotFoundError):
            quotas.claim({"institution": "甲校", "month": "2026-09", "ref": "x"})

    def test_duplicate_ref_conflicts(self) -> None:
        quotas, _, _ = make_quota_service()
        quotas.set_quota({"institution": "甲校", "month": "2026-09", "total": 5})
        quotas.claim({"institution": "甲校", "month": "2026-09", "ref": "dup"})
        with self.assertRaises(ConflictError):
            quotas.claim({"institution": "甲校", "month": "2026-09", "ref": "dup"})

    def test_shrink_total_below_claimed_rejected(self) -> None:
        quotas, _, _ = make_quota_service()
        quotas.set_quota({"institution": "甲校", "month": "2026-09", "total": 5})
        quotas.claim({"institution": "甲校", "month": "2026-09", "ref": "a", "quantity": 3})
        with self.assertRaises(ValidationError):
            quotas.set_quota({"institution": "甲校", "month": "2026-09", "total": 2})
        view = quotas.set_quota({"institution": "甲校", "month": "2026-09", "total": 4})
        self.assertEqual((view["total"], view["remaining"], view["claimed"]), (4, 1, 3))


class QuotaWaitlistTests(unittest.TestCase):
    def test_exhausted_claims_waitlist_in_arrival_order(self) -> None:
        quotas, _, _ = make_quota_service()
        quotas.set_quota({"institution": "甲校", "month": "2026-09", "total": 2})
        quotas.claim({"institution": "甲校", "month": "2026-09", "ref": "a"})
        quotas.claim({"institution": "甲校", "month": "2026-09", "ref": "b"})

        w1 = quotas.claim({"institution": "甲校", "month": "2026-09", "ref": "c"})
        w2 = quotas.claim({"institution": "甲校", "month": "2026-09", "ref": "d"})
        self.assertEqual((w1["state"], w1["position"], w1["remaining"]), ("WAITING", 1, 0))
        self.assertEqual((w2["state"], w2["position"]), ("WAITING", 2))

        view = quotas.get_quota("甲校", "2026-09")
        self.assertEqual([c["ref"] for c in view["waitlist"]], ["c", "d"])

    def test_release_promotes_waitlist_in_original_order(self) -> None:
        quotas, _, _ = make_quota_service()
        quotas.set_quota({"institution": "甲校", "month": "2026-09", "total": 1})
        first = quotas.claim({"institution": "甲校", "month": "2026-09", "ref": "a"})
        w1 = quotas.claim({"institution": "甲校", "month": "2026-09", "ref": "b"})
        w2 = quotas.claim({"institution": "甲校", "month": "2026-09", "ref": "c"})
        self.assertEqual([w1["state"], w2["state"]], ["WAITING", "WAITING"])

        result = quotas.release({"claim_id": first["claim_id"]})
        # 仅队首 b 补位；c 继续候补，余量仍为 0
        self.assertEqual([c["ref"] for c in result["promoted"]], ["b"])
        self.assertEqual(result["remaining"], 0)

        view = quotas.get_quota("甲校", "2026-09")
        self.assertEqual([c["ref"] for c in view["waitlist"]], ["c"])
        active_refs = [c["ref"] for c in view["active_claims"]]
        self.assertEqual(active_refs, ["b"])

    def test_head_of_line_blocks_larger_later_request(self) -> None:
        quotas, _, _ = make_quota_service()
        quotas.set_quota({"institution": "甲校", "month": "2026-09", "total": 2})
        quotas.claim({"institution": "甲校", "month": "2026-09", "ref": "a"})
        quotas.claim({"institution": "甲校", "month": "2026-09", "ref": "x"})
        # 队首需要 2 个，后者只需要 1 个
        quotas.claim({"institution": "甲校", "month": "2026-09", "ref": "big", "quantity": 2})
        quotas.claim({"institution": "甲校", "month": "2026-09", "ref": "small", "quantity": 1})

        # 只释放 1 个：队首要 2 个，不能补位；严格排序下后者也不得跳过
        result = quotas.release({"institution": "甲校", "month": "2026-09", "ref": "x"})
        self.assertEqual(result["promoted"], [])
        self.assertEqual(result["remaining"], 1)
        view = quotas.get_quota("甲校", "2026-09")
        self.assertEqual([c["ref"] for c in view["waitlist"]], ["big", "small"])

    def test_release_waiting_claim_rejected(self) -> None:
        quotas, _, _ = make_quota_service()
        quotas.set_quota({"institution": "甲校", "month": "2026-09", "total": 0})
        waiting = quotas.claim({"institution": "甲校", "month": "2026-09", "ref": "a"})
        with self.assertRaises(StateError):
            quotas.release({"claim_id": waiting["claim_id"]})


class QuotaConcurrencyTests(unittest.TestCase):
    def _race_two_claims(self, quotas: QuotaService) -> tuple[dict, dict]:
        quotas.set_quota({"institution": "甲校", "month": "2026-09", "total": 1})
        barrier = threading.Barrier(2)

        def do_claim(ref: str) -> dict:
            barrier.wait()
            return quotas.claim({"institution": "甲校", "month": "2026-09", "ref": ref})

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(do_claim, f"race-{i}") for i in range(2)]
            first, second = (f.result() for f in futures)
        return first, second

    def test_two_concurrent_claims_for_one_slot_memory(self) -> None:
        quotas, _, _ = make_quota_service()
        first, second = self._race_two_claims(quotas)
        states = sorted([first["state"], second["state"]])
        self.assertEqual(states, ["ACTIVE", "WAITING"])
        view = quotas.get_quota("甲校", "2026-09")
        self.assertEqual((view["remaining"], view["claimed"], view["waiting_count"]), (0, 1, 1))

    def test_two_concurrent_claims_for_one_slot_sqlite(self) -> None:
        # 关键并发证据：SQLite 原子事务（BEGIN IMMEDIATE）下两个请求同时领取一个名额
        with tempfile.TemporaryDirectory() as tmp:
            store = SQLiteStore(f"{tmp}/quota.db")
            try:
                quotas = QuotaService(store, ManualClock(NOW), UuidIdGenerator())
                first, second = self._race_two_claims(quotas)
                states = sorted([first["state"], second["state"]])
                self.assertEqual(states, ["ACTIVE", "WAITING"])
                # 两个请求各自看到的剩余名额都不可能为负：成功者 0，候补者 0
                self.assertEqual(first["remaining"], 0)
                self.assertEqual(second["remaining"], 0)

                # 直接核对持久化状态，杜绝超卖
                quota_rows = store.query("institution_quotas")
                self.assertEqual(len(quota_rows), 1)
                self.assertEqual(quota_rows[0]["remaining"], 0)
                claim_rows = store.query("institution_quota_claims")
                self.assertEqual(sum(c["quantity"] for c in claim_rows if c["state"] == "ACTIVE"), 1)
                self.assertEqual(sum(1 for c in claim_rows if c["state"] == "WAITING"), 1)

                # 释放唯一占用名额：候补者按原排序自动补位
                active = next(c for c in claim_rows if c["state"] == "ACTIVE")
                result = quotas.release({"claim_id": active["claim_id"]})
                self.assertEqual(len(result["promoted"]), 1)
                self.assertEqual(result["remaining"], 0)
            finally:
                store.close()


class QuotaHttpConcurrencyTests(unittest.TestCase):
    """两个 HTTP 请求同时领取最后一个名额（SQLite 后端）。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        store = SQLiteStore(f"{self._tmp.name}/quota.db")
        self.store = store
        clock = ManualClock(NOW)
        quotas = QuotaService(store, clock, UuidIdGenerator())
        # catalog/bookings 仅为满足 create_server 签名
        from service_09252_008.application.booking_service import BookingService
        from service_09252_008.application.catalog_service import CatalogService

        catalog = CatalogService(store, clock, UuidIdGenerator())
        bookings = BookingService(store, clock, UuidIdGenerator())
        self.server = create_server("127.0.0.1", 0, catalog, bookings, quotas)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        self.store.close()
        self._tmp.cleanup()

    def _post(self, path: str, body: dict) -> tuple[int, dict]:
        data = json.dumps(body).encode("utf-8")
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}",
            data=data,
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                return response.status, json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode("utf-8"))

    def _get(self, path: str) -> dict:
        with urllib.request.urlopen(f"http://127.0.0.1:{self.port}{path}", timeout=10) as response:
            return json.loads(response.read().decode("utf-8"))

    def test_two_http_requests_race_for_single_slot(self) -> None:
        status, body = self._post("/institutions/%E7%94%B2%E6%A0%A1/quotas/2026-09", {"total": 1})
        self.assertEqual(status, 200)
        self.assertEqual(body["remaining"], 1)

        barrier = threading.Barrier(2)

        def do_request(ref: str) -> tuple[int, dict]:
            barrier.wait()
            return self._post("/quota-claims", {"institution": "甲校", "month": "2026-09", "ref": ref})

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(do_request, f"http-race-{i}") for i in range(2)]
            responses = [f.result() for f in futures]

        self.assertTrue(all(status == 200 for status, _ in responses))
        states = sorted(body["state"] for _, body in responses)
        self.assertEqual(states, ["ACTIVE", "WAITING"])
        self.assertTrue(all(body["remaining"] == 0 for _, body in responses))

        view = self._get("/institutions/%E7%94%B2%E6%A0%A1/quotas/2026-09")
        self.assertEqual((view["total"], view["remaining"], view["claimed"], view["waiting_count"]), (1, 0, 1, 1))


if __name__ == "__main__":
    unittest.main()
