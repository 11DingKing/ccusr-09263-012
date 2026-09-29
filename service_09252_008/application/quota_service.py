"""机构月度名额配额服务。

配额按「机构 + 月份」分别计算，核心约定：

- ``claim`` 在单个 ``store.transaction()`` 内完成“读余量-判定-扣减”，
  内存后端由排他锁、SQLite 后端由 ``BEGIN IMMEDIATE`` 串行化，
  并发领取绝不会超卖；接口返回扣减后的剩余名额；
- 余量不足时领取请求不失败，而是按到达顺序进入该机构当月的候补队列；
- ``release`` 归还名额后，候补者严格按原排序（序号升序）依次补位，
  队首需求得不到完整满足时停止，不跳过队首。
"""
from __future__ import annotations

import re
from typing import Any

from ..domain.errors import ConflictError, NotFoundError, StateError, ValidationError
from ..domain.models import DomainEvent, dt_to_str
from ..persistence.store import Store
from .booking_service import COLLECTION_EVENTS
from .ports import Clock, IdGenerator

COLLECTION_QUOTAS = "institution_quotas"
COLLECTION_QUOTA_CLAIMS = "institution_quota_claims"

CLAIM_ACTIVE = "ACTIVE"  # 已占用名额
CLAIM_WAITING = "WAITING"  # 候补中
CLAIM_RELEASED = "RELEASED"  # 已释放（终态）

_MONTH_RE = re.compile(r"^(\d{4})-(0[1-9]|1[0-2])$")


class QuotaService:
    """机构月度名额配额用例：设置配额、领取、释放与候补补位。"""

    def __init__(self, store: Store, clock: Clock, ids: IdGenerator) -> None:
        self._store = store
        self._clock = clock
        self._ids = ids

    # ------------------------------------------------------------------
    # 辅助
    # ------------------------------------------------------------------

    def _emit(self, event_type: str, payload: dict[str, Any]) -> None:
        event = DomainEvent(
            event_id=self._ids.new_id("evt"),
            type=event_type,
            booking_id=None,
            payload=payload,
            created_at=self._clock.now(),
        )
        self._store.put(COLLECTION_EVENTS, event.event_id, event.to_dict())

    @staticmethod
    def _validate_month(month: Any) -> str:
        if not isinstance(month, str) or not _MONTH_RE.match(month):
            raise ValidationError("field month must be 'YYYY-MM'", details={"month": month})
        return month

    @staticmethod
    def _validate_institution(institution: Any) -> str:
        if not isinstance(institution, str) or not institution.strip():
            raise ValidationError("field institution must be a non-empty string")
        return institution.strip()

    def _quota_key(self, institution: str, month: str) -> str:
        return f"{institution}|{month}"

    def _load_quota(self, institution: str, month: str) -> dict[str, Any]:
        record = self._store.get(COLLECTION_QUOTAS, self._quota_key(institution, month))
        if record is None:
            raise NotFoundError(
                f"quota not configured: {institution} {month}",
                details={"institution": institution, "month": month},
            )
        return record

    def _save_quota(self, quota: dict[str, Any]) -> None:
        quota["updated_at"] = dt_to_str(self._clock.now())
        self._store.put(COLLECTION_QUOTAS, self._quota_key(quota["institution"], quota["month"]), quota)

    def _claims(self, institution: str, month: str) -> list[dict[str, Any]]:
        return [
            c
            for c in self._store.query(COLLECTION_QUOTA_CLAIMS, institution=institution)
            if c["month"] == month
        ]

    def _waiting_sorted(self, institution: str, month: str) -> list[dict[str, Any]]:
        waiting = [c for c in self._claims(institution, month) if c["state"] == CLAIM_WAITING]
        return sorted(waiting, key=lambda c: (c["seq"], c["claim_id"]))

    # ------------------------------------------------------------------
    # 用例
    # ------------------------------------------------------------------

    def set_quota(self, request: dict[str, Any]) -> dict[str, Any]:
        """设置（或调整）某机构某月的总名额。"""
        institution = self._validate_institution(request.get("institution"))
        month = self._validate_month(request.get("month"))
        total = request.get("total")
        if isinstance(total, bool) or not isinstance(total, int) or total < 0:
            raise ValidationError("field total must be a non-negative integer", details={"field": "total"})
        with self._store.transaction():
            existing = self._store.get(COLLECTION_QUOTAS, self._quota_key(institution, month))
            if existing is None:
                quota = {
                    "institution": institution,
                    "month": month,
                    "total": total,
                    "remaining": total,
                    "waitlist_seq": 0,
                    "created_at": dt_to_str(self._clock.now()),
                }
            else:
                claimed = sum(
                    c["quantity"] for c in self._claims(institution, month) if c["state"] == CLAIM_ACTIVE
                )
                if total < claimed:
                    raise ValidationError(
                        "total cannot be lower than already claimed quantity",
                        details={"total": total, "claimed": claimed},
                    )
                quota = existing
                quota["total"] = total
                quota["remaining"] = total - claimed
            self._save_quota(quota)
            self._emit("institution_quota_set", {"institution": institution, "month": month, "total": total})
            return self._quota_view(quota)

    def claim(self, request: dict[str, Any]) -> dict[str, Any]:
        """领取名额：余量充足则原子扣减，否则按序进入候补。返回剩余名额。"""
        institution = self._validate_institution(request.get("institution"))
        month = self._validate_month(request.get("month"))
        quantity = request.get("quantity", 1)
        if isinstance(quantity, bool) or not isinstance(quantity, int) or quantity < 1:
            raise ValidationError("field quantity must be a positive integer", details={"field": "quantity"})
        ref = request.get("ref")
        if not isinstance(ref, str) or not ref.strip():
            raise ValidationError("field ref must be a non-empty string", details={"field": "ref"})
        ref = ref.strip()

        with self._store.transaction():
            quota = self._load_quota(institution, month)
            # 同一业务引用不得重复领取（释放后可重新登记）
            duplicate = next(
                (
                    c
                    for c in self._claims(institution, month)
                    if c["ref"] == ref and c["state"] != CLAIM_RELEASED
                ),
                None,
            )
            if duplicate is not None:
                raise ConflictError(
                    "ref already has a claim for this institution/month",
                    details={"claim_id": duplicate["claim_id"], "ref": ref},
                )

            now = dt_to_str(self._clock.now())
            if quota["remaining"] >= quantity:
                quota["remaining"] -= quantity
                self._save_quota(quota)
                claim = {
                    "claim_id": self._ids.new_id("qcl"),
                    "institution": institution,
                    "month": month,
                    "ref": ref,
                    "quantity": quantity,
                    "state": CLAIM_ACTIVE,
                    "seq": None,
                    "created_at": now,
                    "updated_at": now,
                }
                self._store.put(COLLECTION_QUOTA_CLAIMS, claim["claim_id"], claim)
                self._emit(
                    "institution_quota_claimed",
                    {"institution": institution, "month": month, "quantity": quantity, "ref": ref},
                )
            else:
                quota["waitlist_seq"] += 1
                self._save_quota(quota)
                seq = quota["waitlist_seq"]
                claim = {
                    "claim_id": self._ids.new_id("qcl"),
                    "institution": institution,
                    "month": month,
                    "ref": ref,
                    "quantity": quantity,
                    "state": CLAIM_WAITING,
                    "seq": seq,
                    "created_at": now,
                    "updated_at": now,
                }
                self._store.put(COLLECTION_QUOTA_CLAIMS, claim["claim_id"], claim)
                self._emit(
                    "institution_quota_waitlisted",
                    {"institution": institution, "month": month, "quantity": quantity, "ref": ref, "seq": seq},
                )
            return self._claim_view(claim, quota)

    def release(self, request: dict[str, Any]) -> dict[str, Any]:
        """释放名额并按原候补顺序补位。

        定位方式：给出 ``claim_id``，或给出 ``institution/month/ref``。
        """
        claim_id = request.get("claim_id")
        with self._store.transaction():
            if claim_id is not None:
                if not isinstance(claim_id, str) or not claim_id.strip():
                    raise ValidationError("field claim_id must be a non-empty string")
                record = self._store.get(COLLECTION_QUOTA_CLAIMS, claim_id.strip())
                if record is None:
                    raise NotFoundError("claim not found", details={"claim_id": claim_id})
            else:
                institution = self._validate_institution(request.get("institution"))
                month = self._validate_month(request.get("month"))
                ref = request.get("ref")
                if not isinstance(ref, str) or not ref.strip():
                    raise ValidationError("field ref must be a non-empty string", details={"field": "ref"})
                matches = [
                    c
                    for c in self._claims(institution, month)
                    if c["ref"] == ref.strip() and c["state"] != CLAIM_RELEASED
                ]
                if not matches:
                    raise NotFoundError(
                        "active claim not found",
                        details={"institution": institution, "month": month, "ref": ref},
                    )
                record = sorted(matches, key=lambda c: c["created_at"])[0]

            if record["state"] != CLAIM_ACTIVE:
                raise StateError(
                    "only an ACTIVE claim can be released",
                    details={"claim_id": record["claim_id"], "state": record["state"]},
                )

            institution = record["institution"]
            month = record["month"]
            quota = self._load_quota(institution, month)
            record["state"] = CLAIM_RELEASED
            record["updated_at"] = dt_to_str(self._clock.now())
            self._store.put(COLLECTION_QUOTA_CLAIMS, record["claim_id"], record)
            quota["remaining"] += record["quantity"]

            # 严格按候补序号补位：队首数量不满足则停止，不跳过
            promoted: list[dict[str, Any]] = []
            for waiting in self._waiting_sorted(institution, month):
                if waiting["quantity"] > quota["remaining"]:
                    break
                quota["remaining"] -= waiting["quantity"]
                waiting["state"] = CLAIM_ACTIVE
                waiting["updated_at"] = dt_to_str(self._clock.now())
                self._store.put(COLLECTION_QUOTA_CLAIMS, waiting["claim_id"], waiting)
                promoted.append(waiting)
                self._emit(
                    "institution_quota_waitlist_promoted",
                    {
                        "institution": institution,
                        "month": month,
                        "claim_id": waiting["claim_id"],
                        "ref": waiting["ref"],
                    },
                )
            self._save_quota(quota)
            self._emit(
                "institution_quota_released",
                {
                    "institution": institution,
                    "month": month,
                    "claim_id": record["claim_id"],
                    "quantity": record["quantity"],
                    "promoted": [c["claim_id"] for c in promoted],
                },
            )
            return {
                "released_claim_id": record["claim_id"],
                "promoted": [self._claim_view(c, quota) for c in promoted],
                **self._quota_view(quota),
            }

    def get_quota(self, institution: str, month: str) -> dict[str, Any]:
        """机构负责人查看月度名额：总量、剩余、已占与候补队列。"""
        institution = self._validate_institution(institution)
        month = self._validate_month(month)
        with self._store.transaction():
            quota = self._load_quota(institution, month)
            return self._quota_view(quota)

    # ------------------------------------------------------------------
    # 视图
    # ------------------------------------------------------------------

    def _quota_view(self, quota: dict[str, Any]) -> dict[str, Any]:
        claims = self._claims(quota["institution"], quota["month"])
        active = [c for c in claims if c["state"] == CLAIM_ACTIVE]
        waiting = self._waiting_sorted(quota["institution"], quota["month"])
        return {
            "institution": quota["institution"],
            "month": quota["month"],
            "total": quota["total"],
            "remaining": quota["remaining"],
            "claimed": sum(c["quantity"] for c in active),
            "waiting_count": len(waiting),
            "active_claims": [self._claim_view(c, quota) for c in sorted(active, key=lambda c: c["created_at"])],
            "waitlist": [self._claim_view(c, quota) for c in waiting],
        }

    def _claim_view(self, claim: dict[str, Any], quota: dict[str, Any]) -> dict[str, Any]:
        view = {
            "claim_id": claim["claim_id"],
            "institution": claim["institution"],
            "month": claim["month"],
            "ref": claim["ref"],
            "quantity": claim["quantity"],
            "state": claim["state"],
            "remaining": quota["remaining"],
        }
        if claim["state"] == CLAIM_WAITING:
            ahead = [c for c in self._waiting_sorted(claim["institution"], claim["month"]) if c["seq"] < claim["seq"]]
            view["position"] = len(ahead) + 1
        return view
