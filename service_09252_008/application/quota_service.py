"""机构月度名额配额服务。

配额按“月份（UTC，``YYYY-MM``）+ 机构”分别计算：

- ``set_quota`` 设定某月某机构的名额上限；
- ``claim`` 领取一个名额：有名额则占用（``HELD``）并返回剩余名额，
  名额已满则进入候补（``WAITING``），候补顺序即申请先后（``position``）；
- ``release`` 释放名额：占用中的名额被释放后，候补者按原排序依次补位。

并发约定与预约锁定一致：每个用例在单个 ``store.transaction()`` 内完成
“读-判-写”。SQLite 后端以 ``BEGIN IMMEDIATE`` 串行化写事务，
因此两个并发领取不会超卖：满额时第二个请求必然落入候补。
"""
from __future__ import annotations

from typing import Any

from ..domain.errors import BusinessRuleError, ConflictError, NotFoundError, StateError, ValidationError
from ..domain.models import DomainEvent, Quota, QuotaClaim, validate_month_key
from ..persistence.store import Store
from .ports import Clock, IdGenerator

COLLECTION_QUOTAS = "quotas"
COLLECTION_QUOTA_CLAIMS = "quota_claims"
COLLECTION_EVENTS = "events"

#: 领取记录状态
CLAIM_HELD = "HELD"  # 已占用名额
CLAIM_WAITING = "WAITING"  # 候补排队
CLAIM_RELEASED = "RELEASED"  # 已释放（终态）

#: 仍占用名额或候补位的状态
CLAIM_ACTIVE_STATUSES = frozenset({CLAIM_HELD, CLAIM_WAITING})


class QuotaService:
    """机构月度名额的设定、领取、释放与候补补位用例。"""

    def __init__(self, store: Store, clock: Clock, ids: IdGenerator) -> None:
        self._store = store
        self._clock = clock
        self._ids = ids

    # ------------------------------------------------------------------
    # 辅助
    # ------------------------------------------------------------------

    @staticmethod
    def _quota_id(institution: str, month: str) -> str:
        return f"{month}:{institution}"

    def _emit(self, event_type: str, payload: dict[str, Any]) -> None:
        event = DomainEvent(
            event_id=self._ids.new_id("evt"),
            type=event_type,
            booking_id=None,
            payload=payload,
            created_at=self._clock.now(),
        )
        self._store.put(COLLECTION_EVENTS, event.event_id, event.to_dict())

    def _require_institution(self, request: dict[str, Any]) -> str:
        institution = request.get("institution")
        if not isinstance(institution, str) or not institution.strip():
            raise ValidationError("field institution must be a non-empty string")
        return institution.strip()

    def _require_month(self, request: dict[str, Any]) -> str:
        try:
            return validate_month_key(request.get("month"))
        except ValueError as exc:
            raise ValidationError(str(exc), details={"field": "month"}) from exc

    def _load_quota(self, institution: str, month: str) -> Quota:
        record = self._store.get(COLLECTION_QUOTAS, self._quota_id(institution, month))
        if record is None:
            raise NotFoundError(
                f"quota not configured for {institution} {month}",
                details={"institution": institution, "month": month},
            )
        return Quota.from_dict(record)

    def _save_quota(self, quota: Quota) -> None:
        self._store.put(COLLECTION_QUOTAS, self._quota_id(quota.institution, quota.month), quota.to_dict())

    def _load_claim(self, claim_id: str) -> QuotaClaim:
        record = self._store.get(COLLECTION_QUOTA_CLAIMS, claim_id)
        if record is None:
            raise NotFoundError(f"quota claim not found: {claim_id}", details={"claim_id": claim_id})
        return QuotaClaim.from_dict(record)

    def _claims_of(self, quota_id: str) -> list[QuotaClaim]:
        return [QuotaClaim.from_dict(r) for r in self._store.query(COLLECTION_QUOTA_CLAIMS, quota_id=quota_id)]

    def _next_position(self, quota_id: str) -> int:
        positions = [c.position for c in self._claims_of(quota_id)]
        return (max(positions) + 1) if positions else 1

    def _promote_waitlist(self, quota: Quota) -> list[str]:
        """名额腾出后按原排序（position 升序）让候补者补位，返回补位成功的领取 ID。"""
        promoted: list[str] = []
        waiting = sorted(
            (c for c in self._claims_of(quota.quota_id) if c.status == CLAIM_WAITING),
            key=lambda c: c.position,
        )
        for claim in waiting:
            if quota.remaining <= 0:
                break
            claim.status = CLAIM_HELD
            claim.updated_at = self._clock.now()
            self._store.put(COLLECTION_QUOTA_CLAIMS, claim.claim_id, claim.to_dict())
            quota.used += 1
            promoted.append(claim.claim_id)
            self._emit(
                "quota_promoted",
                {
                    "claim_id": claim.claim_id,
                    "quota_id": quota.quota_id,
                    "institution": quota.institution,
                    "month": quota.month,
                    "position": claim.position,
                },
            )
        return promoted

    # ------------------------------------------------------------------
    # 用例
    # ------------------------------------------------------------------

    def set_quota(self, request: dict[str, Any]) -> dict[str, Any]:
        """设定或调整某机构某月的名额上限。"""
        institution = self._require_institution(request)
        month = self._require_month(request)
        total = request.get("total")
        if isinstance(total, bool) or not isinstance(total, int) or total < 0:
            raise ValidationError("field total must be a non-negative integer", details={"field": "total"})
        with self._store.transaction():
            existing = self._store.get(COLLECTION_QUOTAS, self._quota_id(institution, month))
            quota = Quota.from_dict(existing) if existing else Quota(
                quota_id=self._quota_id(institution, month), institution=institution, month=month, total=total
            )
            if total < quota.used:
                raise BusinessRuleError(
                    "cannot reduce quota below the number of claims currently held",
                    details={"total": total, "used": quota.used},
                )
            quota.total = total
            self._emit(
                "quota_configured",
                {"quota_id": quota.quota_id, "institution": institution, "month": month, "total": total},
            )
            # 上调配额出现空缺时，候补者同样按原排序补位
            promoted = self._promote_waitlist(quota)
            self._save_quota(quota)
            view = self._quota_view(quota)
            view["promoted_claim_ids"] = promoted
            return view

    def claim(self, request: dict[str, Any]) -> dict[str, Any]:
        """领取一个名额；满额时进入候补。返回体含扣减后的剩余名额。"""
        institution = self._require_institution(request)
        month = self._require_month(request)
        applicant = request.get("applicant_id")
        if not isinstance(applicant, str) or not applicant.strip():
            raise ValidationError("field applicant_id must be a non-empty string", details={"field": "applicant_id"})
        applicant = applicant.strip()

        with self._store.transaction():
            quota = self._load_quota(institution, month)
            # 同一申请者不得重复占位（也顺带承接并发重试）
            duplicate = next(
                (
                    c
                    for c in self._claims_of(quota.quota_id)
                    if c.applicant_id == applicant and c.status in CLAIM_ACTIVE_STATUSES
                ),
                None,
            )
            if duplicate is not None:
                raise ConflictError(
                    "applicant already holds or waits for a quota slot this month",
                    details={"claim_id": duplicate.claim_id, "status": duplicate.status},
                )

            now = self._clock.now()
            claim = QuotaClaim(
                claim_id=self._ids.new_id("clm"),
                quota_id=quota.quota_id,
                institution=institution,
                month=month,
                applicant_id=applicant,
                status=CLAIM_HELD if quota.remaining > 0 else CLAIM_WAITING,
                position=self._next_position(quota.quota_id),
                created_at=now,
                updated_at=now,
            )
            if claim.status == CLAIM_HELD:
                quota.used += 1
                self._save_quota(quota)
                self._emit(
                    "quota_granted",
                    {
                        "claim_id": claim.claim_id,
                        "quota_id": quota.quota_id,
                        "institution": institution,
                        "month": month,
                        "applicant_id": applicant,
                        "remaining": quota.remaining,
                    },
                )
            else:
                self._save_quota(quota)
                self._emit(
                    "quota_waitlisted",
                    {
                        "claim_id": claim.claim_id,
                        "quota_id": quota.quota_id,
                        "institution": institution,
                        "month": month,
                        "applicant_id": applicant,
                        "position": claim.position,
                    },
                )
            self._store.put(COLLECTION_QUOTA_CLAIMS, claim.claim_id, claim.to_dict())
            return self._claim_view(claim, quota)

    def release(self, claim_id: str, request: dict[str, Any] | None = None) -> dict[str, Any]:
        """释放名额；占用中的名额释放后，候补者按原排序补位。"""
        request = request or {}
        with self._store.transaction():
            claim = self._load_claim(claim_id)
            if claim.status == CLAIM_RELEASED:
                raise StateError(
                    "claim has already been released", details={"claim_id": claim_id}
                )
            quota = self._load_quota(claim.institution, claim.month)
            was_held = claim.status == CLAIM_HELD
            claim.status = CLAIM_RELEASED
            claim.updated_at = self._clock.now()
            self._store.put(COLLECTION_QUOTA_CLAIMS, claim.claim_id, claim.to_dict())

            promoted: list[str] = []
            if was_held:
                quota.used -= 1
                self._emit(
                    "quota_released",
                    {
                        "claim_id": claim.claim_id,
                        "quota_id": quota.quota_id,
                        "institution": quota.institution,
                        "month": quota.month,
                        "reason": request.get("reason"),
                    },
                )
                promoted = self._promote_waitlist(quota)
            else:
                self._emit(
                    "quota_waitlist_cancelled",
                    {"claim_id": claim.claim_id, "quota_id": quota.quota_id},
                )
            self._save_quota(quota)
            view = self._quota_view(quota)
            view["released_claim_id"] = claim.claim_id
            view["promoted_claim_ids"] = promoted
            return view

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------

    def get_quota(self, institution: str, month: str) -> dict[str, Any]:
        with self._store.transaction():
            quota = self._load_quota(institution, month)
            return self._quota_view(quota)

    def get_claim(self, claim_id: str) -> dict[str, Any]:
        with self._store.transaction():
            claim = self._load_claim(claim_id)
            quota = self._load_quota(claim.institution, claim.month)
            return self._claim_view(claim, quota)

    def list_claims(self, institution: str, month: str) -> dict[str, Any]:
        with self._store.transaction():
            quota = self._load_quota(institution, month)
            claims = sorted(self._claims_of(quota.quota_id), key=lambda c: c.position)
            return {
                "quota": self._quota_view(quota),
                "claims": [self._claim_view(c, quota) for c in claims],
            }

    def quota_for_month(self, month: str) -> dict[str, Any]:
        """机构负责人按月查看名下各机构配额（亦可由 month_key_from_datetime 推导）。"""
        try:
            month = validate_month_key(month)
        except ValueError as exc:
            raise ValidationError(str(exc), details={"field": "month"}) from exc
        with self._store.transaction():
            quotas = [Quota.from_dict(r) for r in self._store.query(COLLECTION_QUOTAS, month=month)]
            quotas.sort(key=lambda q: q.institution)
            return {"month": month, "items": [self._quota_view(q) for q in quotas]}

    def _quota_view(self, quota: Quota) -> dict[str, Any]:
        claims = self._claims_of(quota.quota_id)
        held = sum(1 for c in claims if c.status == CLAIM_HELD)
        waiting = sum(1 for c in claims if c.status == CLAIM_WAITING)
        return {
            "quota_id": quota.quota_id,
            "institution": quota.institution,
            "month": quota.month,
            "total": quota.total,
            "used": quota.used,
            "remaining": quota.remaining,
            "held_claims": held,
            "waiting_claims": waiting,
        }

    def _claim_view(self, claim: QuotaClaim, quota: Quota) -> dict[str, Any]:
        return {
            "claim_id": claim.claim_id,
            "quota_id": claim.quota_id,
            "institution": claim.institution,
            "month": claim.month,
            "applicant_id": claim.applicant_id,
            "status": claim.status,
            "position": claim.position,
            "remaining": quota.remaining,
            "created_at": claim.created_at.isoformat(),
            "updated_at": claim.updated_at.isoformat(),
        }
