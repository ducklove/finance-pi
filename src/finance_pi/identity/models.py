from __future__ import annotations

from datetime import date
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class ShareClass(StrEnum):
    COMMON = "common"
    PREFERRED = "preferred"
    OTHER = "other"


class SecurityType(StrEnum):
    EQUITY = "equity"
    SPAC_PRE = "spac_pre"
    SPAC_POST = "spac_post"
    ETF = "etf"
    ETN = "etn"
    OTHER = "other"


class DelistingReason(StrEnum):
    MERGER = "merger"
    DELISTING_FOR_CAUSE = "delisting_for_cause"
    VOLUNTARY = "voluntary"
    TRANSFER = "transfer"
    UNKNOWN = "unknown"


class RelationType(StrEnum):
    PREFERRED_TO_COMMON = "preferred_to_common"
    COMMON_TO_PREFERRED = "common_to_preferred"
    SPAC_PRE_MERGER = "spac_pre_merger"
    SPAC_POST_MERGER = "spac_post_merger"
    SUCCESSOR = "successor"


class StrictIdentityModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Issuer(StrictIdentityModel):
    issuer_id: str = Field(min_length=1)
    corp_code: str = Field(pattern=r"^\d{8}$")
    name: str = Field(min_length=1)


class Security(StrictIdentityModel):
    security_id: str = Field(min_length=1)
    issuer_id: str = Field(min_length=1)
    security_type: SecurityType
    share_class: ShareClass
    isin: str | None = None
    name: str | None = None


class Listing(StrictIdentityModel):
    listing_id: str = Field(min_length=1)
    security_id: str = Field(min_length=1)
    ticker: str = Field(pattern=r"^\d{6}$")
    market: str = Field(min_length=1)
    listed_date: date
    delisted_date: date | None = None
    delisting_reason: DelistingReason = DelistingReason.UNKNOWN

    @model_validator(mode="after")
    def delisted_after_listed(self) -> Listing:
        if self.delisted_date is not None and self.delisted_date < self.listed_date:
            raise ValueError("delisted_date must be on or after listed_date")
        return self

    def is_active_on(self, as_of: date) -> bool:
        if as_of < self.listed_date:
            return False
        return self.delisted_date is None or as_of <= self.delisted_date


class SecurityRelation(StrictIdentityModel):
    parent_security_id: str
    child_security_id: str
    relation_type: RelationType
    effective_from: date
    effective_to: date | None = None

    @model_validator(mode="after")
    def effective_range_is_ordered(self) -> SecurityRelation:
        if self.effective_to is not None and self.effective_to < self.effective_from:
            raise ValueError("effective_to must be on or after effective_from")
        return self


class IdentityReviewItem(StrictIdentityModel):
    source: str
    payload: dict[str, Any]
    reason: str
    created_date: date
    resolved: bool = False

    @field_validator("payload")
    @classmethod
    def payload_is_not_empty(cls, value: dict[str, Any]) -> dict[str, Any]:
        if not value:
            raise ValueError("payload must not be empty")
        return value
