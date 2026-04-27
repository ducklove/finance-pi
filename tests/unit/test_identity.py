from __future__ import annotations

from datetime import date

import pytest
from pydantic import ValidationError

from finance_pi.identity import DelistingReason, Listing, resolve_listing


def test_resolve_listing_uses_ticker_and_date() -> None:
    old = Listing(
        listing_id="L001",
        security_id="S001",
        ticker="123456",
        market="KOSPI",
        listed_date=date(2010, 1, 4),
        delisted_date=date(2015, 12, 30),
        delisting_reason=DelistingReason.MERGER,
    )
    new = Listing(
        listing_id="L002",
        security_id="S002",
        ticker="123456",
        market="KOSDAQ",
        listed_date=date(2018, 1, 2),
    )

    assert resolve_listing([old, new], "123456", date(2012, 3, 5)).listing_id == "L001"
    assert resolve_listing([old, new], "123456", date(2020, 3, 5)).listing_id == "L002"


def test_listing_rejects_invalid_ticker() -> None:
    with pytest.raises(ValidationError):
        Listing(
            listing_id="L001",
            security_id="S001",
            ticker="ABCDEF",
            market="KOSPI",
            listed_date=date(2010, 1, 4),
        )
