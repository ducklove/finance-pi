from __future__ import annotations

from collections.abc import Iterable
from datetime import date

from finance_pi.identity.models import Listing


def resolve_listing(listings: Iterable[Listing], ticker: str, as_of: date) -> Listing:
    """Resolve (ticker, as_of_date) to exactly one listing."""

    matches = [
        listing for listing in listings if listing.ticker == ticker and listing.is_active_on(as_of)
    ]
    if not matches:
        raise LookupError(f"no active listing for ticker={ticker!r} as_of={as_of.isoformat()}")
    if len(matches) > 1:
        ids = ", ".join(sorted(listing.listing_id for listing in matches))
        raise LookupError(
            f"ambiguous listing for ticker={ticker!r} as_of={as_of.isoformat()}: {ids}"
        )
    return matches[0]
