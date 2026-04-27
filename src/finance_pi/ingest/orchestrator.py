from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date

from finance_pi.ingest.models import SourceAdapter, WriteResult


@dataclass(frozen=True)
class IngestOrchestrator:
    """Run source adapters through the common ingest lifecycle."""

    adapters: Iterable[SourceAdapter]

    def run(self, since: date, until: date) -> list[WriteResult]:
        results: list[WriteResult] = []
        for adapter in self.adapters:
            for unit in adapter.list_pending(since, until):
                batch = adapter.fetch(unit)
                results.append(adapter.write_bronze(batch))
        return results
