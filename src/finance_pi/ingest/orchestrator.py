from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date

from finance_pi.ingest.models import SourceAdapter, WriteResult


@dataclass(frozen=True)
class IngestOrchestrator:
    """Run source adapters through the common ingest lifecycle."""

    adapters: Iterable[SourceAdapter]

    def run_iter(self, since: date, until: date) -> Iterable[WriteResult]:
        for adapter in self.adapters:
            for unit in adapter.list_pending(since, until):
                batch = adapter.fetch(unit)
                yield adapter.write_bronze(batch)

    def run(self, since: date, until: date) -> list[WriteResult]:
        return list(self.run_iter(since, until))
