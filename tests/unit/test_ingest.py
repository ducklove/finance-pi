from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from finance_pi.cli.app import _dart_financial_requests
from finance_pi.config import ProjectPaths
from finance_pi.ingest import IngestOrchestrator, request_hash
from finance_pi.ingest.models import IngestUnit, RawBatch, WriteResult
from finance_pi.sources.kis.adapter import KisUniverseDailyAdapter
from finance_pi.sources.naver.adapter import NaverDailyBackfillAdapter
from finance_pi.sources.opendart.adapter import DartFilingsAdapter, DartFinancialsBulkAdapter
from finance_pi.storage import DataLakeLayout, ParquetDatasetWriter


def test_request_hash_is_stable_for_param_order() -> None:
    left = request_hash("krx", "daily", {"b": 2, "a": 1})
    right = request_hash("krx", "daily", {"a": 1, "b": 2})
    assert left == right


def test_raw_batch_to_frame_accepts_null_then_string() -> None:
    batch = RawBatch(
        IngestUnit("opendart", date(2026, 4, 29), "corpCode.xml"),
        [
            {"stock_code": None},
            {"stock_code": "036720"},
        ],
    )

    frame = batch.to_frame({"stock_code": pl.String})

    assert frame["stock_code"].to_list() == [None, "036720"]


def test_raw_batch_to_frame_casts_json_date_strings() -> None:
    batch = RawBatch(
        IngestUnit("opendart", date(2026, 4, 29), "corpCode.xml"),
        [
            {"snapshot_dt": "2026-04-28", "stock_code": None},
            {"snapshot_dt": "2026-04-28", "stock_code": "036720"},
        ],
    )

    frame = batch.to_frame({"snapshot_dt": pl.Date, "stock_code": pl.String})

    assert frame["snapshot_dt"].dtype == pl.Date
    assert frame["snapshot_dt"].to_list() == [date(2026, 4, 28), date(2026, 4, 28)]
    assert frame["stock_code"].to_list() == [None, "036720"]


def test_dart_filings_adapter_chunks_and_marks_completed_ranges(tmp_path) -> None:
    class FakeOpenDartClient:
        def fetch_filings(self, since: date, until: date):
            return [
                {
                    "rcept_dt": since,
                    "corp_code": "00126380",
                    "corp_name": "Samsung",
                    "stock_code": "005930",
                    "rcept_no": f"{since:%Y%m%d}000001",
                    "report_nm": "report",
                    "rm": None,
                }
            ]

    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    adapter = DartFilingsAdapter(
        layout,
        ParquetDatasetWriter(),
        FakeOpenDartClient(),
        chunk_days=2,
    )

    units = list(adapter.list_pending(date(2026, 4, 1), date(2026, 4, 5)))
    result = adapter.write_bronze(adapter.fetch(units[0]))

    assert len(units) == 3
    assert result.rows == 1
    assert len(list(adapter.list_pending(date(2026, 4, 1), date(2026, 4, 5)))) == 2


def test_dart_filings_refresh_latest_merges_new_receipts(tmp_path) -> None:
    class FakeOpenDartClient:
        receipt = "20260429000001"

        def fetch_filings(self, since: date, until: date):
            return [
                {
                    "rcept_dt": since,
                    "corp_code": "00126380",
                    "corp_name": "Samsung",
                    "stock_code": "005930",
                    "rcept_no": self.receipt,
                    "report_nm": "report",
                    "rm": None,
                }
            ]

    client = FakeOpenDartClient()
    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    adapter = DartFilingsAdapter(
        layout,
        ParquetDatasetWriter(),
        client,
        chunk_days=1,
        refresh_latest=True,
    )
    target = date(2026, 4, 29)

    first = next(iter(adapter.list_pending(target, target)))
    adapter.write_bronze(adapter.fetch(first))
    client.receipt = "20260429000002"
    refreshed = next(iter(adapter.list_pending(target, target)))
    result = adapter.write_bronze(adapter.fetch(refreshed))

    frame = pl.read_parquet(layout.partition_path("bronze.dart_filings_raw", target))
    assert result.rows == 1
    assert frame["rcept_no"].sort().to_list() == ["20260429000001", "20260429000002"]


def test_dart_financial_requests_accepts_mixed_filing_date_schemas(tmp_path) -> None:
    data_root = tmp_path / "data"
    first = data_root / "bronze" / "dart_filings" / "dt=2026-03-15" / "part.parquet"
    second = data_root / "bronze" / "dart_filings" / "dt=2026-03-16" / "part.parquet"
    first.parent.mkdir(parents=True)
    second.parent.mkdir(parents=True)
    pl.DataFrame(
        [
            {
                "rcept_dt": date(2026, 3, 15),
                "corp_code": "00126380",
                "corp_name": "Samsung",
                "stock_code": "005930",
                "rcept_no": "20260315000001",
                "report_nm": "\uc0ac\uc5c5\ubcf4\uace0\uc11c (2025.12)",
                "rm": None,
            }
        ]
    ).write_parquet(first)
    pl.DataFrame(
        [
            {
                "rcept_dt": "2026-03-16",
                "corp_code": "00258801",
                "corp_name": "Kakao",
                "stock_code": "035720",
                "rcept_no": "20260316000001",
                "report_nm": "\uc0ac\uc5c5\ubcf4\uace0\uc11c (2025.12)",
                "rm": None,
            }
        ]
    ).write_parquet(second)

    requests = _dart_financial_requests(
        ProjectPaths(root=tmp_path),
        date(2026, 3, 1),
        date(2026, 3, 31),
        ("11011",),
    )

    assert [request["corp_code"] for request in requests] == ["00126380", "00258801"]
    assert {request["bsns_year"] for request in requests} == {2025}


def test_dart_financial_requests_advance_available_date_on_correction(tmp_path) -> None:
    # A correction filing ([기재정정]) later in the window must fold into the
    # same (corp, year, report) request and advance its available_date.
    data_root = tmp_path / "data"
    original = data_root / "bronze" / "dart_filings" / "dt=2026-03-15" / "part.parquet"
    correction = data_root / "bronze" / "dart_filings" / "dt=2026-03-20" / "part.parquet"
    original.parent.mkdir(parents=True)
    correction.parent.mkdir(parents=True)
    pl.DataFrame(
        [
            {
                "rcept_dt": date(2026, 3, 15),
                "corp_code": "00126380",
                "corp_name": "Samsung",
                "stock_code": "005930",
                "rcept_no": "20260315000001",
                "report_nm": "사업보고서 (2025.12)",
                "rm": None,
            }
        ]
    ).write_parquet(original)
    pl.DataFrame(
        [
            {
                "rcept_dt": date(2026, 3, 20),
                "corp_code": "00126380",
                "corp_name": "Samsung",
                "stock_code": "005930",
                "rcept_no": "20260320000001",
                "report_nm": "[기재정정]사업보고서 (2025.12)",
                "rm": "정",
            }
        ]
    ).write_parquet(correction)

    requests = _dart_financial_requests(
        ProjectPaths(root=tmp_path),
        date(2026, 3, 1),
        date(2026, 3, 31),
        ("11011",),
    )

    assert len(requests) == 1
    assert requests[0]["corp_code"] == "00126380"
    assert requests[0]["bsns_year"] == 2025
    assert requests[0]["available_date"] == date(2026, 3, 20)


def test_kis_universe_adapter_writes_combined_date_partitions(tmp_path) -> None:
    class FakeKisClient:
        def fetch_daily_prices(self, ticker: str, since: date, until: date):
            return [
                {
                    "date": since,
                    "ticker": ticker,
                    "isin": None,
                    "name": ticker,
                    "market": "KRX",
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.0,
                    "volume": 10,
                    "trading_value": 1000,
                    "market_cap": None,
                    "listed_shares": None,
                }
            ]

    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    adapter = KisUniverseDailyAdapter(
        layout,
        ParquetDatasetWriter(),
        FakeKisClient(),
        ("005930", "035720"),
        chunk_days=1,
        sleep_seconds=0,
    )
    unit = next(iter(adapter.list_pending(date(2026, 4, 28), date(2026, 4, 28))))
    result = adapter.write_bronze(adapter.fetch(unit))

    frame = pl.read_parquet(result.path)
    assert result.rows == 2
    assert sorted(frame["ticker"].to_list()) == ["005930", "035720"]


def test_kis_universe_adapter_merges_existing_partial_partition(tmp_path) -> None:
    class FakeKisClient:
        def fetch_daily_prices(self, ticker: str, since: date, until: date):
            return [
                {
                    "date": since,
                    "ticker": ticker,
                    "isin": None,
                    "name": ticker,
                    "market": "KRX",
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.0,
                    "volume": 10,
                    "trading_value": 1000,
                    "market_cap": None,
                    "listed_shares": None,
                }
            ]

    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    first = KisUniverseDailyAdapter(
        layout,
        ParquetDatasetWriter(),
        FakeKisClient(),
        ("005930",),
        chunk_days=1,
        sleep_seconds=0,
    )
    second = KisUniverseDailyAdapter(
        layout,
        ParquetDatasetWriter(),
        FakeKisClient(),
        ("005930", "035720"),
        chunk_days=1,
        sleep_seconds=0,
    )
    unit = next(iter(first.list_pending(date(2026, 4, 28), date(2026, 4, 28))))
    first.write_bronze(first.fetch(unit))

    unit = next(iter(second.list_pending(date(2026, 4, 28), date(2026, 4, 28))))
    result = second.write_bronze(second.fetch(unit))

    frame = pl.read_parquet(result.path)
    assert result.rows == 1
    assert result.reason == "merged existing KIS partitions: 1"
    assert sorted(frame["ticker"].to_list()) == ["005930", "035720"]


def test_kis_universe_adapter_batches_and_skips_existing_tickers(tmp_path) -> None:
    class FakeKisClient:
        def fetch_daily_prices(self, ticker: str, since: date, until: date):
            return [
                {
                    "date": since,
                    "ticker": ticker,
                    "isin": None,
                    "name": ticker,
                    "market": "KRX",
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.0,
                    "volume": 10,
                    "trading_value": 1000,
                    "market_cap": None,
                    "listed_shares": None,
                }
            ]

    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    first = KisUniverseDailyAdapter(
        layout,
        ParquetDatasetWriter(),
        FakeKisClient(),
        ("005930",),
        chunk_days=1,
        sleep_seconds=0,
    )
    unit = next(iter(first.list_pending(date(2026, 4, 28), date(2026, 4, 28))))
    first.write_bronze(first.fetch(unit))

    resumed = KisUniverseDailyAdapter(
        layout,
        ParquetDatasetWriter(),
        FakeKisClient(),
        ("005930", "035720", "000660"),
        chunk_days=1,
        ticker_batch_size=1,
        sleep_seconds=0,
    )
    units = list(resumed.list_pending(date(2026, 4, 28), date(2026, 4, 28)))

    assert [unit.params["tickers"] for unit in units] == [("035720",), ("000660",)]


def test_naver_daily_backfill_adapter_writes_request_chunks(tmp_path) -> None:
    class FakeNaverClient:
        def fetch_daily_prices(self, ticker: str, since: date, until: date):
            return [
                {
                    "date": since,
                    "ticker": ticker,
                    "isin": None,
                    "name": ticker,
                    "market": "KRX",
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.0,
                    "volume": 10,
                    "trading_value": None,
                    "market_cap": None,
                    "listed_shares": None,
                }
            ]

    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    adapter = NaverDailyBackfillAdapter(
        layout,
        ParquetDatasetWriter(),
        FakeNaverClient(),
        ("005930", "035720"),
        chunk_days=3650,
        ticker_batch_size=1,
        sleep_seconds=0,
    )

    units = list(adapter.list_pending(date(2026, 4, 28), date(2026, 4, 28)))
    result = adapter.write_bronze(adapter.fetch(units[0]))

    assert len(units) == 2
    assert result.rows == 1
    assert "bronze" in result.path.parts
    assert "naver_daily" in result.path.parts
    remaining = list(adapter.list_pending(date(2026, 4, 28), date(2026, 4, 28)))
    assert len(remaining) == 1
    assert remaining[0].params["tickers"] == ("035720",)


def test_naver_daily_backfill_adapter_retries_only_failed_tickers(tmp_path) -> None:
    class FlakyNaverClient:
        def __init__(self) -> None:
            self.fail_ticker = "035720"
            self.calls: list[str] = []

        def fetch_daily_prices(self, ticker: str, since: date, until: date):
            self.calls.append(ticker)
            if ticker == self.fail_ticker:
                raise RuntimeError("boom")
            return [
                {
                    "date": since,
                    "ticker": ticker,
                    "isin": None,
                    "name": ticker,
                    "market": "KRX",
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.0,
                    "volume": 10,
                    "trading_value": None,
                    "market_cap": None,
                    "listed_shares": None,
                }
            ]

    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    client = FlakyNaverClient()
    adapter = NaverDailyBackfillAdapter(
        layout,
        ParquetDatasetWriter(),
        client,
        ("005930", "035720"),
        chunk_days=3650,
        ticker_batch_size=2,
        sleep_seconds=0,
    )

    units = list(adapter.list_pending(date(2026, 4, 28), date(2026, 4, 28)))
    assert len(units) == 1
    result = adapter.write_bronze(adapter.fetch(units[0]))

    assert result.rows == 1
    assert result.reason == "partial Naver failures: 1"
    chunk_dir = result.path.parent
    sidecar = chunk_dir / "part.parquet.failures.json"
    assert sidecar.exists()

    retry_units = list(adapter.list_pending(date(2026, 4, 28), date(2026, 4, 28)))
    assert len(retry_units) == 1
    assert retry_units[0].params["tickers"] == ("035720",)
    assert retry_units[0].params["retry_of"] == units[0].request_hash

    client.fail_ticker = ""
    retry_result = adapter.write_bronze(adapter.fetch(retry_units[0]))

    assert retry_result.rows == 1
    assert not sidecar.exists()
    assert list(adapter.list_pending(date(2026, 4, 28), date(2026, 4, 28))) == []

    chunk_paths = layout.root.glob("bronze/naver_daily/*/*/part.parquet")
    frames = [pl.read_parquet(path) for path in chunk_paths]
    tickers = sorted(ticker for frame in frames for ticker in frame["ticker"].to_list())
    assert tickers == ["005930", "035720"]


def test_dart_financials_bulk_merges_same_rcept_date_partitions(tmp_path) -> None:
    class FakeOpenDartClient:
        def fetch_financials(
            self,
            corp_code: str,
            bsns_year: int,
            reprt_code: str,
            *,
            available_date: date,
            fs_div: str = "CFS",
            is_backfilled: bool = False,
        ):
            return [
                {
                    "security_id": None,
                    "corp_code": corp_code,
                    "fiscal_period_end": date(bsns_year, 12, 31),
                    "event_date": date(bsns_year, 12, 31),
                    "rcept_dt": available_date,
                    "available_date": available_date,
                    "report_type": reprt_code,
                    "account_id": "ifrs-full_Assets",
                    "account_name": "Assets",
                    "amount": 1000.0,
                    "is_consolidated": fs_div == "CFS",
                    "accounting_basis": "K-IFRS",
                    "is_backfilled": is_backfilled,
                }
            ]

    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    adapter = DartFinancialsBulkAdapter(
        layout,
        ParquetDatasetWriter(),
        FakeOpenDartClient(),
        (
            {
                "corp_code": "00126380",
                "bsns_year": 2025,
                "report_code": "11011",
                "available_date": date(2026, 3, 15),
            },
            {
                "corp_code": "00258801",
                "bsns_year": 2025,
                "report_code": "11011",
                "available_date": date(2026, 3, 15),
            },
        ),
        batch_size=1,
        sleep_seconds=0,
        is_backfilled=True,
    )

    units = list(adapter.list_pending(date(2026, 3, 1), date(2026, 3, 31)))
    first = adapter.write_bronze(adapter.fetch(units[0]))
    second = adapter.write_bronze(adapter.fetch(units[1]))

    frame = pl.read_parquet(layout.partition_path("bronze.dart_financials_raw", date(2026, 3, 15)))
    assert first.rows == 1
    assert second.rows == 1
    assert sorted(frame["corp_code"].to_list()) == ["00126380", "00258801"]
    assert frame["is_backfilled"].to_list() == [True, True]
    assert list(adapter.list_pending(date(2026, 3, 1), date(2026, 3, 31))) == []


def test_dart_financials_bulk_writes_partial_marker_and_retries(tmp_path) -> None:
    class FlakyOpenDartClient:
        def __init__(self) -> None:
            self.fail_corp_code = "00258801"

        def fetch_financials(
            self,
            corp_code: str,
            bsns_year: int,
            reprt_code: str,
            *,
            available_date: date,
            fs_div: str = "CFS",
            is_backfilled: bool = False,
        ):
            if corp_code == self.fail_corp_code:
                raise RuntimeError("quota exceeded")
            return [
                {
                    "security_id": None,
                    "corp_code": corp_code,
                    "fiscal_period_end": date(bsns_year, 12, 31),
                    "event_date": date(bsns_year, 12, 31),
                    "rcept_dt": available_date,
                    "available_date": available_date,
                    "report_type": reprt_code,
                    "account_id": "ifrs-full_Assets",
                    "account_name": "Assets",
                    "amount": 1000.0,
                    "is_consolidated": fs_div == "CFS",
                    "accounting_basis": "K-IFRS",
                    "is_backfilled": is_backfilled,
                }
            ]

    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    client = FlakyOpenDartClient()
    adapter = DartFinancialsBulkAdapter(
        layout,
        ParquetDatasetWriter(),
        client,
        (
            {
                "corp_code": "00126380",
                "bsns_year": 2025,
                "report_code": "11011",
                "available_date": date(2026, 3, 15),
            },
            {
                "corp_code": "00258801",
                "bsns_year": 2025,
                "report_code": "11011",
                "available_date": date(2026, 3, 15),
            },
        ),
        batch_size=2,
        sleep_seconds=0,
    )

    units = list(adapter.list_pending(date(2026, 3, 1), date(2026, 3, 31)))
    assert len(units) == 1
    result = adapter.write_bronze(adapter.fetch(units[0]))

    assert result.rows == 1
    assert "partial OpenDART failures: 1" in result.reason
    marker = adapter._marker_path(units[0])
    assert marker.read_text(encoding="utf-8").splitlines()[0] == "partial"

    retry_units = list(adapter.list_pending(date(2026, 3, 1), date(2026, 3, 31)))
    assert len(retry_units) == 1
    assert [request["corp_code"] for request in retry_units[0].params["requests"]] == ["00258801"]

    client.fail_corp_code = ""
    retry_result = adapter.write_bronze(adapter.fetch(retry_units[0]))

    assert retry_result.rows == 1
    assert marker.read_text(encoding="utf-8") == "ok\n"
    assert list(adapter.list_pending(date(2026, 3, 1), date(2026, 3, 31))) == []

    frame = pl.read_parquet(layout.partition_path("bronze.dart_financials_raw", date(2026, 3, 15)))
    assert sorted(frame["corp_code"].to_list()) == ["00126380", "00258801"]


def test_orchestrator_isolates_unit_failures(tmp_path) -> None:
    class FailingAdapter:
        name = "failing"

        def list_pending(self, since: date, until: date):
            yield IngestUnit(self.name, since, "/boom", {})

        def fetch(self, unit: IngestUnit) -> RawBatch:
            raise RuntimeError("adapter exploded")

        def write_bronze(self, batch: RawBatch):
            raise AssertionError("should not be reached")

    class RecordingAdapter:
        name = "recording"

        def __init__(self) -> None:
            self.ran = False

        def list_pending(self, since: date, until: date):
            yield IngestUnit(self.name, since, "/ok", {})

        def fetch(self, unit: IngestUnit) -> RawBatch:
            return RawBatch(unit, [{"value": 1}])

        def write_bronze(self, batch: RawBatch):
            self.ran = True
            return WriteResult(path=tmp_path / "ok.parquet", rows=1)

    recording = RecordingAdapter()
    results = IngestOrchestrator([FailingAdapter(), recording]).run(
        date(2026, 4, 28), date(2026, 4, 28)
    )

    assert recording.ran is True
    assert len(results) == 2
    assert results[0].skipped is True
    assert "adapter exploded" in results[0].reason
    assert results[1].rows == 1


def test_cli_ingest_wrapper_raises_after_isolating_failures() -> None:
    class FailingAdapter:
        name = "failing"

        def list_pending(self, since: date, until: date):
            yield IngestUnit(self.name, since, "/boom", {})

        def fetch(self, unit: IngestUnit) -> RawBatch:
            raise RuntimeError("adapter exploded")

        def write_bronze(self, batch: RawBatch):
            raise AssertionError("should not be reached")

    from finance_pi.cli import app as cli_app

    with pytest.raises(RuntimeError, match="adapter exploded"):
        cli_app._run_and_print(
            [FailingAdapter()],
            date(2026, 4, 28),
            date(2026, 4, 28),
        )
