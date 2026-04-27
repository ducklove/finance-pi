from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path


@dataclass(frozen=True)
class DataLakeLayout:
    """Path helper for the medallion data lake."""

    root: Path

    def ensure_base_dirs(self) -> None:
        for relative in [
            "bronze/krx_daily",
            "bronze/kis_daily",
            "bronze/dart_filings",
            "bronze/dart_financials",
            "bronze/dart_company",
            "bronze/pre2010",
            "silver/prices",
            "silver/financials",
            "silver/filings",
            "silver/corporate_actions",
            "silver/security_identity",
            "gold/daily_prices_adj",
            "gold/fundamentals_pit",
            "gold/universe_history",
            "catalog",
            "_cache",
            "reports/data_quality",
            "reports/backtest_fraud",
        ]:
            (self.root / relative).mkdir(parents=True, exist_ok=True)

    def partition_path(
        self,
        dataset: str,
        partition_date: date,
        *,
        source: str | None = None,
    ) -> Path:
        dt = partition_date.isoformat()
        match dataset:
            case "bronze.krx_daily_raw":
                return self.root / "bronze" / "krx_daily" / f"dt={dt}" / "part.parquet"
            case "bronze.kis_daily_raw":
                return self.root / "bronze" / "kis_daily" / f"dt={dt}" / "part.parquet"
            case "bronze.dart_filings_raw":
                return self.root / "bronze" / "dart_filings" / f"dt={dt}" / "part.parquet"
            case "bronze.dart_financials_raw":
                return self.root / "bronze" / "dart_financials" / f"rcept_dt={dt}" / "part.parquet"
            case "bronze.dart_company_raw":
                return self.root / "bronze" / "dart_company" / f"snapshot_dt={dt}" / "part.parquet"
            case "bronze.pre2010_raw":
                if source is None:
                    raise ValueError("source is required for bronze.pre2010_raw")
                return (
                    self.root
                    / "bronze"
                    / "pre2010"
                    / f"source={source}"
                    / f"dt={dt}"
                    / "part.parquet"
                )
            case "silver.prices":
                return self.root / "silver" / "prices" / f"dt={dt}" / "part.parquet"
            case "silver.filings":
                return self.root / "silver" / "filings" / f"dt={dt}" / "part.parquet"
            case "silver.corporate_actions":
                return self.root / "silver" / "corporate_actions" / f"dt={dt}" / "part.parquet"
            case "gold.daily_prices_adj":
                return self.root / "gold" / "daily_prices_adj" / f"dt={dt}" / "part.parquet"
            case "gold.fundamentals_pit":
                return self.root / "gold" / "fundamentals_pit" / f"dt={dt}" / "part.parquet"
            case "gold.universe_history":
                return self.root / "gold" / "universe_history" / f"dt={dt}" / "part.parquet"
            case _:
                raise ValueError(f"{dataset} is not date-partitioned")

    def singleton_path(self, dataset: str) -> Path:
        match dataset:
            case "silver.security_identity":
                return self.root / "silver" / "security_identity" / "part.parquet"
            case "gold.security_master":
                return self.root / "gold" / "security_master.parquet"
            case _:
                raise ValueError(f"{dataset} is not a singleton dataset")
