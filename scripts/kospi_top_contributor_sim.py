from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from pykrx import stock

DATE_FMT = "%Y%m%d"


@dataclass(frozen=True)
class YearResult:
    year: int
    start_date: str
    end_date: str
    ticker: str
    name: str
    start_weight: float
    stock_return: float
    contribution_return: float
    kospi_like_return: float
    ex_top_return: float
    removal_impact: float


def _market_cap(date: str, cache_dir: Path) -> pd.DataFrame:
    cache_path = cache_dir / f"kospi_market_cap_{date}.csv"
    if cache_path.exists():
        return pd.read_csv(cache_path, dtype={"티커": str}).set_index("티커")

    try:
        df = stock.get_market_cap(date, market="KOSPI")
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            "Failed to fetch KOSPI market-cap data through pykrx. "
            "Check KRX/pykrx access or pre-populate the cache CSV for this date: "
            f"{cache_path}"
        ) from exc
    if df.empty:
        raise RuntimeError(f"No KOSPI market-cap data returned for {date}")
    df.index = df.index.astype(str).str.zfill(6)
    df.index.name = "티커"
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(cache_path)
    return df


def _trading_dates(year: int, local_price_root: Path | None) -> tuple[str, str]:
    if local_price_root is not None and local_price_root.exists():
        dates = []
        for path in local_price_root.glob(f"dt={year}-*/part.parquet"):
            date = path.parent.name.removeprefix("dt=")
            dates.append(date.replace("-", ""))
        if dates:
            return min(dates), max(dates)

    # Fallback: pykrx usually adjusts non-trading endpoints internally for OHLCV.
    ohlcv = stock.get_index_ohlcv_by_date(f"{year}0101", f"{year}1231", "1001")
    if ohlcv.empty:
        raise RuntimeError(f"No KOSPI index dates returned for {year}")
    dates = [d.strftime(DATE_FMT) for d in ohlcv.index]
    return min(dates), max(dates)


def _ticker_name(ticker: str) -> str:
    try:
        return stock.get_market_ticker_name(ticker)
    except Exception:  # noqa: BLE001
        return ticker


def compute_year(
    year: int,
    cache_dir: Path,
    local_price_root: Path | None,
) -> YearResult:
    start_date, end_date = _trading_dates(year, local_price_root)
    start = _market_cap(start_date, cache_dir)
    end = _market_cap(end_date, cache_dir)

    required = {"종가", "시가총액"}
    missing = required - set(start.columns) | required - set(end.columns)
    if missing:
        raise RuntimeError(f"Missing required market-cap columns: {sorted(missing)}")

    joined = start[["종가", "시가총액"]].join(
        end[["종가"]], how="inner", lsuffix="_start", rsuffix="_end"
    )
    joined = joined[
        (joined["종가_start"] > 0) & (joined["시가총액"] > 0) & (joined["종가_end"] > 0)
    ]
    if joined.empty:
        raise RuntimeError(f"No overlapping KOSPI tickers for {year}")

    joined["return"] = joined["종가_end"] / joined["종가_start"] - 1.0
    joined["contribution_value"] = joined["시가총액"] * joined["return"]
    total_start_cap = float(joined["시가총액"].sum())
    total_contribution = float(joined["contribution_value"].sum())

    kospi_like_return = total_contribution / total_start_cap
    joined["excluded_return"] = (
        (total_contribution - joined["contribution_value"])
        / (total_start_cap - joined["시가총액"])
    )
    joined["removal_impact"] = kospi_like_return - joined["excluded_return"]

    top = joined.sort_values("removal_impact", ascending=False).iloc[0]
    ticker = str(top.name)
    top_start_cap = float(top["시가총액"])
    top_contribution = float(top["contribution_value"])
    ex_top_return = float(top["excluded_return"])

    return YearResult(
        year=year,
        start_date=start_date,
        end_date=end_date,
        ticker=ticker,
        name=_ticker_name(ticker),
        start_weight=top_start_cap / total_start_cap,
        stock_return=float(top["return"]),
        contribution_return=top_contribution / total_start_cap,
        kospi_like_return=kospi_like_return,
        ex_top_return=ex_top_return,
        removal_impact=float(top["removal_impact"]),
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-year", type=int, default=1995)
    parser.add_argument("--end-year", type=int, default=2025)
    parser.add_argument("--cache-dir", type=Path, default=Path("data/_cache/kospi_contributors"))
    parser.add_argument(
        "--local-price-root",
        type=Path,
        default=Path("data/gold/daily_prices_adj"),
        help="Used only to find first/last local trading date per year.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/reports/kospi_top_contributors.csv"),
    )
    args = parser.parse_args()

    rows = [
        compute_year(year, args.cache_dir, args.local_price_root).__dict__
        for year in range(args.start_year, args.end_year + 1)
    ]
    out = pd.DataFrame(rows)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.output, index=False)
    print(out.to_string(index=False))
    print(f"\nwrote {args.output}")


if __name__ == "__main__":
    main()
