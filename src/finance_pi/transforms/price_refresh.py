"""기업행동 변경 시 원천의 최신 수정주가를 보관하고 재계산에 사용한다."""

from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path

import polars as pl

from finance_pi.http import HttpJsonClient
from finance_pi.sources.naver.client import NaverDailyPriceClient
from finance_pi.storage.parquet import ParquetDatasetWriter


def refresh_action_price_history(data_root: Path, security_ids: set[str], *, client=None) -> int:
    """원본 Silver는 보존한다. 누락된 과거 구간이 있으면 성공으로 기록하지 않는다."""
    from finance_pi.transforms.builders import (
        _load_corporate_actions,
        _read_silver_prices_for_securities,
    )

    if not security_ids:
        return 0
    client = client or NaverDailyPriceClient(
        HttpJsonClient(source="naver", base_url="https://api.finance.naver.com")
    )
    prices = _read_silver_prices_for_securities(data_root, sorted(security_ids))
    if prices is None or prices.is_empty():
        return 0
    prepared = []
    actions = _load_corporate_actions(data_root)
    for security_id in sorted(security_ids):
        old = prices.filter(pl.col("security_id") == security_id)
        if old.is_empty():
            continue
        ticker = security_id.removeprefix("S")
        rows = client.fetch_daily_prices(ticker, old["date"].min(), old["date"].max())
        if not rows:
            raise ValueError(f"{ticker}: 기업행동 수정주가 재조회 결과가 비어 있습니다")
        fresh = (
            pl.DataFrame(rows)
            .select("date", "open", "high", "low", "close", "volume")
            .with_columns(pl.col("date").cast(pl.Date), pl.lit(security_id).alias("security_id"))
        )
        if fresh.select(pl.struct("date", "security_id").n_unique()).item() != fresh.height:
            raise ValueError(f"{ticker}: 수정주가에 중복 날짜가 있습니다")
        if fresh.filter(
            pl.col("close").is_null() | ~pl.col("close").is_finite() | (pl.col("close") <= 0)
        ).height:
            raise ValueError(f"{ticker}: 수정주가에 유효하지 않은 종가가 있습니다")
        # 이미 조정됐다고 표시된 과거 Naver 값은 비율을 다시 곱할 수 없다.
        # 새 원천에서 이 구간을 모두 확인해야 한다. 원천 제공 시작일 이전의
        # KRX 원가격은 기존 기업행동 보정 경로가 담당한다.
        required = old.filter(pl.col("price_source") == "naver").select("date").unique()
        missing = required.join(fresh.select("date"), on="date", how="anti")
        if missing.height:
            raise ValueError(f"{ticker}: 기존 Naver 수정주가 {missing.height}일 재조회 누락")
        if actions is not None:
            event_dates = actions.filter(pl.col("security_id") == security_id)[
                "effective_date"
            ].to_list()
            returns = fresh.sort("date").with_columns(
                (pl.col("close") / pl.col("close").shift(1)).alias("_ratio")
            )
            # 이번 재조회를 일으킨 가장 최근 이벤트만 검사한다. 수십 년 전
            # 회생/재상장 구간의 실제 가격 재평가를 새 병합 오류로 단정하지 않는다.
            for event_date in [max(event_dates)] if event_dates else []:
                nearby = returns.filter(
                    ((pl.col("date") - event_date).dt.total_days().abs() <= 10)
                    & ((pl.col("_ratio") > 1.5) | (pl.col("_ratio") < 0.5))
                )
                if nearby.height:
                    raise ValueError(
                        f"{ticker}: {event_date} 주변 원천 수정주가에 큰 단절이 남아 있습니다"
                    )
        prepared.append((ticker, fresh))

    writer = ParquetDatasetWriter()
    for ticker, fresh in prepared:
        fingerprint = hashlib.sha256(
            json.dumps(fresh.to_dicts(), default=str, sort_keys=True).encode()
        ).hexdigest()[:16]
        capture = data_root / "bronze/action_price_refresh" / ticker / f"{fingerprint}.parquet"
        if not capture.exists():
            writer.write(
                fresh,
                capture,
                source="naver",
                request_hash=fingerprint,
                include_ingest_metadata=True,
            )
        snapshot = data_root / "silver/action_price_refresh" / f"{ticker}.parquet"
        writer.write(
            fresh.with_columns(pl.lit(date.today()).alias("refreshed_on")),
            snapshot,
            mode="overwrite",
        )
    return len(prepared)


def apply_refreshed_price_history(data_root: Path, prices: pl.DataFrame) -> pl.DataFrame:
    """원천에서 다시 확인한 날짜만 최신 주식 단위로 대체한다. 반복 실행해도 동일하다."""
    paths = [
        data_root / "silver/action_price_refresh" / f"{sid.removeprefix('S')}.parquet"
        for sid in prices["security_id"].unique().to_list()
    ]
    frames = [pl.read_parquet(path) for path in paths if path.exists()]
    if not frames:
        return prices
    values = ["open", "high", "low", "close", "volume"]
    fresh = pl.concat(frames, how="diagonal_relaxed").select(
        "date", "security_id", *[pl.col(c).alias(f"_fresh_{c}") for c in values]
    )
    joined = prices.join(fresh, on=["date", "security_id"], how="left")
    if "price_basis" not in joined.columns:
        from finance_pi.transforms.builders import _price_basis_expr

        joined = joined.with_columns(_price_basis_expr(prices).alias("price_basis"))
    return joined.with_columns(
        *[pl.coalesce(f"_fresh_{c}", c).alias(c) for c in values],
        pl.when(pl.col("_fresh_close").is_not_null())
        .then(pl.lit("adjusted"))
        .otherwise(pl.col("price_basis"))
        .alias("price_basis"),
    ).drop([f"_fresh_{c}" for c in values])
