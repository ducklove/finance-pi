"""기업행동 수정주가 재조회: 미리보기 후 --apply로 백업 및 반영."""

from __future__ import annotations

import argparse
import fcntl
import json
import shutil
import tempfile
from datetime import datetime
from pathlib import Path

import polars as pl

from finance_pi.storage.parquet import ParquetDatasetWriter
from finance_pi.transforms.builders import (
    _adjusted_price_frame,
    _load_corporate_actions,
    _read_silver_prices_for_securities,
)
from finance_pi.transforms.price_refresh import (
    apply_refreshed_price_history,
    refresh_action_price_history,
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", type=Path, default=Path("data"))
    parser.add_argument("--tickers", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    root = args.data_root.resolve()
    lock_path = root / "_state/pipeline.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock = lock_path.open("a")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    ids = {f"S{t.strip()}" for t in args.tickers.split(",") if t.strip()}
    prices = _read_silver_prices_for_securities(root, sorted(ids))
    if prices is None or set(prices["security_id"]) != ids:
        raise ValueError("대상 종목의 Silver 가격을 모두 찾을 수 없습니다")
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    report = root / "_state" / f"action-refresh-{stamp}"
    report.mkdir(parents=True, exist_ok=False)
    with tempfile.TemporaryDirectory() as tmp:
        staging = Path(tmp)
        silver = staging / "silver/prices/dt=staging/part.parquet"
        silver.parent.mkdir(parents=True)
        prices.write_parquet(silver)
        actions = _load_corporate_actions(root)
        if actions is not None:
            action_path = staging / "silver/corporate_actions/dt=staging/part.parquet"
            action_path.parent.mkdir(parents=True)
            actions.write_parquet(action_path)
        refresh_action_price_history(staging, ids)
        adjusted = _adjusted_price_frame(
            apply_refreshed_price_history(staging, prices), _load_corporate_actions(root)
        )
        old = (
            pl.scan_parquet(
                str(root / "gold/daily_prices_adj/dt=*/part.parquet"), hive_partitioning=False
            )
            .filter(pl.col("security_id").is_in(sorted(ids)))
            .select("date", "security_id", pl.col("close_adj").alias("old_close"))
            .collect()
        )
        compare = adjusted.join(old, on=["date", "security_id"], how="left")
        summary = (
            compare.group_by("security_id")
            .agg(
                pl.len().alias("rows"),
                ((pl.col("close_adj") - pl.col("old_close")).abs() > 0.00001)
                .sum()
                .alias("changed"),
                pl.col("date").min().alias("since"),
                pl.col("date").max().alias("until"),
            )
            .sort("security_id")
            .to_dicts()
        )
        (report / "review.json").write_text(json.dumps(summary, default=str, indent=2))
        compare.write_parquet(report / "comparison.parquet")
        print(json.dumps(summary, default=str), flush=True)
        print(f"검증 자료: {report}", flush=True)
        if not args.apply:
            return
        # 정규 파이프라인과 같은 잠금 아래 전체 백업 후 날짜별 원자적 반영.
        partitions = adjusted.partition_by("date", as_dict=True)
        for key in partitions:
            day = key[0]
            path = root / f"gold/daily_prices_adj/dt={day}/part.parquet"
            if path.exists():
                backup = report / "backup" / path.relative_to(root)
                backup.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(path, backup)
        writer = ParquetDatasetWriter()
        for index, (key, updates) in enumerate(partitions.items(), 1):
            day = key[0]
            path = root / f"gold/daily_prices_adj/dt={day}/part.parquet"
            old_frame = pl.read_parquet(path, hive_partitioning=False) if path.exists() else None
            if old_frame is not None:
                untouched = old_frame.filter(~pl.col("security_id").is_in(sorted(ids)))
                updates = pl.concat([untouched, updates], how="diagonal_relaxed")
                if set(updates["security_id"]) != set(old_frame["security_id"]):
                    raise ValueError(f"{day}: 종목 집합이 바뀌어 반영 중단")
            writer.write(updates.sort("security_id"), path, mode="overwrite")
            if index % 500 == 0:
                print(f"가격 반영 {index}/{len(partitions)}", flush=True)
        for layer in ["bronze", "silver"]:
            folder = (
                staging
                / layer
                / ("action_price_refresh" if layer == "bronze" else "action_price_refresh")
            )
            for source in folder.rglob("*.parquet"):
                target = root / source.relative_to(staging)
                if target.exists():
                    backup = report / "backup" / target.relative_to(root)
                    backup.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(target, backup)
                target.parent.mkdir(parents=True, exist_ok=True)
                writer.write(pl.read_parquet(source), target, mode="overwrite")
        (report / "complete.json").write_text(json.dumps(summary, default=str))
        print("가격 및 재조회 원천 반영 완료", flush=True)


if __name__ == "__main__":
    main()
