from __future__ import annotations

import argparse
import math
from dataclasses import dataclass
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

HORIZONS = [5, 20, 60]
FUTURE_HORIZONS = [5, 20, 60, 120]
SURGE_RULES = {
    5: 0.15,
    20: 0.30,
    60: 0.50,
}


@dataclass(frozen=True)
class RegressionResult:
    name: str
    nobs: int
    groups: int
    r2: float
    coefficients: dict[str, float]
    tstats: dict[str, float]


def _load_pair_panel(data_root: Path) -> pd.DataFrame:
    con = duckdb.connect()
    security_master = data_root / "gold/security_master.parquet"
    price_glob = str(data_root / "gold/daily_prices_adj/**/*.parquet")
    query = f"""
    with sm as (
        select security_id, ticker, name, market, share_class, security_type
        from read_parquet('{security_master}')
        where security_type = 'equity'
    ),
    prefs as (
        select
            security_id as pref_id,
            ticker as pref_ticker,
            name as pref_name,
            market as pref_market,
            substr(ticker, 1, 5) || '0' as common_ticker
        from sm
        where share_class = 'preferred'
          and name like '%우%'
    ),
    commons as (
        select
            security_id as common_id,
            ticker as common_ticker,
            name as common_name,
            market as common_market
        from sm
        where share_class = 'common'
    ),
    pairs as (
        select
            c.common_id,
            p.pref_id,
            c.common_ticker,
            p.pref_ticker,
            c.common_name,
            p.pref_name,
            c.common_market,
            p.pref_market
        from prefs p
        join commons c on c.common_ticker = p.common_ticker
    ),
    px as (
        select date, security_id, close_adj, volume, trading_value, market_cap
        from read_parquet('{price_glob}', hive_partitioning=true)
        where close_adj > 0
    )
    select
        c.date,
        pairs.common_id,
        pairs.pref_id,
        pairs.common_ticker,
        pairs.pref_ticker,
        pairs.common_name,
        pairs.pref_name,
        pairs.common_market,
        pairs.pref_market,
        c.close_adj as common_close,
        p.close_adj as pref_close,
        c.volume as common_volume,
        p.volume as pref_volume,
        c.trading_value as common_trading_value,
        p.trading_value as pref_trading_value,
        c.market_cap as common_market_cap,
        p.market_cap as pref_market_cap
    from pairs
    join px c on c.security_id = pairs.common_id
    join px p on p.security_id = pairs.pref_id and p.date = c.date
    order by pairs.pref_id, c.date
    """
    frame = con.sql(query).fetchdf()
    if frame.empty:
        raise RuntimeError("No matched common/preferred price rows found.")
    frame["date"] = pd.to_datetime(frame["date"])
    frame["pair_id"] = frame["common_ticker"] + "/" + frame["pref_ticker"]
    frame["discount"] = 1.0 - frame["pref_close"] / frame["common_close"]
    frame["log_pref_to_common"] = np.log(frame["pref_close"] / frame["common_close"])
    return frame


def _add_features(panel: pd.DataFrame) -> pd.DataFrame:
    panel = panel.sort_values(["pair_id", "date"]).copy()
    grouped = panel.groupby("pair_id", sort=False)
    for k in HORIZONS:
        panel[f"common_ret_{k}d"] = grouped["common_close"].pct_change(k)
        panel[f"pref_ret_{k}d"] = grouped["pref_close"].pct_change(k)
        panel[f"discount_chg_{k}d"] = panel["discount"] - grouped["discount"].shift(k)
        panel[f"common_velocity_{k}d"] = panel[f"common_ret_{k}d"] / k
        panel[f"pref_rel_ret_{k}d"] = panel[f"pref_ret_{k}d"] - panel[f"common_ret_{k}d"]
    for h in FUTURE_HORIZONS:
        panel[f"future_common_ret_{h}d"] = (
            grouped["common_close"].shift(-h) / panel["common_close"] - 1.0
        )
        panel[f"future_pref_ret_{h}d"] = (
            grouped["pref_close"].shift(-h) / panel["pref_close"] - 1.0
        )
        panel[f"future_pref_rel_ret_{h}d"] = (
            panel[f"future_pref_ret_{h}d"] - panel[f"future_common_ret_{h}d"]
        )
        panel[f"future_discount_chg_{h}d"] = grouped["discount"].shift(-h) - panel["discount"]
    panel["discount_pair_mean"] = grouped["discount"].transform("mean")
    panel["discount_pair_std"] = grouped["discount"].transform("std").replace(0, np.nan)
    panel["discount_z"] = (
        panel["discount"] - panel["discount_pair_mean"]
    ) / panel["discount_pair_std"]
    panel["year"] = panel["date"].dt.year
    return panel


def _cluster_ols(
    data: pd.DataFrame,
    y_col: str,
    x_cols: list[str],
    group_col: str = "pair_id",
    fixed_effect_col: str | None = "pair_id",
    name: str = "",
) -> RegressionResult:
    cols = list(
        dict.fromkeys(
            [y_col, group_col, *x_cols, *([fixed_effect_col] if fixed_effect_col else [])]
        )
    )
    d = data[cols].replace([np.inf, -np.inf], np.nan).dropna().copy()
    if fixed_effect_col:
        d[y_col] = d[y_col] - d.groupby(fixed_effect_col)[y_col].transform("mean")
        for col in x_cols:
            d[col] = d[col] - d.groupby(fixed_effect_col)[col].transform("mean")
    y = d[y_col].to_numpy(dtype=float)
    x = d[x_cols].to_numpy(dtype=float)
    keep = np.isfinite(y) & np.isfinite(x).all(axis=1)
    y = y[keep]
    x = x[keep]
    groups = d.loc[keep, group_col].to_numpy()
    if len(y) <= len(x_cols):
        raise RuntimeError(f"Not enough observations for {name or y_col}.")
    xtx = x.T @ x
    xtx_inv = np.linalg.pinv(xtx)
    beta = xtx_inv @ (x.T @ y)
    resid = y - x @ beta

    meat = np.zeros((len(x_cols), len(x_cols)))
    unique_groups = np.unique(groups)
    for group in unique_groups:
        mask = groups == group
        score = x[mask].T @ resid[mask]
        meat += np.outer(score, score)
    nobs = len(y)
    k = len(x_cols)
    g = len(unique_groups)
    finite = (g / (g - 1)) * ((nobs - 1) / max(nobs - k, 1)) if g > 1 else 1.0
    vcov = finite * xtx_inv @ meat @ xtx_inv
    se = np.sqrt(np.maximum(np.diag(vcov), 0))
    tstats = np.divide(beta, se, out=np.full_like(beta, np.nan), where=se > 0)
    sse = float(np.sum(resid**2))
    sst = float(np.sum((y - y.mean()) ** 2))
    r2 = 1.0 - sse / sst if sst > 0 else math.nan
    return RegressionResult(
        name=name or y_col,
        nobs=nobs,
        groups=g,
        r2=r2,
        coefficients=dict(zip(x_cols, beta, strict=True)),
        tstats=dict(zip(x_cols, tstats, strict=True)),
    )


def _quantile_tables(panel: pd.DataFrame) -> dict[int, pd.DataFrame]:
    tables: dict[int, pd.DataFrame] = {}
    for k in HORIZONS:
        cols = [f"common_ret_{k}d", f"pref_ret_{k}d", f"discount_chg_{k}d", f"pref_rel_ret_{k}d"]
        d = panel[["date", "pair_id", *cols]].replace([np.inf, -np.inf], np.nan).dropna().copy()
        d["bucket"] = pd.qcut(d[f"common_ret_{k}d"], 10, labels=False, duplicates="drop") + 1
        table = (
            d.groupby("bucket", observed=True)
            .agg(
                observations=("pair_id", "size"),
                common_return_mean=(f"common_ret_{k}d", "mean"),
                pref_return_mean=(f"pref_ret_{k}d", "mean"),
                pref_minus_common_mean=(f"pref_rel_ret_{k}d", "mean"),
                discount_change_mean=(f"discount_chg_{k}d", "mean"),
                discount_change_median=(f"discount_chg_{k}d", "median"),
            )
            .reset_index()
        )
        tables[k] = table
    return tables


def _surge_tables(panel: pd.DataFrame) -> dict[int, pd.DataFrame]:
    tables: dict[int, pd.DataFrame] = {}
    for k, threshold in SURGE_RULES.items():
        ret_col = f"common_ret_{k}d"
        event = panel.loc[panel[ret_col] >= threshold].copy()
        rows = []
        for h in FUTURE_HORIZONS:
            d = event[
                [
                    "pair_id",
                    "date",
                    ret_col,
                    f"discount_chg_{k}d",
                    f"future_pref_rel_ret_{h}d",
                    f"future_discount_chg_{h}d",
                ]
            ].replace([np.inf, -np.inf], np.nan).dropna()
            rows.append(
                {
                    "surge_horizon_days": k,
                    "surge_threshold": threshold,
                    "future_horizon_days": h,
                    "events": len(d),
                    "pairs": d["pair_id"].nunique(),
                    "common_surge_return_mean": d[ret_col].mean(),
                    "same_period_discount_change_mean": d[f"discount_chg_{k}d"].mean(),
                    "future_pref_minus_common_mean": d[f"future_pref_rel_ret_{h}d"].mean(),
                    "future_discount_change_mean": d[f"future_discount_chg_{h}d"].mean(),
                    "future_discount_narrows_share": (d[f"future_discount_chg_{h}d"] < 0).mean(),
                }
            )
        tables[k] = pd.DataFrame(rows)
    return tables


def _distributed_lag_table(panel: pd.DataFrame, max_lag: int = 20) -> pd.DataFrame:
    d = panel.sort_values(["pair_id", "date"]).copy()
    grouped = d.groupby("pair_id", sort=False)
    d["common_ret_1d_calc"] = grouped["common_close"].pct_change()
    d["pref_ret_1d_calc"] = grouped["pref_close"].pct_change()
    x_cols = []
    for lag in range(max_lag + 1):
        col = f"common_ret_lag_{lag}"
        d[col] = grouped["common_ret_1d_calc"].shift(lag)
        x_cols.append(col)
    res = _cluster_ols(
        d,
        "pref_ret_1d_calc",
        x_cols,
        fixed_effect_col="pair_id",
        name="preferred daily return on common daily return lags",
    )
    rows = []
    cumulative = 0.0
    for col in x_cols:
        lag = int(col.rsplit("_", 1)[1])
        cumulative += res.coefficients[col]
        rows.append(
            {
                "lag_days": lag,
                "beta": res.coefficients[col],
                "tstat": res.tstats[col],
                "cumulative_beta": cumulative,
            }
        )
    return pd.DataFrame(rows)


def _format_pct(value: float) -> str:
    if pd.isna(value):
        return "n/a"
    return f"{value * 100:.2f}%"


def _write_report(
    report_path: Path,
    panel: pd.DataFrame,
    quantiles: dict[int, pd.DataFrame],
    surges: dict[int, pd.DataFrame],
    regressions: list[RegressionResult],
    lag_table: pd.DataFrame,
    output_dir: Path,
) -> None:
    top_line = {
        "start": panel["date"].min().date(),
        "end": panel["date"].max().date(),
        "rows": len(panel),
        "pairs": panel["pair_id"].nunique(),
        "commons": panel["common_ticker"].nunique(),
    }
    q20 = quantiles[20]
    bottom = q20.iloc[0]
    top = q20.iloc[-1]
    surge20 = surges[20]
    surge20_60 = surge20.loc[surge20["future_horizon_days"] == 60].iloc[0]
    lag0_beta = lag_table.loc[lag_table["lag_days"] == 0, "beta"].iloc[0]
    lag20_cumulative_beta = lag_table.loc[
        lag_table["lag_days"] == 20,
        "cumulative_beta",
    ].iloc[0]
    bottom_mean = _format_pct(bottom["discount_change_mean"])
    top_mean = _format_pct(top["discount_change_mean"])
    bottom_median = _format_pct(bottom["discount_change_median"])
    top_median = _format_pct(top["discount_change_median"])
    surge20_60_rel = _format_pct(surge20_60["future_pref_minus_common_mean"])
    surge20_60_discount = _format_pct(surge20_60["future_discount_change_mean"])
    reg_lines = []
    for res in regressions:
        pieces = [
            f"{name}={coef:.4f} (t={res.tstats[name]:.2f})"
            for name, coef in res.coefficients.items()
        ]
        reg_lines.append(
            f"- {res.name}: n={res.nobs:,}, pairs={res.groups}, R2={res.r2:.3f}, "
            + ", ".join(pieces)
        )
    text = f"""# 우선주 괴리율과 보통주 급등 속도 분석

## 데이터

- 범위: {top_line["start"]} ~ {top_line["end"]}
- 보통주-우선주 매칭: {top_line["pairs"]:,}개 우선주 쌍, {top_line["commons"]:,}개 보통주
- 일별 쌍 관측치: {top_line["rows"]:,}개
- 가격: `data/gold/daily_prices_adj`, 조정종가 기준
- 매칭 방식: 우선주 티커 앞 5자리 + `0`을 보통주 티커로 사용

## 핵심 결과

1. 보통주의 단기 상승률이 클수록 같은 기간 우선주 괴리율은 커졌다.
   20거래일 보통주 수익률 하위 10% 구간의 평균 괴리율 변화는 {bottom_mean}였고,
   상위 10% 구간은 {top_mean}였다.
   중앙값 기준으로도 하위 10%는 {bottom_median},
   상위 10%는 {top_median}였다.

2. 급등 직후에는 우선주가 뒤늦게 따라붙는 패턴이 관찰됐다.
   보통주가 20거래일에 30% 이상 상승한 사건에서, 이후 60거래일 우선주 초과수익 평균은
   {surge20_60_rel},
   괴리율 변화 평균은
   {surge20_60_discount}였다.

3. 분산시차 회귀에서 보통주 일간 수익률에 대한 우선주 반응은 당일 반응만으로 끝나지 않았다.
   당일 베타는 {lag0_beta:.3f},
   20거래일까지 누적 베타는 {lag20_cumulative_beta:.3f}였다.
   이는 "같은 방향으로 움직이지만 느슨하고 지연된 커플링"이라는 가설과 부합한다.

## 회귀 요약

{chr(10).join(reg_lines)}

해석 기준: 괴리율 변화 회귀에서 양의 계수는 보통주가 빠르게 오를수록
우선주가 덜 따라와 할인 폭이 확대됨을 뜻한다.
미래 우선주 초과수익 회귀에서 양의 계수는 현재 괴리율이 높을수록
이후 우선주가 보통주 대비 더 강하게 움직임을 뜻한다.

## 산출물

- 일별 패널: `{output_dir / "preferred_discount_pair_panel.csv"}`
- 분위수 표: `{output_dir / "preferred_discount_return_quantiles.csv"}`
- 급등 이벤트 표: `{output_dir / "preferred_discount_surge_events.csv"}`
- 회귀 표: `{output_dir / "preferred_discount_regressions.csv"}`
- 분산시차 표: `{output_dir / "preferred_discount_distributed_lag.csv"}`

## 주의점

- 우선주는 유동성이 낮고 가격제한폭, 거래정지, 지정종목, 상장폐지 가능성의 영향을 크게 받는다.
- 여기서는 거래비용, 세금, 슬리피지, 체결 가능 수량을 반영하지 않았다.
- 통계적 관계가 존재하더라도 차익거래 가능성을 의미하지는 않는다.
"""
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(text, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", type=Path, default=Path("data"))
    parser.add_argument("--output-dir", type=Path, default=Path("data/reports"))
    parser.add_argument(
        "--report",
        type=Path,
        default=Path("docs/preferred-discount-surge-analysis.md"),
    )
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    panel = _add_features(_load_pair_panel(args.data_root))

    quantiles = _quantile_tables(panel)
    surges = _surge_tables(panel)
    regressions = []
    for k in HORIZONS:
        regressions.append(
            _cluster_ols(
                panel,
                f"discount_chg_{k}d",
                [f"common_ret_{k}d"],
                name=f"{k}d discount change on common return",
            )
        )
        start_discount = panel["discount"] - panel[f"discount_chg_{k}d"]
        normal_discount = panel[
            panel["discount"].between(-1.0, 0.95)
            & start_discount.between(-1.0, 0.95)
        ]
        regressions.append(
            _cluster_ols(
                normal_discount,
                f"discount_chg_{k}d",
                [f"common_ret_{k}d"],
                name=f"{k}d discount change on common return, discount-trimmed",
            )
        )
    for h in [20, 60, 120]:
        regressions.append(
            _cluster_ols(
                panel,
                f"future_pref_rel_ret_{h}d",
                ["discount_z"],
                name=f"future {h}d preferred minus common return on discount z-score",
            )
        )
    lag_table = _distributed_lag_table(panel)

    panel.to_csv(args.output_dir / "preferred_discount_pair_panel.csv", index=False)
    pd.concat(
        [table.assign(horizon_days=k) for k, table in quantiles.items()],
        ignore_index=True,
    ).to_csv(args.output_dir / "preferred_discount_return_quantiles.csv", index=False)
    pd.concat(surges.values(), ignore_index=True).to_csv(
        args.output_dir / "preferred_discount_surge_events.csv", index=False
    )
    pd.DataFrame(
        [
            {
                "name": res.name,
                "nobs": res.nobs,
                "groups": res.groups,
                "r2": res.r2,
                **{f"coef_{k}": v for k, v in res.coefficients.items()},
                **{f"tstat_{k}": v for k, v in res.tstats.items()},
            }
            for res in regressions
        ]
    ).to_csv(args.output_dir / "preferred_discount_regressions.csv", index=False)
    lag_table.to_csv(args.output_dir / "preferred_discount_distributed_lag.csv", index=False)
    _write_report(args.report, panel, quantiles, surges, regressions, lag_table, args.output_dir)

    print(f"wrote {args.report}")
    print(f"panel rows={len(panel):,}, pairs={panel['pair_id'].nunique():,}")
    for res in regressions:
        print(res)


if __name__ == "__main__":
    main()
