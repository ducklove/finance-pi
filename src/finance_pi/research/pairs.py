"""보통주·우선주 연구: 고정 입력, 다음 관측일 집행, 수량·현금 원장.

수정주가 기준의 연구용 단위다. 원주가·배당·권리 이력이 완비되기 전에는
실제 주수나 총수익 백테스트로 승격하지 않는다.
"""

from __future__ import annotations

import hashlib
import json
import math
import statistics
from collections import deque
from datetime import date, timedelta
from pathlib import Path
from typing import Literal

import polars as pl
from pydantic import BaseModel, ConfigDict, Field, model_validator

from finance_pi.research.etfs import ETF_ENGINE_VERSION

ENGINE_VERSION = "preferred-switch-3"


class PairConfig(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, allow_inf_nan=False, validate_default=True
    )
    strategy: Literal["preferred_switch", "etf_switch"] = "preferred_switch"
    catalog_snapshot_id: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    common: str = Field(pattern=r"^[0-9]{6}$")
    preferred: str = Field(pattern=r"^[0-9A-Z]{6}$")
    start: date
    end: date
    capital: float = Field(default=10_000_000, ge=10_000, le=1_000_000_000)
    window: int = Field(default=126, ge=20, le=252)
    entry_z: float = Field(default=2, ge=0.5, le=5)
    exit_z: float = Field(default=0.5, ge=-2, le=3)
    max_holding: int = Field(default=40, ge=1, le=252)
    commission_bps: float = Field(default=5, ge=0, le=100)
    sell_tax_bps: float = Field(default=20, ge=0, le=100)
    slippage_bps: float = Field(default=10, ge=0, le=200)
    participation: float = Field(default=0.01, gt=0, le=0.05)

    @model_validator(mode="after")
    def validate_range(self):
        if self.common == self.preferred or self.start >= self.end:
            raise ValueError("종목과 연구 기간을 확인해 주세요.")
        if (self.end - self.start).days > 2922 or self.exit_z >= self.entry_z:
            raise ValueError("최대 8년, 복귀 기준은 진입 기준보다 작아야 합니다.")
        return self


def digest(value: object) -> str:
    content = json.dumps(
        value, sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(",", ":")
    )
    return hashlib.sha256(content.encode()).hexdigest()


def pair_list(data_root: Path) -> list[dict]:
    path = data_root / "silver/security_relations/part.parquet"
    if not path.exists():
        return []
    rows = pl.read_parquet(path).to_dicts()
    return [
        {
            "common": str(r["common_security_id"]).removeprefix("S"),
            "preferred": str(r["preferred_security_id"]).removeprefix("S"),
            "name": str(
                r.get("preferred_name") or r.get("preferred_ticker") or r["preferred_security_id"]
            ),
            "confidence": r.get("confidence"),
        }
        for r in rows
        if r.get("confidence") == "high"
    ]


def load_snapshot(data_root: Path, config: PairConfig, *, catalog: dict | None = None) -> dict:
    pairs = (
        [catalog["pair"]]
        if catalog
        else []
        if config.strategy == "etf_switch"
        else pair_list(data_root)
    )
    if not any(p["common"] == config.common and p["preferred"] == config.preferred for p in pairs):
        raise ValueError("해당 전략에서 확인된 연구 대상 쌍이 아닙니다.")
    since = config.start - timedelta(days=550)
    paths = []
    current = since
    while current <= config.end:
        path = data_root / f"gold/daily_prices_adj/dt={current}/part.parquet"
        if path.is_file():
            paths.append(path.as_posix())
        current += timedelta(days=1)
    if not paths:
        raise ValueError("연구 기간의 가격 자료가 없습니다.")
    frame = pl.scan_parquet(paths, hive_partitioning=True)
    fields = frame.collect_schema().names()
    required = {
        "date",
        "security_id",
        "close_adj",
        "trading_value",
        "is_halted",
        "is_designated",
        "is_liquidation_window",
    }
    if not required.issubset(fields):
        raise ValueError("가격·거래상태 필드가 부족합니다. 데이터 재검증이 필요합니다.")
    rows = (
        frame.filter(pl.col("security_id").is_in(["S" + config.common, "S" + config.preferred]))
        .select(
            sorted(required)
            + [(pl.col("volume") if "volume" in fields else pl.lit(None)).alias("volume")]
        )
        .sort("date", "security_id")
        .collect()
        .to_dicts()
    )
    by_date: dict[str, dict] = {}
    for row in rows:
        day = str(row["date"])
        code = row["security_id"].removeprefix("S")
        bucket = by_date.setdefault(day, {})
        if code in bucket:
            raise ValueError("중복 종목·일자 가격을 발견했습니다.")
        price = row["close_adj"]
        value = row["trading_value"]
        if price is None or not math.isfinite(price) or price <= 0:
            raise ValueError("누락되거나 유효하지 않은 가격이 있습니다.")
        flags = [row[f] for f in ("is_halted", "is_designated", "is_liquidation_window")]
        volume = row["volume"]
        basis = "observed_trading_value"
        if value is None or not math.isfinite(value) or value <= 0:
            if volume is not None and math.isfinite(volume) and volume > 0:
                value, basis = float(price) * volume, "adjusted_close_times_volume_proxy"
            elif value is None or not math.isfinite(value) or value < 0:
                value, basis = 0.0, "missing"
        if not math.isfinite(value):
            raise ValueError("유동성 추정값이 유효하지 않습니다.")
        if day >= str(config.start) and basis == "missing" and all(v is False for v in flags):
            raise ValueError("거래 가능한 날의 거래대금·거래량이 모두 누락됐습니다.")
        bucket[code] = {
            "price": float(price),
            "trading_value": float(value)
            if value is not None and math.isfinite(value) and value >= 0
            else 0.0,
            "tradable": all(v is False for v in flags),
            "liquidity_basis": basis,
        }
    bars = []
    market_dates = {Path(p).parent.name.removeprefix("dt=") for p in paths}
    if any(day >= str(config.start) and day not in by_date for day in market_dates):
        raise ValueError("시장 가격이 존재하는 날짜에 양쪽 종목 가격이 모두 누락됐습니다.")
    for day, quotes in sorted(by_date.items()):
        if len(quotes) != 2:
            if day >= str(config.start):
                raise ValueError(f"{day}의 양쪽 가격이 완전하지 않습니다.")
            continue
        bars.append(
            {"date": day, "common": quotes[config.common], "preferred": quotes[config.preferred]}
        )
    evaluation = [b for b in bars if b["date"] >= str(config.start)]
    if len(evaluation) < 2:
        raise ValueError("평가 기간에 최소 두 관측일이 필요합니다.")
    snapshot = {
        "schema_version": 1,
        "source": "finance-pi/gold.daily_prices_adj",
        "price_basis": "adjusted_research_units",
        "common": config.common,
        "preferred": config.preferred,
        "bars": bars,
    }
    if catalog:
        snapshot["catalog"] = catalog
    if config.strategy == "etf_switch":
        snapshot["instrument_review"] = next(
            p for p in pairs if p["common"] == config.common and p["preferred"] == config.preferred
        )
    return {**snapshot, "snapshot_id": digest(snapshot)}


def signals(bars: list[dict], config: PairConfig) -> list[dict]:
    history: deque[float] = deque(maxlen=config.window)
    result = []
    preferred = False
    age = 0
    is_etf = config.strategy == "etf_switch"
    round_trip_bps = 4 * (config.commission_bps + config.slippage_bps) + 2 * config.sell_tax_bps
    for bar in bars:
        discount = 1 - bar["preferred"]["price"] / bar["common"]["price"]
        spread = (
            math.log(bar["common"]["price"] / bar["preferred"]["price"]) if is_etf else discount
        )
        std = statistics.stdev(history) if len(history) == config.window else 0
        mean = statistics.mean(history) if len(history) == config.window else None
        z = (spread - mean) / std if std > 1e-12 else None
        deviation_bps = math.expm1(spread - mean) * 10_000 if is_etf and mean is not None else None
        reason = "ETF A 유지" if is_etf else "보통주 유지"
        if z is None:
            reason = "분포 추정 자료 부족 또는 변동 없음"
        elif not preferred and z >= config.entry_z and is_etf and deviation_bps <= round_trip_bps:
            reason = "상대가격 이탈이 왕복 교체 비용 이하라 유지"
        elif not preferred and z >= config.entry_z:
            preferred, age, reason = True, 0, "할인율 확대"
            if is_etf:
                reason = "ETF B 상대가격 하락·왕복 비용 기준 초과"
        elif preferred and (z <= config.exit_z or age >= config.max_holding):
            preferred, age, reason = False, 0, "분포 복귀 또는 최대 보유기간"
        elif preferred:
            reason = "ETF B 유지" if is_etf else "우선주 유지"
        result.append(
            {
                "date": bar["date"],
                "discount": discount,
                "z": z,
                "target": "preferred" if preferred else "common",
                "reason": reason,
            }
        )
        if is_etf:
            result[-1].update(
                {"relative_deviation_bps": deviation_bps, "round_trip_cost_bps": round_trip_bps}
            )
        history.append(spread)
        age += int(preferred)
    return result


def simulate(
    bars: list[dict],
    signal_rows: list[dict],
    config: PairConfig,
    mode: str,
    cost_multiplier: float = 1,
) -> dict:
    cash = config.capital
    qty = {"common": 0, "preferred": 0}
    targets = None
    last_target = None
    target_signal_date = None
    nav, trades = [], []
    total_cost = 0.0
    commission = config.commission_bps * cost_multiplier / 10_000
    tax = config.sell_tax_bps * cost_multiplier / 10_000
    slip = config.slippage_bps * cost_multiplier / 10_000
    for i, bar in enumerate(bars):
        if bar["date"] < str(config.start):
            continue
        prices = {leg: bar[leg]["price"] for leg in qty}
        # 종료된 전 관측일의 신호만 집행한다. 초기 현금에도 동일하게 적용한다.
        previous = signal_rows[i - 1] if i else None
        target = previous["target"] if previous and previous["z"] is not None else None
        if mode != "switch":
            target = mode if previous and previous["z"] is not None else None
        if target is not None and target != last_target:
            wealth = cash + sum(qty[k] * prices[k] for k in qty)
            weights = (
                {"common": 0.5, "preferred": 0.5}
                if target == "mixed"
                else {
                    "common": float(target == "common"),
                    "preferred": float(target == "preferred"),
                }
            )
            targets = {
                k: int(wealth * weights[k] / (prices[k] * (1 + slip) * (1 + commission)))
                for k in qty
            }
            last_target = target
            target_signal_date = previous["date"]
        used = {k: 0 for k in qty}
        for side in ("sell", "buy"):
            buy_budgets = {}
            if side == "buy" and targets is not None:
                needs = {
                    k: max(0, targets[k] - qty[k]) * prices[k] * (1 + slip) * (1 + commission)
                    for k in qty
                }
                total_need = sum(needs.values())
                buy_budgets = {k: cash * needs[k] / total_need if total_need else 0 for k in qty}
            for leg in qty:
                if targets is None or not bar[leg]["tradable"]:
                    continue
                limit = int(bar[leg]["trading_value"] * config.participation / prices[leg])
                delta = targets[leg] - qty[leg]
                wanted = max(0, delta if side == "buy" else -delta)
                amount = min(wanted, max(0, limit - used[leg]))
                price = prices[leg] * (1 + slip if side == "buy" else 1 - slip)
                if side == "buy":
                    amount = min(
                        amount,
                        int(max(0, min(cash, buy_budgets[leg])) / (price * (1 + commission))),
                    )
                if not amount:
                    continue
                fee = amount * price * (commission + (tax if side == "sell" else 0))
                cost = fee + amount * abs(price - prices[leg])
                cash += amount * price - fee if side == "sell" else -(amount * price + fee)
                qty[leg] += -amount if side == "sell" else amount
                used[leg] += amount
                total_cost += cost
                trades.append(
                    {
                        "date": bar["date"],
                        "signal_date": target_signal_date,
                        "leg": leg,
                        "side": side,
                        "quantity": amount,
                        "price": price,
                        "fee": fee,
                        "cost": cost,
                    }
                )
        equity = cash + sum(qty[k] * prices[k] for k in qty)
        nav.append(
            {
                "date": bar["date"],
                "nav": equity,
                "cash": cash,
                "common_quantity": qty["common"],
                "preferred_quantity": qty["preferred"],
            }
        )
    peak = config.capital
    drawdown = 0.0
    for row in nav:
        peak = max(peak, row["nav"])
        drawdown = min(drawdown, row["nav"] / peak - 1)
    return {
        "mode": mode,
        "nav": nav,
        "trades": trades,
        "return_pct": (nav[-1]["nav"] / config.capital - 1) * 100,
        "max_drawdown_pct": drawdown * 100,
        "cost": total_cost,
        "trade_count": len(trades),
        "ending_cash": cash,
    }


def analyze(snapshot: dict, config: PairConfig) -> dict:
    from finance_pi.research.validation import period_validation

    bars = snapshot["bars"]
    signal_rows = signals(bars, config)
    if not any(s["z"] is not None and s["date"] >= str(config.start) for s in signal_rows):
        raise ValueError("유효한 할인율 분포를 추정할 이력이 부족합니다.")
    scenarios = [
        simulate(bars, signal_rows, config, mode)
        for mode in ("switch", "common", "preferred", "mixed")
    ]
    stress = simulate(bars, signal_rows, config, "switch", 2)
    stress_benchmark = simulate(bars, signal_rows, config, "mixed", 2)
    liquidity_stress = []
    for fraction in (0.5, 0.1):
        constrained = config.model_copy(update={"participation": config.participation * fraction})
        tested = simulate(bars, signal_rows, constrained, "switch", 2)
        benchmark = simulate(bars, signal_rows, constrained, "mixed", 2)
        liquidity_stress.append(
            {
                "participation_multiplier": fraction,
                "participation": constrained.participation,
                "cost_multiplier": 2,
                **{
                    k: tested[k]
                    for k in (
                        "return_pct",
                        "max_drawdown_pct",
                        "cost",
                        "trade_count",
                        "ending_cash",
                    )
                },
                "benchmark_return_pct": benchmark["return_pct"],
                "excess_return_pct": tested["return_pct"] - benchmark["return_pct"],
            }
        )
    return {
        "engine_version": ETF_ENGINE_VERSION if config.strategy == "etf_switch" else ENGINE_VERSION,
        "config": config.model_dump(mode="json"),
        "config_hash": digest(config.model_dump(mode="json")),
        "snapshot": snapshot,
        "scenarios": scenarios,
        "stress": {
            **{k: stress[k] for k in ("return_pct", "max_drawdown_pct", "cost")},
            "benchmark_return_pct": stress_benchmark["return_pct"],
            "excess_return_pct": stress["return_pct"] - stress_benchmark["return_pct"],
        },
        "validation": period_validation(snapshot, config, signal_rows),
        "liquidity_stress": {
            "version": "fixed-signal-capacity-1",
            "scenarios": liquidity_stress,
            "note": (
                "신호와 초기 자금은 고정하고 비용 2배·참여율 50%/10%를 "
                "양쪽 전략에 적용한 체결 민감도입니다. "
                "전략 재최적화나 실거래 합격 판정이 아닙니다."
            ),
        },
        "liquidity_coverage": {
            leg: {
                basis: sum(
                    b[leg].get("liquidity_basis", "observed_trading_value") == basis
                    for b in bars
                    if b["date"] >= str(config.start)
                )
                for basis in (
                    "observed_trading_value",
                    "adjusted_close_times_volume_proxy",
                    "missing",
                )
            }
            for leg in ("common", "preferred")
        },
        "signals": [s for s in signal_rows if s["date"] >= str(config.start)],
        "latest_signal": signal_rows[-1],
        "live_eligible": False,
        "limitations": [
            "수정주가 기준 연구 단위이며 실제 주문 수량이 아닙니다.",
            "현금배당·권리·과거 호가·결제 및 체결순서는 재현하지 않습니다.",
            "현재 확인된 종목 관계로 선정되어 생존편향 가능성이 있습니다.",
            "최종 보유분은 평가 종료이며 청산 비용은 포함하지 않습니다.",
            "거래대금 누락 시 수정종가×거래량을 추정치로 쓰며 실제 체결 한도가 아닙니다.",
            "외부평가·가상 체결·계좌 주문 검증 전에는 실거래로 전환할 수 없습니다.",
        ]
        + (
            [
                "ETF 신호는 과거 로그 가격비의 이탈이며 iNAV 괴리율이나 확정 차익이 아닙니다.",
                "분배금 시점·정책 차이가 미반영되어 등가 노출·총수익 비교가 아닙니다.",
            ]
            if config.strategy == "etf_switch"
            else []
        ),
    }
