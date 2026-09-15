"""현선물 비용 차익 연구. 합성 시나리오와 사용자 호가 재생, 주문 전송 없음."""

import math
from datetime import date, datetime, time, timedelta, timezone
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from finance_pi.research.pairs import digest

VERSION = "cash-futures-1"
KST = timezone(timedelta(hours=9))
SOURCES = [
    "https://global.krx.co.kr/contents/GLB/02/0201/0201040401/GLB0201040401.jsp",
    "https://www.krx.co.kr/contents/OPN/01/01040201/OPN01040201.jsp",
]


class Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False, validate_default=True)


class BasisConfig(Strict):
    kind: Literal["single_stock", "index_etf"] = "single_stock"
    direction: Literal["cash_carry", "reverse_carry"] = "cash_carry"
    spot_code: str = Field(default="005930", pattern=r"^[0-9A-Z]{6}$")
    contract: str = Field(default="SYNTHETIC-SSF", min_length=1, max_length=40)
    expiry: date = date(2026, 12, 10)
    capital: float = Field(default=100_000_000, ge=10_000, le=10_000_000_000)
    multiplier: float = Field(default=10, gt=0, le=1_000_000)
    spot_fee_bps: float = Field(default=2, ge=0, le=100)
    futures_fee_bps: float = Field(default=0.5, ge=0, le=100)
    sell_tax_bps: float = Field(default=20, ge=0, le=100)
    slippage_bps: float = Field(default=2, ge=0, le=100)
    funding_pct: float = Field(default=4, ge=0, le=40)
    borrow_pct: float = Field(default=5, ge=0, le=100)
    initial_margin_pct: float = Field(default=20, ge=1, le=100)
    maintenance_margin_pct: float = Field(default=15, ge=1, le=100)
    reserve_pct: float = Field(default=20, ge=0, le=80)
    min_edge_bps: float = Field(default=10, ge=0, le=1000)
    leg_risk_bps: float = Field(default=10, ge=0, le=1000)
    tracking_risk_bps: float = Field(default=30, ge=0, le=2000)
    expected_dividend: float = Field(default=0, ge=0, le=100_000)
    borrow_confirmed: bool = False
    borrow_shares: int = Field(default=0, ge=0, le=10_000_000)
    max_contracts: int = Field(default=1000, ge=1, le=10000)
    take_profit_fraction: float = Field(default=1, ge=0.1, le=2)
    max_loss_pct: float = Field(default=2, gt=0, le=20)
    participation: float = Field(default=0.1, gt=0, le=1)
    max_quote_age_seconds: float = Field(default=5, gt=0, le=30)
    max_skew_seconds: float = Field(default=2, ge=0, le=5)

    @model_validator(mode="after")
    def contract_rules(self):
        if self.maintenance_margin_pct > self.initial_margin_pct:
            raise ValueError("유지증거금은 개시증거금보다 클 수 없습니다.")
        if self.kind == "single_stock" and self.multiplier != 10:
            raise ValueError(
                "현재는 표준 10주 승수만 지원합니다. 권리조정 계약은 별도 검증이 필요합니다."
            )
        if self.kind == "index_etf" and self.multiplier not in {250_000, 50_000}:
            raise ValueError("지수·ETF 연구 승수는 250000 또는 50000이어야 합니다.")
        return self


class Quote(Strict):
    timestamp: datetime
    spot_at: datetime
    future_at: datetime
    spot_bid: float = Field(gt=0, le=10_000_000)
    spot_ask: float = Field(gt=0, le=10_000_000)
    future_bid: float = Field(gt=0, le=10_000_000)
    future_ask: float = Field(gt=0, le=10_000_000)
    spot_depth: int = Field(ge=0, le=100_000_000)
    future_depth: int = Field(ge=0, le=1_000_000)
    underlying: float = Field(gt=0, le=10_000_000)
    dividend_per_share: float = Field(default=0, ge=0, le=100_000)
    borrow_available: bool = True
    tradable: bool = True
    corporate_action: bool = False
    settlement: float | None = Field(default=None, gt=0, le=10_000_000)

    @model_validator(mode="after")
    def valid_quote(self):
        if any(t.tzinfo is None for t in (self.timestamp, self.spot_at, self.future_at)):
            raise ValueError("호가 시각에 시간대를 지정해야 합니다.")
        if self.spot_bid > self.spot_ask or self.future_bid > self.future_ask:
            raise ValueError("매수 호가가 매도 호가보다 높습니다.")
        if max(self.spot_at, self.future_at) > self.timestamp:
            raise ValueError("관측 시각 이후의 호가를 사용할 수 없습니다.")
        return self


class Scenario(Strict):
    start: date = date(2026, 9, 16)
    spot: float = Field(default=70_000, gt=0, le=10_000_000)
    future: float = Field(default=72_000, gt=0, le=10_000_000)
    underlying: float = Field(default=70_000, gt=0, le=10_000_000)
    spread_bps: float = Field(default=5, ge=0, le=100)
    spot_depth: int = Field(default=100_000, ge=1, le=100_000_000)
    future_depth: int = Field(default=10_000, ge=1, le=1_000_000)


class BasisRequest(Strict):
    mode: Literal["scenario", "replay"] = "scenario"
    config: BasisConfig
    scenario: Scenario | None = None
    quotes: list[Quote] = Field(default_factory=list, max_length=1000)
    source_note: str = Field(default="사용자 가정", min_length=1, max_length=300)

    @model_validator(mode="after")
    def input_mode(self):
        if self.mode == "scenario":
            if self.scenario is None or self.quotes:
                raise ValueError("가정 시뮬레이션에는 scenario만 지정하세요.")
            if not 2 <= (self.config.expiry - self.scenario.start).days <= 365:
                raise ValueError("가정 기간은 2~365일입니다.")
            if (
                self.config.kind == "single_stock"
                and self.scenario.underlying != self.scenario.spot
            ):
                raise ValueError("개별주식선물의 기초자산 가격은 현물과 같아야 합니다.")
        else:
            if self.scenario is not None or len(self.quotes) < 3:
                raise ValueError("호가 재생에는 최소 3개 관측값과 출처가 필요합니다.")
        return self


def eligible(q, c):
    local = q.timestamp.astimezone(KST)
    if q.corporate_action:
        return "권리조정 계약 미지원"
    if not q.tradable:
        return "거래정지 또는 거래 불가"
    regular = time(9) <= local.time() <= time(15, 20)
    settled = q.settlement is not None and time(15, 30) <= local.time() <= time(16)
    if local.weekday() >= 5 or not (regular or settled):
        return "현선물 공통 연속 거래 시간 밖"
    if (
        max((q.timestamp - q.spot_at).total_seconds(), (q.timestamp - q.future_at).total_seconds())
        > c.max_quote_age_seconds
    ):
        return "오래된 호가"
    if abs((q.spot_at - q.future_at).total_seconds()) > c.max_skew_seconds:
        return "현선물 호가 시각 불일치"
    if c.direction == "reverse_carry" and (not c.borrow_confirmed or not q.borrow_available):
        return "대차 확보 미확인 또는 회수"
    return None


def execution(q, c, opening):
    d = 1 if c.direction == "cash_carry" else -1
    buy_spot = (d == 1) == opening
    slip = c.slippage_bps / 10000
    spot = q.spot_ask * (1 + slip) if buy_spot else q.spot_bid * (1 - slip)
    future = q.future_bid * (1 - slip) if buy_spot else q.future_ask * (1 + slip)
    if not opening and q.settlement is not None:
        future = q.settlement
    return spot, future


def fees(spot, future, shares, contracts, c, opening):
    spot_sell = (c.direction == "reverse_carry") == opening
    return (
        shares * spot * (c.spot_fee_bps + (c.sell_tax_bps if spot_sell else 0))
        + contracts * future * c.multiplier * c.futures_fee_bps
    ) / 10000


def plan(q, c):
    """만기 수렴 가정의 보수적 비용 우위. 진입 직전에도 다시 계산한다."""
    d = 1 if c.direction == "cash_carry" else -1
    spot, future = execution(q, c, True)
    mid = (q.spot_bid + q.spot_ask) / 2
    per_contract = c.multiplier if c.kind == "single_stock" else c.multiplier * q.underlying / mid
    notional = per_contract * spot
    margin = future * c.multiplier * c.initial_margin_pct / 100
    days = max(0, (c.expiry - q.timestamp.astimezone(KST).date()).days)
    funding = (notional + margin) * c.funding_pct / 100 * days / 365
    borrow = notional * c.borrow_pct / 100 * days / 365 if d == -1 else 0
    exit_spot, exit_future = execution(q, c, False)
    costs = fees(spot, future, per_contract, 1, c, True)
    costs += fees(exit_spot, q.underlying, per_contract, 1, c, False)
    # 만기 현물 처분도 반대 호가와 슬리피지로 평가한다.
    gross = d * (future - q.underlying) * c.multiplier + d * per_contract * (exit_spot - spot)
    dividend = d * per_contract * c.expected_dividend
    risk = (
        notional * (c.leg_risk_bps + (c.tracking_risk_bps if c.kind == "index_etf" else 0)) / 10000
    )
    net = gross + dividend - funding - borrow - costs - risk
    cash = c.capital * (1 - c.reserve_pct / 100)
    by_capital = math.floor(cash / (notional + margin + costs + funding + borrow))
    by_depth = min(
        math.floor(q.spot_depth * c.participation / per_contract),
        math.floor(q.future_depth * c.participation),
    )
    count = min(by_capital, by_depth, c.max_contracts)
    if d == -1:
        count = min(count, math.floor(c.borrow_shares / per_contract))
    reason = eligible(q, c)
    if days <= 0:
        reason = "만기 당일 신규 진입 금지"
    if not reason and (count < 1 or net / notional * 10000 < c.min_edge_bps):
        reason = "최소 계약·유동성 또는 비용 차감 우위 미충족"
    return {
        "contracts": max(0, count),
        "shares": math.floor(max(0, count) * per_contract),
        "net_per_contract": net,
        "edge_bps": net / notional * 10000,
        "costs_per_contract": costs + funding + borrow + risk,
        "gross_per_contract": gross,
        "dividend_per_contract": dividend,
        "blocked": reason,
        "days": days,
    }


def replay(quotes, c):
    if any(
        q.settlement is not None and q.timestamp.astimezone(KST).time() < time(15, 30)
        for q in quotes
    ):
        raise ValueError("최종정산가격을 장 마감 전에 사용할 수 없습니다.")
    if c.kind == "single_stock" and any(
        not q.spot_bid <= q.underlying <= q.spot_ask for q in quotes
    ):
        raise ValueError("개별주식 기초가격은 동시 현물 호가 범위 안이어야 합니다.")
    if any(b.timestamp <= a.timestamp for a, b in zip(quotes, quotes[1:], strict=False)):
        raise ValueError("호가 시각이 중복되거나 순서가 뒤바뀌었습니다.")
    if (quotes[-1].timestamp - quotes[0].timestamp).days > 366:
        raise ValueError("한 계약 재생은 최대 1년입니다.")
    if any(q.timestamp.astimezone(KST).date() > c.expiry for q in quotes):
        raise ValueError("만기 이후 호가나 다른 월물을 섞을 수 없습니다.")
    if any(
        q.settlement is not None and q.timestamp.astimezone(KST).date() != c.expiry for q in quotes
    ):
        raise ValueError("최종정산가격은 만기 날짜에만 입력하세요.")
    d = 1 if c.direction == "cash_carry" else -1
    entry = None
    pending = None
    events, ledger = [], []
    costs = dividends = 0.0
    peak_required, peak = 0.0, c.capital
    max_dd = 0.0
    latest_plan = plan(quotes[0], c)
    status = "no_trade"
    unresolved = False
    for i, q in enumerate(quotes):
        reason = eligible(q, c)
        if entry is None:
            latest_plan = plan(q, c)
            if pending is not None and not latest_plan["blocked"]:
                n, shares = latest_plan["contracts"], latest_plan["shares"]
                sp, fp = execution(q, c, True)
                initial_margin = fp * n * c.multiplier * c.initial_margin_pct / 100
                costs = fees(sp, fp, shares, n, c, True)
                entry = {
                    "spot": sp,
                    "future": fp,
                    "shares": shares,
                    "contracts": n,
                    "at": q.timestamp,
                    "initial_margin": initial_margin,
                    "reserve": shares * sp,
                    "signal_at": pending,
                    "delta_residual_won": shares * (q.spot_bid + q.spot_ask) / 2
                    - n * c.multiplier * q.underlying,
                }
                events.append(
                    {
                        "type": "paired_entry",
                        "at": q.timestamp.isoformat(),
                        "signal_at": pending.isoformat(),
                        "contracts": n,
                        "shares": shares,
                        "legs": [
                            {
                                "asset": "spot",
                                "symbol": c.spot_code,
                                "side": "buy" if d == 1 else "sell",
                                "quantity": shares,
                                "price": sp,
                            },
                            {
                                "asset": "future",
                                "symbol": c.contract,
                                "side": "sell" if d == 1 else "buy",
                                "quantity": n,
                                "price": fp,
                            },
                        ],
                    }
                )
                status = "open"
                pending = None
            elif not latest_plan["blocked"]:
                pending = q.timestamp
            else:
                pending = None
                if i == 0 or (events and events[-1].get("reason") != latest_plan["blocked"]):
                    events.append(
                        {
                            "type": "blocked",
                            "at": q.timestamp.isoformat(),
                            "reason": latest_plan["blocked"],
                        }
                    )
            if entry is None:
                continue
        elif i:
            elapsed = (q.timestamp - quotes[i - 1].timestamp).total_seconds() / 86400
            funding_base = entry["reserve"] + entry["initial_margin"]
            costs += funding_base * c.funding_pct / 100 * elapsed / 365
            if d == -1:
                costs += entry["reserve"] * c.borrow_pct / 100 * elapsed / 365
            dividends += d * entry["shares"] * q.dividend_per_share
        if q.corporate_action:
            events.append({"type": "halted", "at": q.timestamp.isoformat(), "reason": reason})
            unresolved, status = True, "unresolved"
            break
        # 비동기·오래된 호가로 정산이나 청산 결과를 만들지 않는다.
        if reason and reason != "대차 확보 미확인 또는 회수":
            unresolved = True
            continue
        sp, fp = execution(q, c, False)
        future_pnl = -d * entry["contracts"] * c.multiplier * (fp - entry["future"])
        spot_pnl = d * entry["shares"] * (sp - entry["spot"])
        exit_cost = fees(sp, fp, entry["shares"], entry["contracts"], c, False)
        pnl = future_pnl + spot_pnl + dividends - costs - exit_cost
        required = (
            entry["reserve"]
            + fp * entry["contracts"] * c.multiplier * c.initial_margin_pct / 100
            - future_pnl
            + costs
            - dividends
        )
        peak_required = max(peak_required, required)
        liquid = c.capital - entry["reserve"] + future_pnl - costs + dividends
        maintenance = fp * entry["contracts"] * c.multiplier * c.maintenance_margin_pct / 100
        margin_failure = liquid < maintenance
        end = i == len(quotes) - 1 or q.settlement is not None
        recall = d == -1 and not q.borrow_available
        equity = c.capital + pnl
        peak = max(peak, equity)
        max_dd = min(max_dd, (equity / peak - 1) * 100)
        ledger.append(
            {
                "at": q.timestamp.isoformat(),
                "equity": equity,
                "pnl": pnl,
                "spot_pnl": spot_pnl,
                "future_pnl": future_pnl,
                "dividends": dividends,
                "costs": costs + exit_cost,
                "liquid_cash": liquid,
                "maintenance_required": maintenance,
                "initial_capital_required": required,
            }
        )
        take_profit = (
            q.timestamp > entry["at"]
            and pnl >= latest_plan["net_per_contract"] * entry["contracts"] * c.take_profit_fraction
        )
        stop_loss = pnl <= -c.capital * c.max_loss_pct / 100
        if margin_failure or recall or end or take_profit or stop_loss:
            enough = q.spot_depth * c.participation >= entry["shares"] and (
                q.settlement is not None or q.future_depth * c.participation >= entry["contracts"]
            )
            if not enough:
                unresolved, status = True, "unresolved"
                events.append(
                    {
                        "type": "exit_blocked",
                        "at": q.timestamp.isoformat(),
                        "reason": "청산 호가 수량 부족",
                    }
                )
                break
            status = (
                "margin_liquidation"
                if margin_failure
                else "borrow_recall"
                if recall
                else "stop_loss"
                if stop_loss
                else "take_profit"
                if take_profit
                else "closed"
            )
            events.append(
                {
                    "type": status,
                    "at": q.timestamp.isoformat(),
                    "legs": [
                        {
                            "asset": "spot",
                            "symbol": c.spot_code,
                            "side": "sell" if d == 1 else "buy",
                            "quantity": entry["shares"],
                            "price": sp,
                        },
                        {
                            "asset": "future",
                            "symbol": c.contract,
                            "side": "cash_settlement"
                            if q.settlement is not None
                            else "buy"
                            if d == 1
                            else "sell",
                            "quantity": entry["contracts"],
                            "price": fp,
                        },
                    ],
                }
            )
            unresolved = False
            break
    if entry and status == "open":
        unresolved, status = True, "unresolved"
    pnl = ledger[-1]["pnl"] if ledger and not unresolved else None
    return {
        "status": status,
        "pnl": pnl,
        "return_on_capital_pct": pnl / c.capital * 100 if pnl is not None else None,
        "max_drawdown_pct": max_dd if ledger else None,
        "peak_capital_required": peak_required,
        "additional_capital": max(0, peak_required - c.capital),
        "plan": latest_plan,
        "events": events,
        "ledger": ledger,
        "contracts": entry["contracts"] if entry else 0,
        "shares": entry["shares"] if entry else 0,
        "delta_residual_won": entry["delta_residual_won"] if entry else 0,
        "unhedged_1pct_move_loss": max(
            entry["reserve"], entry["future"] * entry["contracts"] * c.multiplier
        )
        * 0.01
        if entry
        else 0,
        "observations": len(quotes),
        "orders_sent": 0,
        "realized": status
        in {"closed", "margin_liquidation", "borrow_recall", "take_profit", "stop_loss"},
    }


def synthetic(s, c, *, shock=0.0, dividend_scale=1.0, tracking=0.0):
    days = (c.expiry - s.start).days
    result = []
    dates = [
        s.start + timedelta(days=i)
        for i in range(days + 1)
        if (s.start + timedelta(days=i)).weekday() < 5
    ]
    if not dates or dates[-1] != c.expiry:
        raise ValueError("가정 만기일은 평일이어야 합니다. 실제 거래일 달력은 별도 검증하세요.")
    for i, day in enumerate(dates):
        t = i / (len(dates) - 1)
        move = shock * math.sin(math.pi * t)
        underlying = s.underlying * (1 + move)
        spot = s.spot * (1 + move + tracking * t)
        future = underlying + (s.future - s.underlying) * (1 - t)
        # 첫 신호 다음 호가의 진입 가능성을 분리한다.
        stamp = datetime.combine(day, time(15, 40) if day == c.expiry else time(14, 59), KST)
        half = s.spread_bps / 20000
        result.append(
            Quote(
                timestamp=stamp,
                spot_at=stamp,
                future_at=stamp,
                spot_bid=spot * (1 - half),
                spot_ask=spot * (1 + half),
                future_bid=future * (1 - half),
                future_ask=future * (1 + half),
                spot_depth=s.spot_depth,
                future_depth=s.future_depth,
                underlying=underlying,
                dividend_per_share=c.expected_dividend * dividend_scale
                if i == len(dates) // 2
                else 0,
                settlement=underlying if day == c.expiry else None,
            )
        )
    return result


def analyze_basis(payload):
    request = BasisRequest.model_validate(payload)
    c = request.config
    if request.mode == "scenario":
        cases = [
            ("base", "만기 수렴 가정", c, {}),
            (
                "double_cost",
                "수수료·슬리피지 2배",
                c.model_copy(
                    update={
                        "spot_fee_bps": c.spot_fee_bps * 2,
                        "futures_fee_bps": c.futures_fee_bps * 2,
                        "slippage_bps": c.slippage_bps * 2,
                    }
                ),
                {},
            ),
            ("dividend_miss", "예상 배당 미수취", c, {"dividend_scale": 0}),
            (
                "margin_shock",
                "중간 기초자산 30% 불리한 변동",
                c,
                {"shock": 0.3 if c.direction == "cash_carry" else -0.3},
            ),
        ]
        if c.kind == "index_etf":
            cases.append(
                (
                    "tracking",
                    "ETF 만기 추적 차이 1% 불리",
                    c,
                    {"tracking": -0.01 if c.direction == "cash_carry" else 0.01},
                )
            )
        scenarios = [
            {"id": key, "name": name, **replay(synthetic(request.scenario, conf, **kw), conf)}
            for key, name, conf, kw in cases
        ]
        example = synthetic(request.scenario, c)
    else:
        scenarios = [{"id": "replay", "name": "사용자 제공 호가 재생", **replay(request.quotes, c)}]
        example = request.quotes
    body = {
        "engine_version": VERSION,
        "input_hash": digest(payload),
        "config": c.model_dump(mode="json"),
        "mode": request.mode,
        "source_note": request.source_note,
        "source_verified": False,
        "scenarios": scenarios,
        "live_eligible": False,
        "orders_sent": 0,
        "replay_input": {
            "mode": "replay",
            "config": c.model_dump(mode="json"),
            "source_note": "합성 예시 · 실제 시세 아님"
            if request.mode == "scenario"
            else request.source_note,
            "quotes": [q.model_dump(mode="json") for q in example],
        },
        "limitations": [
            "가정 결과는 과거 실적·수익률 분포 또는 반복 가능한 연 수익이 아닙니다.",
            "사용자 호가는 원본 대조 전입니다. 실제 동시 호가·월물·배당·증거금 이력이 필요합니다.",
            "다음 관측 호가로 양쪽 수량을 맞추는 가정입니다. 실제 동시 체결은 미검증입니다.",
            "정산 후 현물은 별도 처분합니다. 정산가격 차이와 현금 부족으로 손실이 날 수 있습니다.",
            "증거금률·비용·대차·배당은 사용자 가정입니다. 투자자별 양도소득세 등은 제외합니다.",
            "현물 전액·개시증거금에 단리 기회비용을 적용하며 공매도 대금은 재사용하지 않습니다.",
            "공매도에는 현물 100% 담보를 가정합니다. 추가 증거금 조달비용은 별도입니다.",
            "휴장일·호가단위·권리조정·ETF 관계는 계약 마스터 검증 전입니다. 실주문은 비활성입니다.",
            "관측 사이 호가·증거금 경로는 미확인입니다. 추가 자금은 초기증거금 복원 기준입니다.",
            "만기에는 정산 확인 후 당일 현물 처분을 가정합니다. 결제일 현금 잠김은 별도입니다.",
        ],
        "spec_sources": SOURCES,
    }
    return {**body, "result_hash": digest(body)}
