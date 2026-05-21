# 우선주 괴리율과 보통주 급등 속도 분석

## 데이터

- 범위: 1990-01-03 ~ 2026-05-12
- 보통주-우선주 매칭: 113개 우선주 쌍, 99개 보통주
- 일별 쌍 관측치: 765,797개
- 가격: `data/gold/daily_prices_adj`, 조정종가 기준
- 매칭 방식: 우선주 티커 앞 5자리 + `0`을 보통주 티커로 사용

## 핵심 결과

1. 보통주의 단기 상승률이 클수록 같은 기간 우선주 괴리율은 커졌다.
   20거래일 보통주 수익률 하위 10% 구간의 평균 괴리율 변화는 -8.06%였고,
   상위 10% 구간은 7.88%였다.
   중앙값 기준으로도 하위 10%는 -3.18%,
   상위 10%는 3.65%였다.

2. 급등 직후에는 우선주가 뒤늦게 따라붙는 패턴이 관찰됐다.
   보통주가 20거래일에 30% 이상 상승한 사건에서, 이후 60거래일 우선주 초과수익 평균은
   8.43%,
   괴리율 변화 평균은
   -5.66%였다.

3. 분산시차 회귀에서 보통주 일간 수익률에 대한 우선주 반응은 당일 반응만으로 끝나지 않았다.
   당일 베타는 0.994,
   20거래일까지 누적 베타는 1.035였다.
   이는 "같은 방향으로 움직이지만 느슨하고 지연된 커플링"이라는 가설과 부합한다.

## 회귀 요약

- 5d discount change on common return: n=765,232, pairs=113, R2=0.005, common_ret_5d=0.1554 (t=2.49)
- 5d discount change on common return, discount-trimmed: n=689,830, pairs=113, R2=0.029, common_ret_5d=0.0691 (t=2.48)
- 20d discount change on common return: n=763,537, pairs=113, R2=0.005, common_ret_20d=0.1381 (t=2.57)
- 20d discount change on common return, discount-trimmed: n=686,262, pairs=113, R2=0.024, common_ret_20d=0.0512 (t=2.61)
- 60d discount change on common return: n=759,017, pairs=113, R2=0.004, common_ret_60d=0.1075 (t=3.09)
- 60d discount change on common return, discount-trimmed: n=679,811, pairs=113, R2=0.018, common_ret_60d=0.0405 (t=3.06)
- future 20d preferred minus common return on discount z-score: n=763,537, pairs=113, R2=0.008, discount_z=0.0159 (t=20.42)
- future 60d preferred minus common return on discount z-score: n=759,017, pairs=113, R2=0.016, discount_z=0.0450 (t=20.53)
- future 120d preferred minus common return on discount z-score: n=752,237, pairs=113, R2=0.028, discount_z=0.0836 (t=20.26)

해석 기준: 괴리율 변화 회귀에서 양의 계수는 보통주가 빠르게 오를수록 우선주가 덜 따라와 할인 폭이 확대됨을 뜻한다.
미래 우선주 초과수익 회귀에서 양의 계수는 현재 괴리율이 높을수록 이후 우선주가 보통주 대비 더 강하게 움직임을 뜻한다.

## 산출물

- 일별 패널: `data/reports/preferred_discount_pair_panel.csv`
- 분위수 표: `data/reports/preferred_discount_return_quantiles.csv`
- 급등 이벤트 표: `data/reports/preferred_discount_surge_events.csv`
- 회귀 표: `data/reports/preferred_discount_regressions.csv`
- 분산시차 표: `data/reports/preferred_discount_distributed_lag.csv`

## 주의점

- 우선주는 유동성이 낮고 가격제한폭, 거래정지, 지정종목, 상장폐지 가능성의 영향을 크게 받는다.
- 여기서는 거래비용, 세금, 슬리피지, 체결 가능 수량을 반영하지 않았다.
- 통계적 관계가 존재하더라도 차익거래 가능성을 의미하지는 않는다.
