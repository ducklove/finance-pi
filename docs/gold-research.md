# Gold investment research API

finance-pi owns collection, normalization, and versioned publication for the sibling `all-about-gold` service. This is a separate research projection; it does not change the equity lake's Bronze/Silver/Gold tables, catalog, or existing daily schedule.

## Refresh and serve

```sh
.venv/bin/python -m finance_pi.research.gold --root .
.venv/bin/python -m finance_pi.cli.app admin --root . --host 127.0.0.1 --port 8401
```

On a Pi serving the LAN, use the existing admin service and its configured port after deploying the new source code. Refresh once on that host before requesting the endpoint. No new timer is registered automatically. Refresh is an explicit command, separate from the existing daily pipeline.

`GET /api/research/gold` returns the current complete snapshot. It never fetches vendors during a request. Existing admin read authentication applies: loopback/LAN access is permitted under the existing policy, and other clients require `X-Admin-Token`. Missing snapshot returns 404; failed authorization returns 401. The endpoint is also listed in the admin API documentation.

```json
{
  "schemaVersion": 1,
  "provider": "finance-pi",
  "publishedAt": "2026-09-26T00:27:46.427826+00:00",
  "history": {"schemaVersion": 2, "assets": []},
  "trends": {"schemaVersion": 1, "mining": [], "reserves": []},
  "research": {},
  "marketSize": {"schemaVersion": 1}
}
```

The example omits observations. All dates inside price points use `YYYY-MM`; annual observations use `YYYY-12`. Mining describes calendar-year output; reserves describe year-end volume. Prices, tonnes and ratios are distinct measures and must retain their labels.

A successful full refresh atomically replaces `data/research/gold/current.json` and saves a timestamped generation in `data/research/gold/releases/`. Failed collection leaves current unchanged. Consumers should display the publication and source dates; serving a prior successful generation does not make it live market data. Immutable releases retain the published normalized observations and source hashes, not every raw HTTP response.

## Sources and definitions

| Dataset | Coverage at initial publication | Definition |
| --- | --- | --- |
| World Bank gold/silver | Jan 1960–Aug 2026 | Monthly average USD per troy ounce, Pink Sheet |
| Yahoo BTC-USD | Oct 2014–Aug 2026 | Mean of available daily closes |
| Yahoo DX-Y.NYB | Jan 1971–Aug 2026 | Mean of available daily closes, index |
| Yahoo KRW=X | Dec 2003–Aug 2026 | Mean of available daily closes, KRW/USD |
| USGS DS140 mining | 1900–2022 | Annual world and US output, metric tonnes |
| IMF IFS gold volumes | 1950–2024; China 1977–2024 | Annual US, DE, IT, FR, CN, KR; via DBnomics |
| Reviewed research | Per-record dates | WGC supply, USGS country output, official holdings snapshots, ETF references |

- [World Bank commodity markets](https://www.worldbank.org/en/research/commodity-markets): the gold pricing definition changes in June 2025 from London afternoon fixing to average daily spot. Silver has earlier historical basis changes. Values are nominal, not inflation adjusted.
- [USGS gold historical statistics](https://www.usgs.gov/media/files/gold-historical-statistics-data-series-140): 2022 edition, calculated/estimated/reported quantities. Do not splice later WGC or USGS snapshot totals into this series.
- [Example IMF IFS series distributed by DBnomics](https://db.nomics.world/IMF/IFS/A.US.RAFAGOLDV_OZT): source units are **millions of fine troy ounces**. Multiply by **31.1034768** for metric tonnes. Missing values are excluded; observed zero is preserved. These figures may include gold deposits/swaps and differ from individually published reserve snapshots.
- Yahoo data excludes the current calendar month. A first month starting after day 7 is excluded to avoid the initial partial BTC month. Monthly means use available source observations, without gap filling or a claim of complete daily coverage.

The UI can align on the union of months to preserve long gold history; pairwise ratios must intersect only the two relevant series. It must not backfill BTC before inception or turn missing observations into zero.

## Maintenance

Source adapters live in `src/finance_pi/sources/gold/`. They use the existing retrying `HttpJsonClient`; XLSX parsing uses Python ZIP/XML readers, with no extra spreadsheet dependency. The World Bank and USGS file URLs are explicit to keep the reviewed source editions reproducible; maintainers should review new source editions before changing them.

`seed_research.py` owns the reviewed WGC/USGS/current-country/ETF snapshot definitions. Update values, dates and source links together, run the script to write `research.json`, then run the full refresh. It does not discover new releases automatically. KRX/tax explanations in the consumer should remain tied to their cited official sources and applicable investor/account conditions.

`all-about-gold` proxies this endpoint through `/api/gold`; configure its server using `FINANCE_PI_BASE_URL` and, where required, `FINANCE_PI_ADMIN_TOKEN`. Browsers receive neither a vendor URL request nor credentials. The consumer's export script creates inspection copies only and is not the runtime data path.

## Tests

```sh
.venv/bin/pytest tests/unit/test_gold_sources.py tests/unit/test_gold_research.py tests/unit/test_admin.py
.venv/bin/ruff check src/finance_pi/sources/gold src/finance_pi/research/gold.py tests/unit/test_gold_sources.py tests/unit/test_gold_research.py
```

Tests cover price aggregation, partial/current months, missing values, IMF unit conversion, atomic refresh failure preservation, endpoint reads, missing snapshots and external authorization.

## Annual market size projection

`marketSize` contains stock, mining, marketCap, usDebt, goldDebtRatioPct, miningStockRatioPct and stockToFlowYears arrays, plus inputs, methodology and sources. Stock (1900–2025) is a reconstruction, not observed WGC history: anchor WGC/Metals Focus end-2025 at 219,891 tonnes, subtract subsequent annual USGS mining; assume no permanent losses and exclude recycled supply. The separate DS140 chart remains unchanged. This model extends DS140 with reviewed MCS 2025 output for 2023 and MCS 2026 output for 2024–2025.

Market cap (1960–2025) is reconstructed end-year stock × 1,000,000 / 31.1034768 × World Bank December monthly-average USD price. It is not year-end closing market value. Debt (1993–2025) is Treasury Debt to the Penny Total Public Debt Outstanding at the final available December business date: nominal face amount including intragovernmental holdings. Gold/debt intersects available years; it compares global gold to US debt, not US gold collateral. Mining/stock divides calendar-year production by end-year stock; its reciprocal is not reserve depletion lifetime. Current incomplete calendar years are excluded.

Review `market_size_inputs.json` annually against linked WGC and USGS publications; update anchor, extensions, review date and consumer methodology together. Treasury observations refresh automatically. Missing production years or failed Treasury retrieval abort the complete publication and preserve the previous snapshot. Tests in `test_gold_market_size.py` cover unit conversion, calendar alignment, reconstruction, pagination completeness and atomic failure preservation.
