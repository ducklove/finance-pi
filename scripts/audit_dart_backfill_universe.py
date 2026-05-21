from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--since", default="1999-01-01")
    parser.add_argument("--until", default="2015-06-23")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/reports/dart_backfill"),
    )
    args = parser.parse_args()

    root = args.root.resolve()
    output_dir = args.output_dir if args.output_dir.is_absolute() else root / args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    filings = root / "data" / "bronze" / "dart_filings" / "dt=*" / "part.parquet"
    identity = root / "data" / "silver" / "security_identity" / "*.parquet"

    where_clause = f"""
    from read_parquet('{filings}', hive_partitioning=true, union_by_name=true)
    where cast(rcept_dt as date) between date '{args.since}' and date '{args.until}'
      and stock_code is not null
      and report_nm like '%사업보고서%'
    """

    universe_path = output_dir / "dart_annual_report_universe_1999_2015.csv"
    by_year_path = output_dir / "dart_annual_report_universe_by_year_1999_2015.csv"
    identity_path = output_dir / "dart_annual_report_universe_with_identity_1999_2015.csv"

    con.sql(
        f"""
        copy (
          select
            cast(rcept_dt as date) as rcept_dt,
            corp_code,
            corp_name,
            lpad(cast(stock_code as varchar), 6, '0') as stock_code,
            rcept_no,
            report_nm
          {where_clause}
          order by rcept_dt, stock_code, rcept_no
        ) to '{universe_path}' (header, delimiter ',')
        """
    )
    con.sql(
        f"""
        copy (
          select
            extract(year from cast(rcept_dt as date)) as filing_year,
            count(distinct rcept_no) as filings,
            count(distinct lpad(cast(stock_code as varchar), 6, '0')) as stock_codes,
            min(cast(rcept_dt as date)) as first_rcept_dt,
            max(cast(rcept_dt as date)) as last_rcept_dt
          {where_clause}
          group by 1
          order by 1
        ) to '{by_year_path}' (header, delimiter ',')
        """
    )
    con.sql(
        f"""
        copy (
          with u as (
            select
              cast(rcept_dt as date) as rcept_dt,
              corp_code,
              corp_name,
              lpad(cast(stock_code as varchar), 6, '0') as stock_code,
              rcept_no,
              report_nm
            {where_clause}
          ),
          sm as (
            select
              ticker,
              name as security_name,
              market,
              listed_date,
              delisted_date,
              security_id
            from read_parquet('{identity}')
          )
          select
            u.*,
            sm.security_id,
            sm.security_name,
            sm.market,
            sm.listed_date,
            sm.delisted_date,
            sm.security_id is null as missing_security_identity
          from u
          left join sm on sm.ticker = u.stock_code
          order by u.rcept_dt, u.stock_code, u.rcept_no
        ) to '{identity_path}' (header, delimiter ',')
        """
    )
    print(f"wrote {universe_path}")
    print(f"wrote {by_year_path}")
    print(f"wrote {identity_path}")
    summary = con.sql(f"select * from read_csv_auto('{by_year_path}')").fetchdf()
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
