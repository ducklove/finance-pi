"""Thin MCP glue for finance-pi.

Targets the official ``mcp`` python SDK (``mcp>=1.0``) via
``mcp.server.fastmcp.FastMCP`` over the stdio transport. The SDK is an
optional dependency (``pip install 'finance-pi[mcp]'``) and is imported
lazily, so importing this module (and the CLI) works without it. The glue is
deliberately small: each tool forwards to the pure logic in
:mod:`finance_pi.mcp_server.tools`, so SDK API drift only touches this file.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

MISSING_MCP_MESSAGE = (
    "The MCP server requires the optional 'mcp' dependency. "
    "Install it with: pip install 'finance-pi[mcp]'"
)


class McpDependencyError(RuntimeError):
    """Raised when the optional ``mcp`` package is not installed."""


def create_server(data_root: Path | str) -> Any:
    """Build a FastMCP server whose tools are bound to *data_root*."""

    try:
        from mcp.server.fastmcp import FastMCP
    except ImportError as exc:
        raise McpDependencyError(MISSING_MCP_MESSAGE) from exc

    from finance_pi.mcp_server import tools

    root = Path(data_root)
    server = FastMCP("finance-pi")

    @server.tool()
    def list_datasets() -> list[dict[str, Any]]:
        """List datasets in the finance-pi Korean-equity data lake.

        Returns name, medallion layer, description, partition scheme, file
        count, and the latest partition date (freshness) per dataset, plus the
        analytics.* convenience views. Use describe_table for columns.
        """

        return tools.list_datasets(root)

    @server.tool()
    def describe_table(view_name: str) -> dict[str, Any]:
        """Describe one catalog view: column names/types plus a description.

        view_name must be schema-qualified, e.g. 'gold.daily_prices_adj',
        'analytics.daily_prices', or 'metadata.datasets'.
        """

        return tools.describe_table(root, view_name)

    @server.tool()
    def query(sql: str, max_rows: int = 200) -> dict[str, Any]:
        """Run a single read-only SELECT/WITH SQL statement on the catalog.

        DuckDB SQL against the medallion views (silver.*, gold.*, analytics.*,
        metadata.datasets). Write/DDL statements are rejected and results are
        capped at max_rows (truncated flag set when more rows exist). Returns
        {columns, rows, row_count_returned, truncated}.
        """

        return tools.query(root, sql, max_rows=max_rows)

    @server.tool()
    def list_factors() -> list[dict[str, Any]]:
        """List registered alpha factors (name, rebalance, direction, inputs)."""

        return tools.list_factors()

    @server.tool()
    def run_backtest(
        factor: str,
        start: str,
        end: str,
        top_fraction: float = 0.1,
        cost_bps: float = 10.0,
        commission_bps: float | None = None,
        sell_tax_bps: float | None = None,
    ) -> dict[str, Any]:
        """Backtest a registered factor and return a compact summary.

        Monthly rebalance between start and end (YYYY-MM-DD). Selects the top
        top_fraction of the universe by factor score. Costs default to a fixed
        cost_bps round trip; passing commission_bps and/or sell_tax_bps
        switches to the Korean trading cost model. Returns final NAV, total
        return, max drawdown, rebalance count, and cost drag - no daily
        series.
        """

        return tools.run_backtest(
            root,
            factor,
            start,
            end,
            top_fraction=top_fraction,
            cost_bps=cost_bps,
            commission_bps=commission_bps,
            sell_tax_bps=sell_tax_bps,
        )

    @server.tool()
    def get_fundamentals(ticker: str, as_of: str) -> dict[str, Any]:
        """Latest annual point-in-time fundamentals for a ticker.

        Returns the account rows (net income, assets, equity, ...) of the most
        recent annual report snapshot visible on or before as_of (YYYY-MM-DD),
        look-ahead free by construction.
        """

        return tools.get_fundamentals(root, ticker, as_of)

    return server


def run_stdio(data_root: Path | str) -> None:
    """Run the MCP server over stdio (blocks until the client disconnects)."""

    create_server(data_root).run(transport="stdio")
