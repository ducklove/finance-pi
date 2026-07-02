"""MCP (Model Context Protocol) server exposing the finance-pi data lake.

``finance_pi.mcp_server.tools`` holds the pure tool logic (no ``mcp``
dependency); ``finance_pi.mcp_server.server`` is the thin FastMCP glue that
requires the optional extra (``pip install finance-pi[mcp]``).
"""
