# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""The seller: an MCP server offering "Validate EU VAT numbers", which answers matching needs with its offer.

Run: uvicorn seller:app --host 0.0.0.0 --port 8000 (with agent.json beside it, EKL_PROVIDER_CREDENTIAL,
EKL_AGENT_CREDENTIAL and EKL_NETWORK_URL).
"""

from typing import Any

from mcp.server.mcpserver import MCPServer

from ekl_provider_adapter import attach

server = MCPServer("vat-check")


@server.tool()
def validate_vat(vat_number: str) -> dict[str, Any]:
    """Validate EU VAT numbers: the format of an EU VAT number, and its country."""
    cleaned = vat_number.replace(" ", "").upper()
    return {"vat_number": cleaned, "country": cleaned[:2], "valid": len(cleaned) > 4 and cleaned[:2].isalpha()}


app = attach(server)


@app.agent.on_need(matching="my_services")
def offer(need: Any) -> None:
    service = need.matched_service
    need.reply(f"I can do this: {service.name}, {service.price}. Connect and call validate_vat.")
