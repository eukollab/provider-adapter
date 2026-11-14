# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""The seller: an MCP server offering "Validate EU VAT numbers", which answers matching needs with its offer.

Run: uvicorn seller:app --host 0.0.0.0 --port 8000 (with agent.json beside it, EKL_PROVIDER_CREDENTIAL,
EKL_AGENT_CREDENTIAL and EKL_NETWORK_URL).
"""

from typing import Any

from mcp.server.mcpserver import MCPServer

from ekl_provider_adapter import EuKollab, attach, service

# It keeps nothing it receives or returns. Declared, so buyers whose rules require exactly that will call it.
server = MCPServer(
    "vat-check",
    extensions=[
        EuKollab(
            data={
                "inputs": {"retention": "none"},
                "results": {"retention": "none"},
                "downstream": {"retention": "none", "subprocessors": []},
                "used_for_training": False,
            }
        )
    ],
)


# What MCP can't say about the tool: it only reads, so `ekl agent init` offers it without asking; and its category,
# tax (which covers VAT), is the tag a need posted with the topic "vat" is matched on.
@server.tool(
    meta=service(
        class_="read_only", category="tax", limitations="Checks the number's format only, not the VAT register."
    )
)
def validate_vat(vat_number: str) -> dict[str, Any]:
    """Validate EU VAT numbers: the format of an EU VAT number, and its country."""
    cleaned = vat_number.replace(" ", "").upper()
    return {"vat_number": cleaned, "country": cleaned[:2], "valid": len(cleaned) > 4 and cleaned[:2].isalpha()}


app = attach(server)


@app.agent.on_need(matching="my_services")
def offer(need: Any) -> None:
    service = need.matched_service
    need.reply(f"I can do this: {service.name}, {service.price}. Connect and call validate_vat.")
