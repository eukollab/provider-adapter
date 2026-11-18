# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""The seller's MCP server: one tool, "Validate EU VAT numbers". Nothing in it is about EuKollab: the connector fronts
it, and its settings on the network are in eukollab.json beside the agent.

Run: python server.py (it serves http://localhost:8000/mcp)
"""

import os
from typing import Any

from mcp.server.mcpserver import MCPServer

server = MCPServer("vat-check", title="VAT Check", description="Checks EU VAT numbers.")


@server.tool(title="Validate EU VAT numbers", annotations={"readOnlyHint": True})
def validate_vat(vat_number: str) -> dict[str, Any]:
    """Validate EU VAT numbers: the format of an EU VAT number, and its country."""
    cleaned = vat_number.replace(" ", "").upper()
    return {"vat_number": cleaned, "country": cleaned[:2], "valid": len(cleaned) > 4 and cleaned[:2].isalpha()}


if __name__ == "__main__":
    import uvicorn

    app = server.streamable_http_app(host=os.environ.get("HOST", "127.0.0.1"))
    uvicorn.run(app, host=os.environ.get("HOST", "127.0.0.1"), port=int(os.environ.get("PORT", "8000")))
