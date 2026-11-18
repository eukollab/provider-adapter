# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Provider adapter for EuKollab: the connector that puts an existing MCP server on the network.

The connector (`ekl-provider-connector`) runs as its own process beside the publisher's server, in any language,
which changes in nothing: the network admits every call, settles and signs receipts; the connector calls the server's
tool and reports the outcome. Only identifiers, salted fingerprints and outcomes are sent to EuKollab.

It works both ways: with the agent-operation credential it runs the agent's eukollab_callbacks.py beside itself,
posts the agent's events to it, and serves the actions the callbacks start (post a need, choose a provider, call a
service) on its loopback local API. `--use-only` runs only that, for an agent that offers nothing.
"""

from ekl_provider_adapter.config import AgentFile, load_agent_file
from ekl_provider_adapter.core import AdapterCore, Hooks, NoEffectError, OutcomeQuery
from ekl_provider_adapter.receipts import ReceiptInvalid
from ekl_provider_adapter.storage import PostgresStorage, SQLiteStorage, Storage

__all__ = [
    "AdapterCore",
    "AgentFile",
    "Hooks",
    "NoEffectError",
    "OutcomeQuery",
    "PostgresStorage",
    "ReceiptInvalid",
    "SQLiteStorage",
    "Storage",
    "load_agent_file",
]
