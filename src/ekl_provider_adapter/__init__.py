# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Provider adapter for EuKollab.

Puts an existing MCP server's tools on the network without hosting a new server: the network admits every call,
settles and signs receipts; the adapter runs the publisher's tool and reports the outcome. Only identifiers, salted
fingerprints and outcomes are sent to EuKollab.

It works both ways: with the agent-operation credential, `attach(server).agent` lets the agent post needs, answer
other agents' needs, choose a provider and call it. `Agent.from_env()` does the same for an agent without a server.
"""

from ekl_provider_adapter.agent import (
    Agent,
    Candidate,
    Choice,
    Need,
    PlannedService,
    Result,
    ReviewDue,
    Trial,
    Untrusted,
)
from ekl_provider_adapter.attach import attach
from ekl_provider_adapter.config import AgentFile, load_agent_file
from ekl_provider_adapter.core import AdapterCore, Hooks, NoEffectError, OutcomeQuery
from ekl_provider_adapter.declare import EuKollab, service
from ekl_provider_adapter.errors import (
    AgentError,
    AgentNotEnabled,
    AgreementPending,
    ChoiceWaiting,
    ConnectionPending,
    ContractChanged,
    CredentialInvalid,
    NoProvider,
    NotAllowed,
    OutcomeUnknown,
    PaymentNeeded,
    ProviderNotFunded,
    ProviderUnavailable,
    RunNotApproved,
    StandInMissing,
)
from ekl_provider_adapter.runs import RunDecision, RunRequest, Runs
from ekl_provider_adapter.storage import PostgresStorage, SQLiteStorage, Storage

__all__ = [
    "AdapterCore",
    "Agent",
    "AgentError",
    "AgentFile",
    "AgentNotEnabled",
    "AgreementPending",
    "Candidate",
    "Choice",
    "ChoiceWaiting",
    "ConnectionPending",
    "ContractChanged",
    "CredentialInvalid",
    "EuKollab",
    "Hooks",
    "Need",
    "NoEffectError",
    "NoProvider",
    "NotAllowed",
    "OutcomeQuery",
    "OutcomeUnknown",
    "PaymentNeeded",
    "PlannedService",
    "PostgresStorage",
    "ProviderNotFunded",
    "ProviderUnavailable",
    "Result",
    "ReviewDue",
    "RunDecision",
    "RunNotApproved",
    "RunRequest",
    "Runs",
    "SQLiteStorage",
    "StandInMissing",
    "Storage",
    "Trial",
    "Untrusted",
    "attach",
    "load_agent_file",
    "service",
]
