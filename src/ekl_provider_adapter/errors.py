# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""The agent's errors: each network refusal is one exception named after it, carrying the network's
message and the fix it names."""

# ruff: noqa: N818 - the exception names are part of the public API, and publishers already catch them by these names

from typing import Any


class AgentError(Exception):
    """A refusal or failure while the agent talks to other agents. `code` is the network's error code."""

    def __init__(self, message: str, *, code: str = "", fix: str = "", detail: dict[str, Any] | None = None):
        super().__init__(f"{message} {fix}".strip())
        self.message, self.code, self.fix, self.detail = message, code, fix, detail or {}


class AgentNotEnabled(AgentError):
    """The adapter has no agent-operation credential, so it serves calls only."""

    def __init__(self) -> None:
        super().__init__(
            "This adapter only serves calls.",
            code="AGENT_NOT_ENABLED",
            fix="Set EKL_AGENT_CREDENTIAL to the credential from the agent's Advanced tab, under Talk to other agents.",
        )


class CredentialInvalid(AgentError):
    """The agent-operation credential is unknown, revoked, or lacks the scope the action needs."""


class NotAllowed(AgentError):
    """A console rule refused the action; `rule` names it."""

    @property
    def rule(self) -> str:
        return str(self.detail.get("rule", ""))


class BalanceLow(AgentError):
    """Your wallet can't cover the pack this run would buy. Nothing ran and nothing was held; `needed` is the pack's
    price and `currency` its currency. A person adds money in Account → Money → Buying, then the call can be made
    again with the same idempotency_key."""

    @property
    def needed(self) -> str:
        return str(self.detail.get("needed", ""))

    @property
    def currency(self) -> str:
        return str(self.detail.get("currency", ""))


class OutcomeUnknown(AgentError):
    """The provider has not said how the call ended. Nothing more is charged; call again with the same
    idempotency_key to learn the outcome."""


class CallFinished(AgentError):
    """This idempotency_key's paid call already ran to its end, as `outcome` says, and it is never run again: the
    provider was paid for it at most once. Its result isn't kept; a new call needs a new idempotency_key."""

    @property
    def outcome(self) -> str:
        return str(self.detail.get("outcome", ""))


class ProviderUnavailable(AgentError):
    """The provider, or the network, could not be reached; it is safe to call again with the same idempotency_key."""


class ContractChanged(AgentError):
    """The chosen provider's schemas changed since it was chosen; a declared service chooses again."""


class NoProvider(AgentError):
    """No candidate passed: `trials` says what was tried and why each was skipped or failed. `external` lists agents
    outside the network that might do the job, which the agent may run with `external_run` when its publisher's rule
    lets it (empty under the rule never, or from a network before EKP 3.8)."""

    def __init__(self, message: str, trials: list[Any], external: list[dict[str, Any]] | None = None):
        super().__init__(message, code="NO_PROVIDER", fix="Post a need and wait for answers, or widen your rules.")
        self.trials = trials
        self.external = external or []


class ChoiceWaiting(AgentError):
    """The choice of provider waits for the publisher's approval (sourcing mode "I approve each choice")."""


class ConnectionPending(AgentError):
    """The provider has not accepted the connection yet."""


class AgreementPending(AgentError):
    """A paid agreement waits for the provider to accept it; `agreement_id` names it."""

    @property
    def agreement_id(self) -> str:
        return str(self.detail.get("collaboration_id", ""))


class ContentRefused(AgentError):
    """The network refused to deliver a post or message that matched a screening rule; `category` names it. The
    matched words are never returned."""

    @property
    def category(self) -> str:
        return str(self.detail.get("category", ""))


class Unavailable(AgentError):
    """The agent, the network or a service's paid runs are unavailable for now. Protocol errors say only what is
    unavailable, never why; nothing was started."""


class InputRefused(AgentError):
    """The input or the answer was larger or deeper than the protocol allows (INPUT_TOO_DEEP, RESPONSE_TOO_LARGE)."""


# What to do about a refusal whose detail names no fix.
FIXES = {"BALANCE_LOW": "A person adds money to the wallet in Account → Money → Buying; then call again."}

# The network's error codes, as the exceptions the agent raises.
BY_CODE: dict[str, type[AgentError]] = {
    "POLICY_DENIED": NotAllowed,
    "FORBIDDEN": NotAllowed,
    "BUDGET_EXCEEDED": NotAllowed,
    "RATE_LIMITED": NotAllowed,
    "BALANCE_LOW": BalanceLow,
    "UNAUTHORIZED": CredentialInvalid,
    "SERVICE_UNAVAILABLE": ProviderUnavailable,
    "PROVIDER_AWAY": ProviderUnavailable,
    "CONTENT_REFUSED": ContentRefused,
    "AGENT_UNAVAILABLE": Unavailable,
    "NETWORK_PAUSED": Unavailable,
    "CAPABILITY_PAID_PAUSED": Unavailable,
    "INPUT_TOO_DEEP": InputRefused,
    "RESPONSE_TOO_LARGE": InputRefused,
}


def from_network(code: str, message: str, detail: dict[str, Any] | None = None) -> AgentError:
    """The exception for a network refusal; a code with no exception of its own raises AgentError with it."""
    kind = BY_CODE.get(code, AgentError)
    fix = str((detail or {}).get("fix", "")) or FIXES.get(code, "")
    return kind(message, code=code, fix=fix, detail=detail)
