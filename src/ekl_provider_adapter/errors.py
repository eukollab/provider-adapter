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


class PaymentNeeded(AgentError):
    """The provider asked for payment before approving the run, and the run's deadline passed unpaid. Nothing ran and
    nothing counts against your limits; `payment_link` is the provider's own page, which your publisher was also told
    about."""

    @property
    def payment_link(self) -> str:
        return str((self.detail.get("payment") or {}).get("url") or "")


class ProviderNotFunded(AgentError):
    """The provider can't take paid runs right now (its commission wallet is empty). The run didn't start; try later
    or choose another provider."""


class RunNotApproved(AgentError):
    """The provider declined the run, or did not answer in time. Nothing ran and nothing counts against your limits."""


class OutcomeUnknown(AgentError):
    """The provider has not said how the call ended. Nothing more is charged; call again with the same
    idempotency_key to learn the outcome."""


class ProviderUnavailable(AgentError):
    """The provider, or the network, could not be reached; it is safe to call again with the same idempotency_key."""


class ContractChanged(AgentError):
    """The chosen provider's schemas changed since it was chosen; a declared service chooses again."""


class NoProvider(AgentError):
    """No candidate passed: `trials` says what was tried and why each was skipped or failed."""

    def __init__(self, message: str, trials: list[Any]):
        super().__init__(message, code="NO_PROVIDER", fix="Post a need and wait for answers, or widen your rules.")
        self.trials = trials


class StandInMissing(AgentError):
    """No agent-operation credential, and no recorded answer for the declared service."""

    def __init__(self, ref: str | None):
        if ref is None:
            super().__init__(
                "Without an agent-operation credential, a service answers from its stand-in, and none is preferred.",
                code="STAND_IN_MISSING",
                fix='Name the service: agent.service("…", prefer="@handle/service"), then record it with ekl try.',
            )
            return
        handle, _, name = ref.partition("/")
        super().__init__(
            f"No recorded answer for {ref} in eukollab/fixtures.",
            code="STAND_IN_MISSING",
            fix=f"Record one: ekl try {handle} {name} --record (or set EKL_AGENT_CREDENTIAL to call it for real).",
        )


class ChoiceWaiting(AgentError):
    """The choice of provider waits for the publisher's approval (sourcing mode "I approve each choice")."""


class ConnectionPending(AgentError):
    """The provider has not accepted the connection yet."""


class AgreementPending(AgentError):
    """A paid agreement waits for the provider to accept it; `agreement_id` names it."""

    @property
    def agreement_id(self) -> str:
        return str(self.detail.get("collaboration_id", ""))


# The network's error codes, as the exceptions the agent raises.
BY_CODE: dict[str, type[AgentError]] = {
    "POLICY_DENIED": NotAllowed,
    "FORBIDDEN": NotAllowed,
    "BUDGET_EXCEEDED": NotAllowed,
    "RATE_LIMITED": NotAllowed,
    "PAYMENT_NEEDED": PaymentNeeded,
    "PROVIDER_NOT_FUNDED": ProviderNotFunded,
    "PROVIDER_NO_ANSWER": RunNotApproved,
    "UNAUTHORIZED": CredentialInvalid,
    "SERVICE_UNAVAILABLE": ProviderUnavailable,
}


def from_network(code: str, message: str, detail: dict[str, Any] | None = None) -> AgentError:
    """The exception for a network refusal; a code with no exception of its own raises AgentError with it."""
    kind = BY_CODE.get(code, AgentError)
    fix = str((detail or {}).get("fix", ""))
    return kind(message, code=code, fix=fix, detail=detail)
