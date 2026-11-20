# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""A real A2A agent, built with the A2A SDK and served in this process, for the connector's A2A upstream tests.

Its skills: `convert` answers with a data artifact, `clarify` asks for input, `break` fails, `reply` answers with a
message rather than a task, and `greet`, a text skill as most agents' are, reads the message's text and answers in
text parts.
"""

from typing import Any

from a2a.helpers.proto_helpers import new_data_message, new_task
from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.events import EventQueue
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.routes import create_agent_card_routes, create_jsonrpc_routes
from a2a.server.tasks import InMemoryTaskStore, TaskUpdater
from a2a.types import AgentCapabilities, AgentCard, AgentInterface, AgentSkill, Part, TaskState
from google.protobuf import json_format, struct_pb2
from starlette.applications import Starlette

RATES = {("EUR", "USD"): 1.1}


def _data(value: dict[str, Any]) -> Part:
    found = struct_pb2.Struct()
    found.update(value)
    return Part(data=struct_pb2.Value(struct_value=found))


def _arguments(context: RequestContext) -> dict[str, Any]:
    for part in context.message.parts if context.message else []:
        if part.HasField("data"):
            value = json_format.MessageToDict(part.data)
            if isinstance(value, dict):
                return value
    return {}


class Converter(AgentExecutor):
    async def execute(self, context: RequestContext, event_queue: EventQueue) -> None:
        meta = json_format.MessageToDict(context.message.metadata) if context.message else {}
        skill = str(meta.get("skill") or "")
        if skill == "reply":
            await event_queue.enqueue_event(new_data_message({"pong": True}))
            return
        updater = TaskUpdater(event_queue, str(context.task_id), str(context.context_id))
        await event_queue.enqueue_event(
            new_task(str(context.task_id), str(context.context_id), TaskState.TASK_STATE_SUBMITTED)
        )
        await updater.start_work()
        args = _arguments(context)
        if skill == "greet":
            said = " ".join(p.text for p in context.message.parts if p.HasField("text")) if context.message else ""
            await updater.add_artifact([Part(text="Hello!"), Part(text=f"You said: {said or 'nothing'}")])
            await updater.complete()
            return
        if skill == "clarify":
            await updater.requires_input(updater.new_agent_message([Part(text="Which currency do you mean?")]))
            return
        if skill == "break":
            await updater.failed(updater.new_agent_message([Part(text="the rate service is down")]))
            return
        rate = RATES.get((str(args.get("from")), str(args.get("to"))))
        amount = float(args.get("amount") or 0)
        await updater.add_artifact([_data({"amount": round(amount * (rate or 0), 2), "rate": rate or 0})])
        await updater.complete()

    async def cancel(self, context: RequestContext, event_queue: EventQueue) -> None:
        del context, event_queue


def card(url: str) -> AgentCard:
    skills = [
        AgentSkill(id="convert", name="Convert", description="Converts an amount between currencies.", tags=["fx"]),
        AgentSkill(id="clarify", name="Clarify", description="Asks which currency is meant.", tags=["fx"]),
        AgentSkill(id="break", name="Break", description="Always fails.", tags=["fx"]),
        AgentSkill(id="reply", name="Reply", description="Answers at once.", tags=["fx"]),
        AgentSkill(
            id="greet",
            name="Greet",
            description="Greets whoever writes.",
            tags=["chat"],
            examples=["hi"],
            input_modes=["text/plain"],
            output_modes=["text/plain"],
        ),
    ]
    return AgentCard(
        name="FX",
        description="Currency conversion",
        version="1.0.0",
        supported_interfaces=[AgentInterface(url=url, protocol_binding="JSONRPC", protocol_version="1.0")],
        capabilities=AgentCapabilities(streaming=False),
        default_input_modes=["application/json"],
        default_output_modes=["application/json"],
        skills=skills,
    )


def a2a_app(base: str = "http://fx.test") -> Starlette:
    agent_card = card(f"{base}/a2a")
    handler = DefaultRequestHandler(Converter(), InMemoryTaskStore(), agent_card)
    routes = [*create_agent_card_routes(agent_card), *create_jsonrpc_routes(handler, "/a2a")]
    return Starlette(routes=routes)
