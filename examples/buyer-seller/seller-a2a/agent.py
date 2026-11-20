# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""The same seller as an A2A agent: one skill, "Validate EU VAT numbers", built with the A2A SDK. Nothing in it is
about EuKollab: the connector reaches the skill over A2A and offers it on the network like any service, so the buyer
calls it exactly as it calls the MCP seller.

Run: pip install "a2a-sdk[http-server]"; python agent.py (its card is at http://localhost:8001/.well-known/agent-card.json)
"""

import os
from typing import Any

from a2a.helpers.proto_helpers import new_task
from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.events import EventQueue
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.routes import create_agent_card_routes, create_jsonrpc_routes
from a2a.server.tasks import InMemoryTaskStore, TaskUpdater
from a2a.types import AgentCapabilities, AgentCard, AgentInterface, AgentSkill, Part, TaskState
from google.protobuf import json_format, struct_pb2
from starlette.applications import Starlette


def validate_vat(vat_number: str) -> dict[str, Any]:
    cleaned = vat_number.replace(" ", "").upper()
    return {"vat_number": cleaned, "country": cleaned[:2], "valid": len(cleaned) > 4 and cleaned[:2].isalpha()}


class VatCheck(AgentExecutor):
    """Answers each message's data part ({"vat_number": "…"}) with one data artifact."""

    async def execute(self, context: RequestContext, event_queue: EventQueue) -> None:
        task_id, context_id = str(context.task_id), str(context.context_id)
        await event_queue.enqueue_event(new_task(task_id, context_id, TaskState.TASK_STATE_SUBMITTED))
        updater = TaskUpdater(event_queue, task_id, context_id)
        arguments: dict[str, Any] = {}
        for part in context.message.parts if context.message else []:
            if part.HasField("data"):
                arguments = dict(json_format.MessageToDict(part.data))
        if not isinstance(arguments.get("vat_number"), str):
            await updater.requires_input(updater.new_agent_message([Part(text="Which VAT number?")]))
            return
        result = struct_pb2.Struct()
        result.update(validate_vat(arguments["vat_number"]))
        await updater.add_artifact([Part(data=struct_pb2.Value(struct_value=result))])
        await updater.complete()

    async def cancel(self, context: RequestContext, event_queue: EventQueue) -> None:
        del context, event_queue


def app(base: str) -> Starlette:
    card = AgentCard(
        name="VAT Check",
        description="Checks EU VAT numbers.",
        version="1.0.0",
        supported_interfaces=[AgentInterface(url=f"{base}/a2a", protocol_binding="JSONRPC", protocol_version="1.0")],
        capabilities=AgentCapabilities(streaming=False),
        default_input_modes=["application/json"],
        default_output_modes=["application/json"],
        skills=[
            AgentSkill(
                id="validate_vat",
                name="Validate EU VAT numbers",
                description="The format of an EU VAT number, and its country.",
                tags=["tax"],
            )
        ],
    )
    handler = DefaultRequestHandler(VatCheck(), InMemoryTaskStore(), card)
    return Starlette(routes=[*create_agent_card_routes(card), *create_jsonrpc_routes(handler, "/a2a")])


if __name__ == "__main__":
    import uvicorn

    host, port = os.environ.get("HOST", "127.0.0.1"), int(os.environ.get("PORT", "8001"))
    uvicorn.run(app(f"http://localhost:{port}"), host=host, port=port)
