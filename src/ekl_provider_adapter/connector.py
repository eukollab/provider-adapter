# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""The connector: the adapter as its own process next to an existing MCP server.

    EKL_PROVIDER_CREDENTIAL=… EKL_NETWORK_URL=… EKL_UPSTREAM_MCP_URL=… ekl-provider-connector --port 8000

It calls the existing server's tools over MCP and needs nothing from that server's process, environment or files:
give it its agent.json (--config, else the file EKL_AGENT_FILE names, else the text EKL_AGENT_JSON holds, else
./agent.json), the provider credential, the server's address (upstream.mcp_url, which may carry the server's own
token), the headers the server wants on each request (EKL_UPSTREAM_HEADERS, a JSON object such as
{"Authorization": "Bearer …"}, for a server that takes its key in a header), network access to the server and to
EuKollab, and, in durable mode, its journal store. It announces agent.json and serves the version its publisher
confirmed. With EKL_AGENT_JSON, one published image runs any agent from environment variables alone. GET /healthz
answers 200 for load balancers and container services.

With EKL_AGENT_CREDENTIAL the connector also lets the agent take part: events go to EKL_CALLBACK_URL in
the publisher's app, or, without one, to the publisher's eukollab_callbacks.py, which the connector runs itself
(start_callbacks), and the agent's actions are served on 127.0.0.1 (EKL_AGENT_API_PORT, default 8765) only.
`POST /agent/service` there calls a declared service for an app in any language, from its stand-ins until the
credential is set; `--use-only` runs only that loopback API and the callbacks, for an agent that offers nothing.
With EKL_STRIPE_WEBHOOK_SECRET, runs paid on the publisher's own Stripe account are approved from its webhook at
`<path>/stripe` (stripe_webhook); with EKL_STRIPE_KEY instead, from Stripe's Events API, which needs no public address
(stripe_events).

`--relay` (what `ekl agent run` starts) listens on nothing public: the network relays the agent's calls down a stream
this process keeps open (relay), so the machine needs no public address.
"""

import argparse
import asyncio
import base64
import contextlib
import gzip
import json
import logging
import os
import re
import socket
import subprocess
import sys
import tempfile
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import httpx2
import mcp_types as types
import uvicorn
from mcp import Client
from mcp.client.streamable_http import streamable_http_client
from mcp.shared.exceptions import MCPError
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import PlainTextResponse, Response
from starlette.routing import Route

from ekl_provider_adapter.agent_journal import AgentJournal
from ekl_provider_adapter.api import HttpProviderApi, ProviderApi
from ekl_provider_adapter.attach import (
    AdapterApp,
    AgentFileSource,
    agent_for,
    local_file,
    runs_for,
    settings_from_environment,
    storage_for,
)
from ekl_provider_adapter.callback_signing import callback_key
from ekl_provider_adapter.callbacks import CallbackChannel, bridge, bridge_runs, callback_url, local_api
from ekl_provider_adapter.config import DEFAULT_NETWORK_URL, inline_agent_file, require_https
from ekl_provider_adapter.core import AdapterCore, Hooks, Settings
from ekl_provider_adapter.local_guard import announce_when_listening, local_api_file
from ekl_provider_adapter.relay import Relay, relay_address
from ekl_provider_adapter.server import adapter_routes
from ekl_provider_adapter.storage import Storage
from ekl_provider_adapter.stripe_events import StripeEvents
from ekl_provider_adapter.stripe_webhook import stripe_routes

log = logging.getLogger(__name__)
CALLBACKS_FILE = "eukollab_callbacks.py"


UPSTREAM_HEADERS = "EKL_UPSTREAM_HEADERS"
_HEADER_NAME = re.compile(r"^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")
# Headers the MCP transport sets itself: a configured value would break the session rather than authenticate it.
_TRANSPORT_HEADERS = frozenset(
    {
        "accept",
        "connection",
        "content-length",
        "content-type",
        "host",
        "last-event-id",
        "mcp-method",
        "mcp-protocol-version",
        "mcp-session-id",
        "transfer-encoding",
    }
)


def upstream_headers(text: str | None) -> dict[str, str]:
    """The headers sent to the upstream server with every request, from EKL_UPSTREAM_HEADERS. Errors name the
    header, never its value, which is usually the server's key."""
    if not text or not text.strip():
        return {}
    try:
        found = json.loads(text)
    except ValueError as exc:
        raise ValueError(f'{UPSTREAM_HEADERS} must be a JSON object, e.g. {{"Authorization": "Bearer …"}}') from exc
    if not isinstance(found, dict):
        raise ValueError(f'{UPSTREAM_HEADERS} must be a JSON object, e.g. {{"Authorization": "Bearer …"}}')
    headers: dict[str, str] = {}
    for name, value in found.items():
        if not _HEADER_NAME.match(name):
            raise ValueError(f"{UPSTREAM_HEADERS}: {name!r} is not a valid header name")
        if name.lower() in _TRANSPORT_HEADERS:
            raise ValueError(f"{UPSTREAM_HEADERS}: {name} is set by the MCP transport and cannot be configured")
        if not isinstance(value, str) or any(c in value for c in "\r\n\0"):
            raise ValueError(f"{UPSTREAM_HEADERS}: the value of {name} must be text on one line")
        headers[name] = value
    return headers


class Upstream:
    """The existing MCP server, reached as an MCP client. Each call opens its own short session, so one failing
    call cannot wedge the others. `headers` go with every request, and redirects are never followed, so they only
    ever reach the configured address."""

    def __init__(self, url: str, http: httpx2.AsyncClient | None = None, headers: dict[str, str] | None = None) -> None:
        self.url = url
        self._http = http
        self.headers = dict(headers or {})
        if http is not None:
            http.headers.update(self.headers)

    @contextlib.asynccontextmanager
    async def _client(self) -> AsyncIterator[Client]:
        http = self._http or httpx2.AsyncClient(timeout=60, headers=self.headers, follow_redirects=False)
        try:
            async with Client(streamable_http_client(self.url, http_client=http)) as client:
                yield client
        finally:
            if self._http is None:
                await http.aclose()

    async def call(self, tool: str, arguments: dict[str, Any]) -> types.CallToolResult:
        try:
            async with self._client() as client:
                return await client.call_tool(tool, arguments)
        except MCPError as exc:
            # A protocol-level refusal (unknown tool, invalid params) means the tool never ran.
            return types.CallToolResult(
                content=[types.TextContent(type="text", text=str(exc))], is_error=True, meta={"ekl": {"effect": "none"}}
            )

    async def tools(self) -> list[types.Tool]:
        async with self._client() as client:
            return list((await client.list_tools()).tools)

    async def list(self) -> list[types.Resource]:
        async with self._client() as client:
            return list((await client.list_resources()).resources)

    async def read(self, uri: str) -> types.ReadResourceResult:
        async with self._client() as client:
            return await client.read_resource(uri)


def _shared(storage_url: str | None) -> bool:
    return bool(storage_url and storage_url.startswith(("postgresql://", "postgres://")))


def connector_app(
    agent_file: AgentFileSource | None = None,
    *,
    storage: Storage | None = None,
    hooks: Hooks | None = None,
    credential: str | None = None,
    network_url: str | None = None,
    api: ProviderApi | None = None,
    upstream: Upstream | None = None,
    upstream_url: str | None = None,
    settings: Settings | None = None,
    callback_url: str | None = None,
    runs_api: Any | None = None,
) -> AdapterApp:
    """The connector's public app: the adapter endpoint in front of the upstream server, plus /healthz. With
    EKL_AGENT_CREDENTIAL and a callback URL it also runs the agent's own side, posting events to the publisher's
    app. Arguments override the environment; tests pass their own api, upstream and storage."""
    local = local_file(agent_file)
    url = (
        upstream_url
        or os.environ.get("EKL_UPSTREAM_MCP_URL")
        or (local.agent.upstream.mcp_url if local and local.agent.upstream else None)
    )
    if upstream is None and not url:
        raise ValueError("set upstream.mcp_url in agent.json, or EKL_UPSTREAM_MCP_URL, to your MCP server's address")
    found_credential, found_network = settings_from_environment(local, credential, network_url)
    target = upstream or Upstream(str(url), headers=upstream_headers(os.environ.get(UPSTREAM_HEADERS)))
    core = AdapterCore(
        local,
        api or HttpProviderApi(found_network, found_credential),
        target.call,
        credential=found_credential,
        endpoint=os.environ.get("EKL_ADAPTER_ENDPOINT"),
        storage=storage_for(local, storage),
        hooks=hooks,
        settings=settings,
    )
    routes, adapter_lifespan = adapter_routes(core, target.tools, resources=target)

    async def healthz(_: Request) -> Response:
        # Liveness only, open to anyone: it says nothing about the agent, the upstream or the network.
        return PlainTextResponse("ok")

    routes = [Route("/healthz", healthz, methods=["GET"]), *routes]
    storage_url = (local.agent.adapter.storage if local else None) or os.environ.get("EKL_ADAPTER_STORAGE")
    own = agent_for(found_network, None, None, storage_url)
    callback = callback_url or os.environ.get("EKL_CALLBACK_URL")
    runs = runs_for(core, local, runs_api, found_network, found_credential)
    channel: CallbackChannel | None = None
    if callback:
        # One channel for the agent's events and the paid runs: one journal of decisions, one v2 negotiation.
        channel = CallbackChannel(
            callback,
            os.environ.get("EKL_AGENT_CREDENTIAL") or found_credential,
            own.journal if own.enabled else AgentJournal(storage_url if _shared(storage_url) else None),
            agent_id=lambda: os.environ.get("EKL_AGENT_ID") or own.agent_id,
        )
        if own.enabled:
            bridge(own, callback, "", channel=channel)
        # Paid runs are approved by the publisher's app, in any language, before the run's approval expiry; an empty
        # answer sends the consumer to agent.json's payment page, and no answer leaves the run waiting.
        bridge_runs(runs, callback, "", channel=channel)

    @contextlib.asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        with contextlib.suppress(Exception):  # the upstream may start later; schemas are read again per listing
            core.tool_schemas.update({t.name: dict(t.output_schema) for t in await target.tools() if t.output_schema})
        async with adapter_lifespan(app):
            await own.start()
            if channel is not None:
                await channel.start()
            watching = asyncio.ensure_future(runs.watch()) if found_credential or runs_api else None
            stripe_key = os.environ.get("EKL_STRIPE_KEY")
            paying = asyncio.ensure_future(StripeEvents(runs, stripe_key).watch()) if stripe_key else None
            try:
                yield
            finally:
                for task in (watching, paying):
                    if task is not None:
                        task.cancel()
                if channel is not None:
                    await channel.stop()
                await own.stop()

    stripe = stripe_routes(core.path, runs, os.environ.get("EKL_STRIPE_WEBHOOK_SECRET"))
    app = AdapterApp(routes=[*routes, *stripe], lifespan=lifespan)
    app.state.ekl_core = core
    app.agent = own
    app.runs = runs
    return app


def use_only_app(callback_url: str | None = None) -> Starlette:
    """The connector for an agent that offers nothing: no upstream and no public endpoint, only the
    agent's events posted to the app and its actions on the loopback API. Without EKL_AGENT_CREDENTIAL the declared
    services answer from their stand-ins."""
    network = require_https(os.environ.get("EKL_NETWORK_URL") or DEFAULT_NETWORK_URL, "EKL_NETWORK_URL")
    own = agent_for(network, None, None, os.environ.get("EKL_ADAPTER_STORAGE"))
    if own.enabled:
        own.announce_itself(network)
    callback = callback_url or os.environ.get("EKL_CALLBACK_URL")
    channel = bridge(own, callback, os.environ.get("EKL_AGENT_CREDENTIAL", "")) if own.enabled and callback else None
    app = local_api(own)
    app.state.ekl_agent = own

    @contextlib.asynccontextmanager
    async def lifespan(_: Starlette) -> AsyncIterator[None]:
        await own.start()
        if channel is not None:
            await channel.start()
        try:
            yield
        finally:
            if channel is not None:
                await channel.stop()
            await own.stop()

    app.router.lifespan_context = lifespan
    return app


def _free_or(port: str) -> str:
    """The port, unless something on this machine already listens on it (another agent's callbacks): then a free one."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        if probe.connect_ex(("127.0.0.1", int(port))) != 0:
            return port
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return str(probe.getsockname()[1])


def start_callbacks(agent_file: str | None) -> subprocess.Popen[bytes] | None:
    """The publisher's eukollab_callbacks.py, run beside the connector on 127.0.0.1, so a deployed connector needs
    no second service for its callbacks. It comes from EKL_CALLBACKS_PY (gzipped and base64-encoded, as
    `ekl agent deploy` sends it) or is the file beside agent.json. It runs when EKL_CALLBACK_URL names no other
    place: with the agent credential it answers the agent's events, and with the provider credential alone it still
    answers paid runs waiting for approval. Its address then becomes EKL_CALLBACK_URL."""
    credential = os.environ.get("EKL_AGENT_CREDENTIAL") or os.environ.get("EKL_PROVIDER_CREDENTIAL")
    if os.environ.get("EKL_CALLBACK_URL") or not credential:
        return None
    sent = os.environ.get("EKL_CALLBACKS_PY")
    child_db = os.environ.get("EKL_CALLBACKS_DB")
    if sent:
        path = Path(tempfile.mkdtemp(prefix="ekl-callbacks-")) / CALLBACKS_FILE
        path.write_bytes(gzip.decompress(base64.b64decode(sent)))
        # The callbacks read agent.json beside them (the services it offers, to accept connections and answer needs
        # for them), so it goes beside them too; and their record of handled events stays where the connector runs,
        # not in this folder, which a restart replaces, so an event is never handled twice across restarts.
        if text := _agent_text(agent_file):
            (path.parent / "agent.json").write_text(text, encoding="utf-8")
        child_db = child_db or str(local_api_file(agent_file).parent / "callbacks.db")
    else:
        beside = agent_file or os.environ.get("EKL_AGENT_FILE") or "agent.json"
        path = Path(beside).resolve().parent / CALLBACKS_FILE
        if not path.is_file():
            return None
    port = os.environ.get("EKL_CALLBACKS_PORT") or _free_or("8002")
    os.environ["EKL_CALLBACK_URL"] = f"http://127.0.0.1:{port}/"
    log.info("running the agent's callbacks (%s) on 127.0.0.1:%s", path.name, port)
    # The callbacks get the key that checks and signs events, never the credentials: code that only answers events
    # can't act as the agent.
    child = {k: v for k, v in os.environ.items() if k not in ("EKL_AGENT_CREDENTIAL", "EKL_PROVIDER_CREDENTIAL")}
    child["EKL_CALLBACK_KEY"] = os.environ.get("EKL_CALLBACK_KEY") or callback_key(credential).hex()
    if child_db:
        Path(child_db).parent.mkdir(parents=True, exist_ok=True)
        child["EKL_CALLBACKS_DB"] = child_db
    # Loopback only, its default: the events it receives are signed, but nothing outside needs to reach it. Its own
    # process group: Ctrl-C in the terminal reaches the connector, which stops it quietly on the way out, rather than
    # the child, which would print a KeyboardInterrupt traceback.
    argv = [sys.executable, str(path), "--port", port]
    return subprocess.Popen(argv, cwd=path.parent, env=child, process_group=0)


def _agent_text(agent_file: str | None) -> str | None:
    """agent.json's text, from the file the connector reads or from EKL_AGENT_JSON, or None when there is neither."""
    named = agent_file or os.environ.get("EKL_AGENT_FILE")
    if named and Path(named).is_file():
        return Path(named).read_text(encoding="utf-8")
    inline = os.environ.get("EKL_AGENT_JSON")
    if inline:
        try:
            return json.dumps(inline_agent_file(inline))
        except ValueError:
            return None
    return None


def hosted_endpoint() -> str | None:
    """The connector's public address, with the default path /ekl, on a host that names it in the environment
    (Railway, Fly.io, Render): a deployment that doesn't set EKL_ADAPTER_ENDPOINT still announces where it runs, in its
    first deploy. The network's endpoint-control check proves the address either way."""
    if domain := os.environ.get("RAILWAY_PUBLIC_DOMAIN"):
        return f"https://{domain}/ekl"
    if app := os.environ.get("FLY_APP_NAME"):
        return f"https://{app}.fly.dev/ekl"
    if url := os.environ.get("RENDER_EXTERNAL_URL"):
        return f"{url.rstrip('/')}/ekl"
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="EuKollab provider connector")
    parser.add_argument(
        "--config",
        default=None,
        help="agent.json (default: EKL_AGENT_FILE, else EKL_AGENT_JSON, else ./agent.json)",
    )
    parser.add_argument("--upstream", default=None, help="the existing MCP server (default: EKL_UPSTREAM_MCP_URL)")
    # A server meant to be reached from outside: the adapter's endpoint admits nothing the network didn't.
    parser.add_argument("--host", default=os.environ.get("HOST", "0.0.0.0"))  # nosec B104 - a public server
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "8000")))
    parser.add_argument(
        "--use-only",
        action="store_true",
        help="an agent that offers nothing: serve only its local API and callbacks, on 127.0.0.1",
    )
    parser.add_argument(
        "--relay",
        action="store_true",
        help="no public address: the network relays the agent's calls to this process (ekl agent run)",
    )
    options = parser.parse_args()
    # What the network says about the announced agent.json is logged here, where the publisher deploying it looks.
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        if os.environ.get("EKL_CALLBACK_URL"):
            callback_url(os.environ["EKL_CALLBACK_URL"])
    except ValueError as problem:
        sys.exit(f"ekl-provider-connector can't start: {problem}")
    if not (options.relay or options.use_only or os.environ.get("EKL_ADAPTER_ENDPOINT")) and (
        found := hosted_endpoint()
    ):
        # Set before agent.json is read, so its ${EKL_ADAPTER_ENDPOINT} expands to it.
        os.environ["EKL_ADAPTER_ENDPOINT"] = found
        log.info("its address, from the host's environment: %s", found)
    # 8765 unless something else listens there (another agent's connector): then a free port, which the app finds in
    # .ekl/local-api.json.
    private_port = int(_free_or(os.environ.get("EKL_AGENT_API_PORT", "8765")))
    os.environ["EKL_AGENT_API_PORT"] = str(private_port)  # the callbacks this starts call it there
    callbacks = start_callbacks(options.config)
    try:
        serve(options, private_port)
    except ValueError as problem:
        # A setting missing or malformed at start (agent.json naming an unset variable, say): one line saying which,
        # where a host's log shows it, rather than a traceback.
        sys.exit(f"ekl-provider-connector can't start: {problem}")
    finally:
        if callbacks is not None:
            callbacks.terminate()


def quiet_for_a_terminal() -> None:
    """`ekl agent run` shows the connector in the publisher's terminal: the lines that matter are the announcement and
    one per relayed call, so the libraries' startup and request lines stay out of it."""
    for name in ("httpx2", "mcp", "uvicorn.error"):
        logging.getLogger(name).setLevel(logging.WARNING)


def serve(options: argparse.Namespace, private_port: int) -> None:
    if options.use_only:
        quiet_for_a_terminal()
        log.info("serving the agent's actions and events on 127.0.0.1:%d (it offers nothing to call)", private_port)
        server = uvicorn.Server(
            uvicorn.Config(use_only_app(), host="127.0.0.1", port=private_port, log_level="warning")
        )

        async def only() -> None:
            await asyncio.gather(
                server.serve(), announce_when_listening(server, f"http://127.0.0.1:{private_port}", options.config)
            )

        asyncio.run(only())
        return
    if options.relay:
        asyncio.run(serve_relayed(options, private_port))
        return
    app = connector_app(options.config, upstream_url=options.upstream)
    # The agent's actions are served beside the public endpoint, on the loopback interface only.
    # Without the credential they still run, so declared services answer from their stand-ins.
    public = uvicorn.Server(uvicorn.Config(app, host=options.host, port=options.port, log_level="info"))
    private = uvicorn.Server(
        uvicorn.Config(local_api(app.agent, app.runs), host="127.0.0.1", port=private_port, log_level="info")
    )

    async def both() -> None:
        await asyncio.gather(
            public.serve(),
            private.serve(),
            announce_when_listening(private, f"http://127.0.0.1:{private_port}", options.config),
        )

    asyncio.run(both())


async def serve_relayed(options: argparse.Namespace, private_port: int) -> None:
    """The connector in relay mode: the agent's relay address is its endpoint, and the app serves the calls the
    network relays instead of listening for them. The loopback API runs as usual; stopping it stops the relay."""
    quiet_for_a_terminal()
    credential, network = settings_from_environment(local_file(options.config), None, None)
    endpoint = await relay_address(network, credential)
    os.environ["EKL_ADAPTER_ENDPOINT"] = endpoint
    app = connector_app(options.config, upstream_url=options.upstream)
    private = uvicorn.Server(
        uvicorn.Config(local_api(app.agent, app.runs), host="127.0.0.1", port=private_port, log_level="warning")
    )
    log.info("serving the agent's calls through the network's relay at %s", endpoint)
    async with app.router.lifespan_context(app):
        relaying = asyncio.ensure_future(Relay(app, network, credential, path=app.state.ekl_core.path).run())
        try:
            await asyncio.gather(
                private.serve(), announce_when_listening(private, f"http://127.0.0.1:{private_port}", options.config)
            )
        finally:
            relaying.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await relaying
