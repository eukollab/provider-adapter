# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""An HTTPS API described by OpenAPI 3.0 or 3.1, reached as if it were an MCP server (EKP 3.4 §5.3).

Each operation becomes one tool: its operationId is the tool's name, its parameters and JSON request body one input
schema, its JSON success response the output schema, and its method the tool's annotations. The rest of the adapter
(admission, outcomes, fingerprints, the endpoint) sees tools and CallToolResults as from any MCP server, so an API
joins the network without a line of MCP code. `ekl` derives agent.json by the same mapping; the shared vectors in
contracts/vectors/openapi-tools.json keep the two in step.
"""

import copy
import json
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote, urljoin, urlsplit

import httpx2
import mcp_types as types
import yaml

from ekl_provider_adapter import trial_key

METHODS = ("get", "put", "post", "delete", "patch", "head")
_NAME = re.compile(r"[^A-Za-z0-9_.-]+")
MAX_NAME = 128
MAX_REF_DEPTH = 32
MAX_RESPONSE_BYTES = 1_000_000
SPEC_CACHE_SECONDS = 60.0
# How long the API has to answer the check that it is up.
ANSWER_SECONDS = 10.0
# Statuses that say the API refused the request before doing anything: the call failed without effect, so an external
# action is settled as failed rather than unknown.
REFUSED = frozenset({400, 401, 403, 404, 405, 406, 411, 413, 414, 415, 422, 429, 431})
# Statuses saying the API can't answer now, rather than that the request failed.
UNAVAILABLE = frozenset({502, 503, 504})
CLASS_HINTS: dict[str, dict[str, bool]] = {
    "read_only": {"readOnlyHint": True},
    "transformation": {"readOnlyHint": False, "destructiveHint": False, "openWorldHint": False},
    "external_action": {"readOnlyHint": False, "openWorldHint": True},
}
NO_BODY_OUTPUT: dict[str, Any] = {
    "type": "object",
    "properties": {"status": {"type": "integer"}},
    "required": ["status"],
}


class UnsupportedSpecError(ValueError):
    """The document is not OpenAPI 3.0 or 3.1."""


@dataclass(frozen=True)
class Parameter:
    name: str  # as the API knows it
    location: str  # path, query or header
    prop: str  # the input property that carries it


@dataclass(frozen=True)
class Operation:
    name: str
    method: str
    path: str
    parameters: tuple[Parameter, ...]
    body: bool
    wrapped: bool  # a body that is not an object, carried as {"result": …}
    tool: dict[str, Any]  # the MCP tool, as tools/list would show it


@dataclass(frozen=True)
class Skipped:
    method: str
    path: str
    reason: str


def tool_name(operation: dict[str, Any], method: str, path: str) -> str:
    """The operationId, or method and path, as a name MCP clients accept."""
    raw = str(operation.get("operationId") or f"{method}{path}")
    return _NAME.sub("_", raw).strip("_")[:MAX_NAME] or method


def resolve(spec: dict[str, Any], node: Any, depth: int = 0, seen: frozenset[str] = frozenset()) -> Any:
    """`node` with every local $ref inlined. A reference that loops back on itself becomes the empty schema, which
    accepts anything; one outside the document is refused, since the connector reads no other file."""
    if depth > MAX_REF_DEPTH:
        return {}
    if isinstance(node, list):
        return [resolve(spec, item, depth + 1, seen) for item in node]
    if not isinstance(node, dict):
        return node
    ref = node.get("$ref")
    if isinstance(ref, str):
        if not ref.startswith("#/"):
            raise UnsupportedSpecError(f"the reference {ref} points outside the document")
        if ref in seen:
            return {}
        target: Any = spec
        for raw in ref[2:].split("/"):
            part = raw.replace("~1", "/").replace("~0", "~")
            if not isinstance(target, dict) or part not in target:
                raise UnsupportedSpecError(f"the reference {ref} names nothing in the document")
            target = target[part]
        siblings = {k: v for k, v in node.items() if k != "$ref"}
        found = resolve(spec, target, depth + 1, seen | {ref})
        return {**found, **resolve(spec, siblings, depth + 1, seen)} if isinstance(found, dict) else found
    return {key: resolve(spec, value, depth + 1, seen) for key, value in node.items()}


INT64_DIGITS = "^-?[0-9]+$"
MAX_SAFE_INTEGER = 2**53 - 1


def json_schema(schema: Any) -> dict[str, Any]:
    """An OpenAPI schema as JSON Schema: 3.0's `nullable` becomes a null type, and the keywords only OpenAPI knows
    (discriminator, xml, externalDocs, example) are dropped."""
    if not isinstance(schema, dict):
        return {}
    out: dict[str, Any] = {}
    for key, value in schema.items():
        if key in ("nullable", "discriminator", "xml", "externalDocs", "example"):
            continue
        if key == "properties" and isinstance(value, dict):
            out[key] = {name: json_schema(inner) for name, inner in value.items()}
        elif key in ("items", "additionalProperties", "not") and isinstance(value, dict):
            out[key] = json_schema(value)
        elif key in ("allOf", "anyOf", "oneOf") and isinstance(value, list):
            out[key] = [json_schema(inner) for inner in value]
        else:
            out[key] = copy.deepcopy(value)
    if out.get("format") == "int64":
        # A 64-bit integer can be beyond what ekl-json-1 carries (±2^53), so it is also taken and given as its
        # decimal string, as protobuf's JSON mapping writes int64 (EKP 3.4 §5.3).
        kind = out.get("type")
        kinds = [kind] if isinstance(kind, str) else list(kind) if isinstance(kind, list) else []
        if "integer" in kinds and "string" not in kinds:
            out["type"] = [*kinds, "string"]
            out["pattern"] = INT64_DIGITS
    if schema.get("nullable") is True:
        kind = out.get("type")
        if isinstance(kind, str):
            out["type"] = [kind, "null"]
        elif isinstance(kind, list) and "null" not in kind:
            out["type"] = [*kind, "null"]
    return out


def _is_json(media: str) -> bool:
    base = media.split(";", maxsplit=1)[0].strip().lower()
    return base == "application/json" or base.endswith("+json")


def _json_content(content: Any) -> dict[str, Any] | None:
    if not isinstance(content, dict):
        return None
    for media, body in content.items():
        if _is_json(str(media)) and isinstance(body, dict):
            return dict(body.get("schema") or {})
    return None


def _success(responses: dict[str, Any]) -> tuple[dict[str, Any] | None, bool]:
    """The lowest 2xx response's JSON schema, and whether a 2xx response has a body at all."""
    codes = sorted(str(c) for c in responses if str(c).startswith("2"))
    for code in codes:
        response = responses[code] if isinstance(responses[code], dict) else {}
        schema = _json_content(response.get("content"))
        if schema is not None:
            return schema, True
    return None, False


def output_schema(schema: dict[str, Any] | None) -> tuple[dict[str, Any], bool]:
    """The tool's output schema, and whether the body is wrapped under `result`: MCP structured content is an object,
    so any other body (an array, a number) is carried as {"result": …}."""
    if schema is None:
        return dict(NO_BODY_OUTPUT), False
    converted = json_schema(schema)
    if converted.get("type") == "object" or ("properties" in converted and "type" not in converted):
        return converted, False
    return {"type": "object", "properties": {"result": converted}, "required": ["result"]}, True


def _annotations(method: str, operation: dict[str, Any]) -> dict[str, bool]:
    declared = operation.get("x-eukollab-class")
    if isinstance(declared, str) and declared in CLASS_HINTS:
        return dict(CLASS_HINTS[declared])
    if method in ("get", "head"):
        return {"readOnlyHint": True}
    hints = {"readOnlyHint": False}
    if method == "delete":
        hints["destructiveHint"] = True
    if method in ("put", "delete"):
        hints["idempotentHint"] = True
    return hints


def _operation(spec: dict[str, Any], path: str, method: str, shared: list[Any], operation: dict[str, Any]) -> Operation:
    params: dict[tuple[str, str], dict[str, Any]] = {}
    for raw in [*shared, *(operation.get("parameters") or [])]:
        param = resolve(spec, raw)
        if isinstance(param, dict) and param.get("name") and param.get("in"):
            params[(str(param["in"]), str(param["name"]))] = param
    properties: dict[str, Any] = {}
    required: list[str] = []
    carried: list[Parameter] = []
    names = [name for (_, name) in params]
    for (location, name), param in params.items():
        if location == "cookie":
            if param.get("required"):
                raise UnsupportedSpecError("it needs a cookie parameter")
            continue
        prop = name if names.count(name) == 1 and name != "body" else f"{location}_{name}"
        schema = json_schema(resolve(spec, param.get("schema") or {}))
        if param.get("description") and "description" not in schema:
            schema["description"] = str(param["description"])
        properties[prop] = schema
        if param.get("required") or location == "path":
            required.append(prop)
        carried.append(Parameter(name, location, prop))
    body = False
    request = resolve(spec, operation.get("requestBody") or {})
    if request:
        body_schema = _json_content(request.get("content"))
        if body_schema is None:
            raise UnsupportedSpecError("its request body is not JSON")
        properties["body"] = json_schema(body_schema)
        if request.get("required"):
            required.append("body")
        body = True
    responses = resolve(spec, operation.get("responses") or {})
    success, _ = _success(responses if isinstance(responses, dict) else {})
    output, wrapped = output_schema(success)
    input_schema: dict[str, Any] = {"type": "object", "properties": properties}
    if required:
        input_schema["required"] = required
    name = tool_name(operation, method, path)
    description = " ".join(
        str(part).strip() for part in (operation.get("summary"), operation.get("description")) if part
    ).strip()
    tool: dict[str, Any] = {
        "name": name,
        "description": description or f"{method.upper()} {path}",
        "inputSchema": input_schema,
        "outputSchema": output,
        "annotations": _annotations(method, operation),
    }
    return Operation(name, method, path, tuple(carried), body, wrapped, tool)


def operations(spec: dict[str, Any]) -> tuple[list[Operation], list[Skipped]]:
    """Every operation the connector can call, in document order, and the ones it can't, with why."""
    version = str(spec.get("openapi") or "")
    if not version.startswith(("3.0", "3.1")):
        named = f"Swagger {spec['swagger']}" if spec.get("swagger") else (version or "no version")
        raise UnsupportedSpecError(f"only OpenAPI 3.0 and 3.1 are supported, not {named}")
    found: list[Operation] = []
    skipped: list[Skipped] = []
    seen: set[str] = set()
    for path, item in (spec.get("paths") or {}).items():
        if not isinstance(item, dict):
            continue
        shared = list(item.get("parameters") or [])
        for method in METHODS:
            operation = item.get(method)
            if not isinstance(operation, dict):
                continue
            try:
                op = _operation(spec, str(path), method, shared, operation)
            except UnsupportedSpecError as problem:
                skipped.append(Skipped(method, str(path), str(problem)))
                continue
            if op.name in seen:
                skipped.append(Skipped(method, str(path), f"another operation is already named {op.name}"))
                continue
            seen.add(op.name)
            found.append(op)
    return found, skipped


def base_url(spec: dict[str, Any], spec_url: str | None) -> str | None:
    """The API's address from the document's first server, its variables at their defaults; a relative address is
    read against where the document came from. A document naming no server is served where it is (OpenAPI's
    default server is `/`), as FastAPI's are; None only for one read from a file."""
    servers = spec.get("servers") or []
    first = servers[0] if servers and isinstance(servers[0], dict) else {}
    url = str(first.get("url") or "/")
    for name, variable in (first.get("variables") or {}).items():
        url = url.replace("{" + str(name) + "}", str((variable or {}).get("default", "")))
    if not urlsplit(url).scheme:
        if not (spec_url and urlsplit(spec_url).scheme in ("http", "https")):
            return None
        url = urljoin(spec_url, url)
    return url


def parse_document(text: str) -> dict[str, Any]:
    """An OpenAPI document in JSON or YAML."""
    try:
        found = json.loads(text)
    except ValueError:
        found = yaml.safe_load(text)
    if not isinstance(found, dict):
        raise UnsupportedSpecError("the OpenAPI document is not an object")
    return found


def _text(value: Any) -> str:
    return value if isinstance(value, str) else json.dumps(value, separators=(",", ":"))


class OpenAPIUpstream:
    """The API, reached over HTTPS. `spec_url` is a URL or a file; `api_url` overrides the document's server.
    `headers` (EKL_UPSTREAM_HEADERS) carry the API's key and go with every request; redirects are never followed."""

    def __init__(
        self,
        spec_url: str,
        api_url: str | None = None,
        *,
        http: httpx2.AsyncClient | None = None,
        headers: dict[str, str] | None = None,
        spec: dict[str, Any] | None = None,
    ) -> None:
        self.spec_url = spec_url
        self.api_url = api_url
        self.headers = dict(headers or {})
        self._http = http
        self._spec = spec
        self._read_at = time.monotonic() if spec is not None else 0.0
        self._fixed = spec is not None

    def _client(self) -> httpx2.AsyncClient:
        if self._http is None:
            self._http = httpx2.AsyncClient(timeout=60, follow_redirects=False)
        return self._http

    async def document(self) -> dict[str, Any]:
        """The OpenAPI document, read again at most once a minute, so a changed API shows in the next listing."""
        if self._spec is not None and (self._fixed or time.monotonic() - self._read_at < SPEC_CACHE_SECONDS):
            return self._spec
        if urlsplit(self.spec_url).scheme in ("http", "https"):
            response = await self._client().get(self.spec_url, headers=self.headers)
            response.raise_for_status()
            text = response.text
        else:
            text = Path(self.spec_url).read_text(encoding="utf-8")
        self._spec = parse_document(text)
        self._read_at = time.monotonic()
        return self._spec

    async def _operations(self) -> dict[str, Operation]:
        found, _ = operations(await self.document())
        return {op.name: op for op in found}

    async def tools(self) -> list[types.Tool]:
        found = [types.Tool.model_validate(op.tool) for op in (await self._operations()).values()]
        await self._answering()
        return found

    async def _answering(self) -> None:
        """Raises unless the API itself answers, with any status: a document read from a file or a minute ago says
        nothing of whether the API is up, and an agent whose API is down is offline."""
        base = self.api_url or base_url(await self.document(), self.spec_url)
        if base:
            await self._client().request("HEAD", base, headers=self.headers, timeout=ANSWER_SECONDS)

    async def list(self) -> list[types.Resource]:
        return []

    async def read(self, uri: str) -> types.ReadResourceResult:
        raise LookupError(f"an OpenAPI upstream serves no resources ({uri})")

    async def call(self, tool: str, arguments: dict[str, Any]) -> types.CallToolResult:
        op = (await self._operations()).get(tool)
        if op is None:
            return _refused(f"the API has no operation {tool}")
        base = self.api_url or base_url(await self.document(), self.spec_url)
        if not base:
            return _refused("the OpenAPI document, read from a file, names no server: set upstream.api_url")
        path = op.path
        query: list[tuple[str, str | int | float | bool | None]] = []
        headers = dict(self.headers) | trial_key.headers()
        for param in op.parameters:
            if param.prop not in arguments:
                continue
            value = arguments[param.prop]
            if param.location == "path":
                path = path.replace("{" + param.name + "}", quote(_text(value), safe=""))
            elif param.location == "query":
                values = value if isinstance(value, list) else [value]
                query.extend((param.name, _text(v)) for v in values)
            elif param.location == "header":
                headers[param.name] = _text(value)
        if "{" in path:
            return _refused(f"a path parameter of {tool} is missing")
        url = base.rstrip("/") + path
        content: bytes | None = None
        if op.body and "body" in arguments:
            body_schema = (op.tool["inputSchema"].get("properties") or {}).get("body") or {}
            content = json.dumps(int64_numbers(arguments["body"], body_schema)).encode()
            headers["content-type"] = "application/json"
        response = await self._client().request(
            op.method.upper(), url, params=query or None, headers=headers, content=content
        )
        return _result(op, response)


def _result(op: Operation, response: httpx2.Response) -> types.CallToolResult:
    content = response.content[: MAX_RESPONSE_BYTES + 1]
    if len(content) > MAX_RESPONSE_BYTES:
        return types.CallToolResult(
            content=[types.TextContent(type="text", text="the response is over 1 MB")], is_error=True
        )
    if not 200 <= response.status_code < 300:
        detail = content.decode("utf-8", errors="replace")[:500]
        meta: dict[str, Any] | None = {"ekl": {"effect": "none"}} if response.status_code in REFUSED else None
        if response.status_code in UNAVAILABLE:
            # The API's gateway or the API itself can't answer now: another of the agent's servers may (upstreams).
            meta = {"ekl": {**(meta or {}).get("ekl", {}), "unavailable": True}}
        return types.CallToolResult(
            content=[types.TextContent(type="text", text=f"HTTP {response.status_code}: {detail}".strip())],
            is_error=True,
            meta=meta,
        )
    schema = op.tool["outputSchema"]
    if schema == NO_BODY_OUTPUT:
        structured: dict[str, Any] = {"status": response.status_code}
    else:
        try:
            body = json.loads(content) if content else None
        except ValueError:
            return types.CallToolResult(
                content=[types.TextContent(type="text", text="the API answered with something other than JSON")],
                is_error=True,
            )
        body = safe_integers(body)
        structured = {"result": body} if op.wrapped or not isinstance(body, dict) else body
    return types.CallToolResult(
        content=[types.TextContent(type="text", text=json.dumps(structured, ensure_ascii=False))],
        structured_content=structured,
    )


def safe_integers(value: Any) -> Any:
    """An answer as ekl-json-1 carries it: an integer beyond ±2^53 becomes its exact decimal string (§5.3)."""
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and abs(value) > MAX_SAFE_INTEGER:
        return str(value)
    if isinstance(value, list):
        return [safe_integers(v) for v in value]
    if isinstance(value, dict):
        return {k: safe_integers(v) for k, v in value.items()}
    return value


def int64_numbers(value: Any, schema: dict[str, Any]) -> Any:
    """A request body as the API expects it: an int64 field given as its decimal string, as an answer gave it, is
    sent as the number."""
    kind = schema.get("type")
    kinds: list[Any] = kind if isinstance(kind, list) else [kind]
    if isinstance(value, str) and schema.get("format") == "int64" and "integer" in kinds:
        return int(value) if value.lstrip("-").isdigit() else value
    if isinstance(value, dict):
        props = schema.get("properties") or {}
        return {k: int64_numbers(v, props.get(k) or {}) for k, v in value.items()}
    if isinstance(value, list):
        items = schema.get("items")
        return [int64_numbers(v, items if isinstance(items, dict) else {}) for v in value]
    return value


def _refused(message: str) -> types.CallToolResult:
    return types.CallToolResult(
        content=[types.TextContent(type="text", text=message)], is_error=True, meta={"ekl": {"effect": "none"}}
    )
