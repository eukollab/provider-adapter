# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Files travelling beside calls (EKP 3.5 §5.5): a large base64 file leaves the call for the file store and comes back
as base64 where it is used, each side checking its SHA-256, so the tool and the agent's code see bytes as before while
the call and its fingerprints carry only references."""

import base64
import hashlib
import json
from pathlib import Path
from typing import Any

import httpx2
import mcp_types as types
import pytest

from ekl_provider_adapter.agent import Agent
from ekl_provider_adapter.agent_journal import AgentJournal
from ekl_provider_adapter.config import AgentFile
from ekl_provider_adapter.conformance.harness import CREDENTIAL, FAST, Call, Harness
from ekl_provider_adapter.conformance.toolbox import ENDPOINT
from ekl_provider_adapter.core import AdapterCore, Hooks
from ekl_provider_adapter.files import (
    INLINE_BYTES,
    PREFIX,
    FileTransfer,
    FileUnavailableError,
    large,
    media_type,
    references,
)
from ekl_provider_adapter.testing import FakeNetwork
from tests.conftest import use

pytestmark = pytest.mark.anyio

PNG = b"\x89PNG\r\n\x1a\n" + bytes(range(256)) * (INLINE_BYTES // 256 + 10)
SMALL = base64.b64encode(b"tiny").decode()


class Store:
    """The file store and the network's two file routes, in memory: uploads and downloads by presigned address."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.uploads: list[tuple[str, int, str]] = []
        self.http = httpx2.AsyncClient(transport=httpx2.MockTransport(self._serve))

    def _serve(self, request: httpx2.Request) -> httpx2.Response:
        key = request.url.path.rsplit("/", 1)[-1]
        if request.method == "PUT":
            self.objects[key] = request.content
            return httpx2.Response(200)
        if key in self.objects:
            return httpx2.Response(200, content=self.objects[key])
        return httpx2.Response(404)

    async def upload(self, sha256: str, size: int, kind: str) -> dict[str, Any]:
        self.uploads.append((sha256, size, kind))
        return {"ref": PREFIX + sha256, "upload_url": f"https://store.test/{sha256}"}

    async def download(self, sha256: str) -> dict[str, Any]:
        return {"url": f"https://store.test/{sha256}", "size": len(self.objects.get(sha256, b""))}

    def transfer(self) -> FileTransfer:
        return FileTransfer(self.upload, self.download, self.http)


def test_only_large_base64_leaves_the_call() -> None:
    encoded = base64.b64encode(PNG).decode()
    assert large(encoded) == PNG and media_type(PNG) == "image/png"
    assert large(SMALL) is None
    assert large("x" * (INLINE_BYTES * 2)) is not None  # base64 of zeros-like text still decodes
    assert large("not base64! " * INLINE_BYTES) is None
    assert references({"a": [PREFIX + "0" * 64]}) and not references({"a": "plain"})


async def test_a_file_goes_by_reference_and_comes_back_checked() -> None:
    store = Store()
    files = store.transfer()
    arguments = {"image": base64.b64encode(PNG).decode(), "note": "hello", "thumb": SMALL}
    sent = await files.send(arguments)
    sha256 = hashlib.sha256(PNG).hexdigest()
    assert sent == {"image": PREFIX + sha256, "note": "hello", "thumb": SMALL}
    assert store.uploads == [(sha256, len(PNG), "image/png")]
    assert await files.receive(sent) == arguments
    # A file that changed in the store, or is gone, never reaches the tool.
    store.objects[sha256] = b"other bytes"
    with pytest.raises(FileUnavailableError):
        await files.receive(sent)
    store.objects.clear()
    with pytest.raises(FileUnavailableError):
        await files.receive(sent)


async def test_a_result_sends_its_files_in_its_content_and_its_json_text() -> None:
    files = Store().transfer()
    structured = {"video": base64.b64encode(PNG).decode(), "frames": 3}
    result = types.CallToolResult(
        content=[types.TextContent(type="text", text=json.dumps(structured))], structured_content=structured
    )
    sent = await files.send_result(result)
    assert sent.structured_content is not None and sent.structured_content["video"].startswith(PREFIX)
    text = sent.content[0]
    assert isinstance(text, types.TextContent) and json.loads(text.text) == sent.structured_content


class FileApi:
    """The harness's network client with the network's file routes, backed by the in-memory store."""

    def __init__(self, inner: Any, store: Store) -> None:
        self.inner, self.store = inner, store
        self.file_http = store.http
        self.downloads: list[tuple[str, str]] = []

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)

    async def file_upload(self, execution_id: str, sha256: str, size: int, kind: str) -> dict[str, Any]:
        del execution_id  # one call's files: the store keeps them by hash
        return await self.store.upload(sha256, size, kind)

    async def file_download(self, sha256: str, execution_id: str) -> dict[str, Any]:
        self.downloads.append((sha256, execution_id))
        return await self.store.download(sha256)


async def test_the_tool_gets_the_bytes_and_the_caller_gets_a_reference() -> None:
    harness = Harness("storage_free")
    store = Store()
    seen: list[dict[str, Any]] = []
    output = base64.b64encode(PNG[::-1]).decode()

    async def invoke(tool: str, arguments: dict[str, Any]) -> types.CallToolResult:
        seen.append(arguments)
        return types.CallToolResult(content=[], structured_content={"video": output})

    api = FileApi(harness.api, store)
    hooks = Hooks(
        outcome_lookup={"sent_lookup": harness.toolbox.sent_lookup},
        cancellation={"stop_slow": harness.toolbox.stop_slow},
    )
    core = AdapterCore(harness.local, api, invoke, credential=CREDENTIAL, endpoint=ENDPOINT, hooks=hooks, settings=FAST)
    assert harness.network.configuration is not None
    core.apply_config(AgentFile.model_validate(harness.network.configuration), harness.network.config_version)
    sha256 = hashlib.sha256(PNG).hexdigest()
    store.objects[sha256] = PNG
    call = Call("lookup", {"image": PREFIX + sha256}, "ex_files_1")
    result = await core.handle(call.tool, call.arguments, harness.token, call.meta())
    assert not result.is_error, result.content
    assert seen == [{"image": base64.b64encode(PNG).decode()}]
    assert api.downloads == [(sha256, "ex_files_1")]
    back = (result.structured_content or {})["video"]
    assert back == PREFIX + hashlib.sha256(PNG[::-1]).hexdigest() and store.objects[back.removeprefix(PREFIX)]
    # A reference to a file that isn't there fails the call before the tool runs.
    gone = Call("lookup", {"image": PREFIX + "0" * 64}, "ex_files_2")
    refused = await core.handle(gone.tool, gone.arguments, harness.token, gone.meta())
    assert refused.is_error and len(seen) == 1
    await core.drain()
    harness.close()


def test_a_consumer_sends_its_files_by_reference_and_gets_the_results_back(tmp_path: Path) -> None:
    store = Store()
    fake = FakeNetwork()
    made = PNG[::-1]
    store.objects[hashlib.sha256(made).hexdigest()] = made
    received: list[dict[str, Any]] = []

    def make_video(arguments: dict[str, Any]) -> dict[str, Any]:
        received.append(arguments)
        return {"video": PREFIX + hashlib.sha256(made).hexdigest()}

    fake.provider("@video", "cap_make_video", make_video, summary="make a video from images")

    async def upload(execution_id: str, receiver: str, sha256: str, size: int, kind: str) -> dict[str, Any]:
        assert receiver == "agt_video" and execution_id.startswith("ex_")
        return await store.upload(sha256, size, kind)

    async def download(sha256: str, execution_id: str) -> dict[str, Any]:
        assert execution_id.startswith("ex_")
        return await store.download(sha256)

    fake.file_upload, fake.file_download, fake.file_http = upload, download, store.http  # type: ignore[attr-defined]
    agent = Agent(fake, AgentJournal(str(tmp_path / "journal.db")))
    [candidate] = agent.find("make a video")
    result = use(agent, candidate).call({"images": [base64.b64encode(PNG).decode()], "title": "Trip"})
    assert received == [{"images": [PREFIX + hashlib.sha256(PNG).hexdigest()], "title": "Trip"}]
    assert store.objects[hashlib.sha256(PNG).hexdigest()] == PNG
    assert result.value == {"video": base64.b64encode(made).decode()}
