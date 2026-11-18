# Copyright © 2026 EuKreate, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Callback protocol v2 (WP5): the connector's decision journal and signed delivery, against apps that answer, fail,
lose answers or sign for something else; and the Python template's durable receiver, driven directly."""

import asyncio
import datetime as dt
import importlib.util
import json
import threading
import time
from pathlib import Path
from types import ModuleType
from typing import Any

import httpx2
import pytest
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route

from ekl_provider_adapter import callback_signing as cb
from ekl_provider_adapter.agent import Agent
from ekl_provider_adapter.agent_journal import AgentJournal
from ekl_provider_adapter.callbacks import CallbackChannel, bridge
from ekl_provider_adapter.testing import FakeNetwork

CREDENTIAL = "ekla_secret"
KEY = cb.callback_key(CREDENTIAL)
AGENT_ID = "agt_me"
URL = "http://127.0.0.1/ekl-events"
TEMPLATES = Path(__file__).resolve().parents[2] / "devtools" / "templates" / "python"


def _signed_answer(request: Request, body: bytes, answer: dict[str, Any], *, digest: str | None = None) -> Response:
    """An app's v2 answer: the response signature bound to the request it answers (or to `digest`, to forge)."""
    fields = cb.parse(request.headers[cb.REQUEST_HEADER])
    payload = json.dumps(answer).encode()
    d = digest or cb.digest(body)
    t = int(time.time())
    v = cb.response_mac(KEY, t, fields["id"], json.loads(body)["agent_id"], d, 200, payload)
    return Response(payload, 200, {cb.RESPONSE_HEADER: f"t={t},id={fields['id']},d={d},v={v}"}, "application/json")


class App:
    """A publisher's app whose behaviour each test scripts, one entry per delivery."""

    def __init__(self, *script: str, answer: dict[str, Any] | None = None) -> None:
        self.script = list(script)
        self.answer = answer if answer is not None else {"accept": True}
        self.bodies: list[bytes] = []
        self.headers: list[dict[str, str]] = []

    async def route(self, request: Request) -> Response:
        body = await request.body()
        self.bodies.append(body)
        self.headers.append(dict(request.headers))
        # The last step repeats, so a background retry meets the same app.
        step = self.script.pop(0) if len(self.script) > 1 else (self.script[0] if self.script else "signed")
        if step == "timeout":
            raise httpx2.ReadTimeout("lost")
        if step == "in_progress":
            return Response(b'{"error": "IN_PROGRESS"}', 409, {"Retry-After": "2"})
        if step == "unsigned":
            return Response(json.dumps(self.answer).encode(), 200, media_type="application/json")
        if step == "other_digest":
            return _signed_answer(request, body, self.answer, digest="0" * 64)
        return _signed_answer(request, body, self.answer)

    def client(self) -> httpx2.AsyncClient:
        app = Starlette(routes=[Route("/ekl-events", self.route, methods=["POST"])])
        return httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app, raise_app_exceptions=True), base_url=URL)


class TimeoutTransport(httpx2.AsyncBaseTransport):
    """Raises a timeout on the deliveries named, else hands the request to the app."""

    def __init__(self, app: App) -> None:
        self.app = app
        self.inner = httpx2.ASGITransport(app=Starlette(routes=[Route("/ekl-events", app.route, methods=["POST"])]))

    async def handle_async_request(self, request: httpx2.Request) -> httpx2.Response:
        if self.app.script and self.app.script[0] == "timeout":
            self.app.script.pop(0)
            self.app.bodies.append(request.content)
            raise httpx2.ReadTimeout("no answer in time")
        return await self.inner.handle_async_request(request)


def _channel(app: App, tmp_path: Path, **options: Any) -> CallbackChannel:
    http = httpx2.AsyncClient(transport=TimeoutTransport(app), base_url=URL)
    return CallbackChannel(
        URL, CREDENTIAL, AgentJournal(str(tmp_path / "journal.db")), agent_id=lambda: AGENT_ID, http=http, **options
    )


async def _no_sleep(_: float) -> None:
    await asyncio.sleep(0)


async def _forever(_: float) -> None:
    await asyncio.Event().wait()


def _decisions(app: App, tmp_path: Path, **options: Any) -> tuple[CallbackChannel, list[str]]:
    """A channel whose connection requests record the connection each applied answer accepts."""
    channel = _channel(app, tmp_path, **options)
    accepted: list[str] = []

    async def perform(request: dict[str, Any], answer: dict[str, Any]) -> None:
        if answer.get("accept"):
            accepted.append(str(request["connection_id"]))

    channel.performers["connection_request"] = perform
    return channel, accepted


# --- the connector -------------------------------------------------------------------------------------------------


def test_a_request_carries_a_v2_signature_over_its_exact_body(tmp_path: Path) -> None:
    app = App()
    channel = _channel(app, tmp_path)
    answer = asyncio.run(channel.decide("connection_request", {"connection_id": "con_1"}, "evt_1"))
    assert answer == {"accept": True}
    fields = cb.parse(app.headers[0][cb.REQUEST_HEADER.lower()])
    body = app.bodies[0]
    assert fields["id"] == "evt_1" and json.loads(body)["agent_id"] == AGENT_ID
    assert fields["v"] == cb.request_mac(KEY, int(fields["t"]), "evt_1", AGENT_ID, body)
    # Not negotiated yet and before 2027-02-11: the v1 header goes too, for apps that only know it.
    assert app.headers[0][cb.LEGACY_HEADER.lower()] == cb.legacy_signature(CREDENTIAL, body)
    assert channel.negotiated


def test_ac265_once_negotiated_an_unsigned_accept_leaves_the_request_undecided(tmp_path: Path) -> None:
    app = App("signed", "unsigned")
    channel, accepted = _decisions(app, tmp_path, sleep=_no_sleep)

    async def go() -> None:
        await channel.decide("connection_request", {"connection_id": "con_first"}, "evt_first")  # v2 negotiated
        await channel.decide("connection_request", {"connection_id": "con_second"}, "evt_second")  # unsigned: failed
        await channel.stop()

    asyncio.run(go())
    assert accepted == ["con_first"]
    second = channel.journal.decision("evt_second")
    assert second is not None and second.state == "pending"
    assert cb.LEGACY_HEADER.lower() not in app.headers[1]  # no v1 header once negotiated


def test_ac265_a_legacy_peers_unsigned_answer_counts_only_until_the_cutoff(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    before, accepted_before = _decisions(App("unsigned"), tmp_path / "a")
    asyncio.run(before.decide("connection_request", {"connection_id": "con_1"}, "evt_1"))
    assert accepted_before == ["con_1"]
    assert "without a callback v2 signature" in caplog.text
    after, accepted_after = _decisions(
        App("unsigned"), tmp_path / "b", legacy_until=dt.datetime(2020, 1, 1, tzinfo=dt.UTC), sleep=_no_sleep
    )

    async def go() -> None:
        await after.decide("connection_request", {"connection_id": "con_1"}, "evt_1")
        await after.stop()

    asyncio.run(go())
    assert accepted_after == []


def test_ac266_a_valid_signature_for_another_request_digest_is_not_applied(tmp_path: Path) -> None:
    app = App("other_digest")
    fake = FakeNetwork()
    agent = Agent(fake, AgentJournal(str(tmp_path / "journal.db")))
    channel = _channel(app, tmp_path, sleep=_no_sleep)
    channel.journal = agent.journal
    bridge(agent, URL, CREDENTIAL, channel=channel)
    fake.connection_request("@stranger")
    fake.run(agent)
    assert fake.responses == []


def test_ac267_two_concurrent_answers_to_one_decision_apply_it_once(tmp_path: Path) -> None:
    journal = AgentJournal(str(tmp_path / "journal.db"))
    journal.open_decision("evt_1", "connection_request", "{}", "d1", time.time() + 60)
    results: list[bool] = []

    def answer(value: dict[str, Any]) -> None:
        results.append(journal.apply_decision("evt_1", "d1", value))

    threads = [threading.Thread(target=answer, args=({"accept": n == 0},)) for n in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert results.count(True) == 1
    row = journal.decision("evt_1")
    assert row is not None and row.state == "applied"


def test_ac328_a_retry_after_a_timed_out_delivery_gets_the_stored_answer(tmp_path: Path) -> None:
    app = App("timeout", "signed")
    channel, accepted = _decisions(app, tmp_path, sleep=_no_sleep)

    async def go() -> None:
        await channel.decide("connection_request", {"connection_id": "con_1"}, "evt_1")
        for _ in range(50):
            if accepted:
                break
            await asyncio.sleep(0.01)
        await channel.stop()

    asyncio.run(go())
    assert accepted == ["con_1"]
    assert app.bodies[0] == app.bodies[1]  # the retry sends the same bytes under the same event ID


def test_ac328_an_answer_after_the_deadline_defaulted_changes_nothing(tmp_path: Path) -> None:
    app = App("signed", answer={"accept": True})
    fake = FakeNetwork()
    agent = Agent(fake, AgentJournal(str(tmp_path / "journal.db")))
    channel = _channel(app, tmp_path)
    channel.journal = agent.journal
    bridge(agent, URL, CREDENTIAL, channel=channel)
    body = channel.body_for("connection_request", "evt_late", {"connection_id": "con_9"})
    agent.journal.open_decision("evt_late", "connection_request", body.decode(), cb.digest(body), time.time() - 5)

    async def go() -> None:
        agent._loop = asyncio.get_running_loop()
        assert await channel.apply_defaults() == 1
        late = agent.journal.decision("evt_late")
        assert late is not None
        assert await channel._attempt(late) is None

    asyncio.run(go())
    assert [r["decision"] for r in fake.responses] == ["decline"]  # the default, and nothing after it


def test_ac339_an_answer_one_second_after_the_deadline_is_refused_before_the_worker_runs(tmp_path: Path) -> None:
    journal = AgentJournal(str(tmp_path / "journal.db"))
    journal.open_decision("evt_1", "connection_request", "{}", "d1", time.time() - 1)
    assert not journal.apply_decision("evt_1", "d1", {"accept": True})  # the worker hasn't run: still refused
    assert [r.event_id for r in journal.default_due()] == ["evt_1"]
    row = journal.decision("evt_1")
    assert row is not None and row.state == "defaulted"
    assert not journal.apply_decision("evt_1", "d1", {"accept": True})


def test_the_journal_survives_a_restart_and_picks_up_what_is_pending(tmp_path: Path) -> None:
    app = App("in_progress", "signed")
    first = _channel(app, tmp_path, sleep=_forever)  # the process stops before any retry

    async def deliver_once() -> None:
        await first.decide("connection_request", {"connection_id": "con_1"}, "evt_1")
        await first.stop()

    asyncio.run(deliver_once())
    again = _channel(app, tmp_path, sleep=_no_sleep)
    applied: list[dict[str, Any]] = []

    async def perform(_: dict[str, Any], answer: dict[str, Any]) -> None:
        applied.append(answer)

    again.performers["connection_request"] = perform

    async def restart() -> None:
        await again.start()
        for _ in range(50):
            if applied:
                break
            await asyncio.sleep(0.01)
        await again.stop()

    asyncio.run(restart())
    assert applied == [{"accept": True}]


def test_events_go_only_to_callbacks_on_this_machine() -> None:
    journal = AgentJournal(":memory:")
    assert CallbackChannel("http://127.0.0.1:8002/", "ekla_x", journal).url == "http://127.0.0.1:8002/"
    for elsewhere in ("https://app.example/ekl-events", "http://app.example/ekl-events"):
        with pytest.raises(ValueError, match="this machine"):
            CallbackChannel(elsewhere, "ekla_x", journal)


# --- the Python template's receiver ----------------------------------------------------------------------------------


def _template(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, name: str = "callbacks") -> ModuleType:
    if not (TEMPLATES / "eukollab_callbacks.py").is_file():
        # The template is ekl's, not this package's: the adapter's own repository has no copy of it.
        pytest.skip("the eukollab_callbacks.py template ships with ekl")
    text = (TEMPLATES / "eukollab_callbacks.py").read_text().replace("{{name}}", "Homes")
    path = tmp_path / "eukollab_callbacks.py"
    path.write_text(text.replace("{{adapter_version}}", "0"))
    monkeypatch.delenv("EKL_AGENT_CREDENTIAL", raising=False)
    monkeypatch.delenv("EKL_PROVIDER_CREDENTIAL", raising=False)
    monkeypatch.setenv("EKL_CALLBACK_KEY", KEY.hex())
    monkeypatch.setenv("EKL_AGENT_ID", AGENT_ID)
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _request(event_id: str, data: dict[str, Any], *, t: float | None = None) -> tuple[bytes, dict[str, str]]:
    body = json.dumps({"event": "message", "event_id": event_id, "agent_id": AGENT_ID, **data}).encode()
    when = int(t if t is not None else time.time())
    return body, {cb.REQUEST_HEADER: cb.request_header(KEY, when, event_id, AGENT_ID, body)}


def _counting(module: ModuleType, answer: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    seen: list[dict[str, Any]] = []

    def on_message(event: dict[str, Any]) -> dict[str, Any]:
        seen.append(event)
        return answer or {"reply": f"answer {len(seen)}"}

    module.on_message = on_message
    return seen


def _rows(module: ModuleType) -> int:
    if not module.STORE.exists():
        return 0
    with module._store() as db:
        return int(db.execute("SELECT COUNT(*) FROM events").fetchone()[0])


def test_ac263_a_retry_whose_first_answer_was_lost_gets_the_stored_answer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _template(tmp_path, monkeypatch)
    seen = _counting(module)
    body, headers = _request("evt_1", {"text": "hi"})
    first = module.receive(body, headers)
    _, retry_headers = _request("evt_1", {"text": "hi"}, t=time.time() + 1)  # a new t and MAC, the same body
    retry = module.receive(body, retry_headers)
    assert len(seen) == 1 and first[2] == retry[2] == b'{"reply": "answer 1"}'
    assert cb.response_valid(
        KEY,
        retry[1][cb.RESPONSE_HEADER],
        event_id="evt_1",
        agent_id=AGENT_ID,
        request_digest=cb.digest(body),
        status=200,
        body=retry[2],
        now=time.time(),
    )


def test_ac264_a_replay_after_300_s_is_refused_and_a_changed_body_conflicts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _template(tmp_path, monkeypatch)
    seen = _counting(module)
    body, old = _request("evt_1", {"text": "hi"}, t=time.time() - 301)
    assert module.receive(body, old)[0] == 401 and not seen
    body, headers = _request("evt_1", {"text": "hi"})
    assert module.receive(body, headers)[0] == 200
    other, other_headers = _request("evt_1", {"text": "something else"})
    status, _, payload = module.receive(other, other_headers)
    assert status == 409 and b"CONFLICT" in payload and len(seen) == 1


def test_ac268_deduplication_survives_a_restart(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    module = _template(tmp_path, monkeypatch)
    _counting(module)
    body, headers = _request("evt_1", {"text": "hi"})
    first = module.receive(body, headers)
    restarted = _template(tmp_path, monkeypatch, name="callbacks_again")
    seen = _counting(restarted)
    assert restarted.receive(body, headers)[2] == first[2] and not seen


def test_ac329_a_bad_mac_is_refused_without_a_store_row_or_the_handler(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _template(tmp_path, monkeypatch)
    seen = _counting(module)
    body, headers = _request("evt_1", {"text": "hi"})
    forged = {cb.REQUEST_HEADER: headers[cb.REQUEST_HEADER][:-4] + "0000"}
    assert module.receive(body, forged)[0] == 401
    assert not seen and _rows(module) == 0


def test_ac330_two_simultaneous_deliveries_run_the_handler_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _template(tmp_path, monkeypatch)
    release = threading.Event()
    seen: list[str] = []

    def on_message(event: dict[str, Any]) -> dict[str, Any]:
        seen.append(event["event_id"])
        release.wait(5)
        return {"reply": "once"}

    module.on_message = on_message
    body, headers = _request("evt_1", {"text": "hi"})
    results: list[tuple[int, dict[str, str], bytes]] = []
    first = threading.Thread(target=lambda: results.append(module.receive(body, headers)))
    first.start()
    while not seen:
        time.sleep(0.01)
    second = module.receive(body, headers)
    assert second[0] == 409 and b"IN_PROGRESS" in second[2] and second[1]["Retry-After"] == "2"
    release.set()
    first.join()
    assert module.receive(body, headers)[2] == b'{"reply": "once"}' and seen == ["evt_1"]


def test_ac340_a_handler_paused_past_its_lease_is_taken_over_and_commits_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _template(tmp_path, monkeypatch)
    monkeypatch.setattr(module, "LEASE_SECONDS", 0.2)
    monkeypatch.setattr(module, "RENEW_SECONDS", 60.0)  # the paused handler doesn't renew
    paused = threading.Event()
    calls: list[int] = []

    def on_message(event: dict[str, Any]) -> dict[str, Any]:
        calls.append(len(calls) + 1)
        if len(calls) == 1:
            paused.wait(5)
            module.remember("who", "first")
            return {"reply": "first"}
        module.remember("who", "second")
        return {"reply": "second"}

    module.on_message = on_message
    body, headers = _request("evt_1", {"text": "hi"})
    results: list[tuple[int, dict[str, str], bytes]] = []
    first = threading.Thread(target=lambda: results.append(module.receive(body, headers)))
    first.start()
    time.sleep(0.4)  # past the lease
    second = module.receive(body, headers)
    paused.set()
    first.join()
    assert second[2] == b'{"reply": "second"}'
    assert results[0][0] == 409  # the first handler's commit found its fence stale
    assert module.receive(body, headers)[2] == b'{"reply": "second"}' and module.recalled("who") == "second"


def test_the_template_declines_connections_for_services_it_does_not_offer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _template(tmp_path, monkeypatch)
    (tmp_path / "agent.json").write_text(json.dumps({"capabilities": [{"tool": "search_homes", "class": "read_only"}]}))
    assert module.on_connection_request({"capability_ids": ["cap_search_homes"]}) == {"accept": True}
    assert "decline" in module.on_connection_request({"capability_ids": ["cap_other"]})
    assert "decline" in module.on_connection_request({"capability_ids": []})
