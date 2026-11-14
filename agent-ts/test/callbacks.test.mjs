// Copyright © 2026 EuKreate, Inc.
// SPDX-License-Identifier: Apache-2.0
// Callback protocol v2 on the app's side (WP5): signatures checked before anything else, answers kept and signed,
// and the handler run once, under a fenced lease. The connector's local API from the app's side (WP3).

import { test } from "node:test";
import assert from "node:assert/strict";
import { createHash, createHmac } from "node:crypto";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { callbackKey, callbacks, createAgent, fileStore, localApiUrl, receiver, signRequest } from "../index.mjs";

const KEY = createHmac("sha256", "ekla_secret").update("ekl-callback-v2").digest();
const AGENT = "agt_me";
const sha256 = (data) => createHash("sha256").update(data).digest("hex");

function folder() {
  return join(mkdtempSync(join(tmpdir(), "ekl-cb-")), "callbacks");
}

function request(eventId, data, t = Math.floor(Date.now() / 1000)) {
  const body = JSON.stringify({ event: "message", event_id: eventId, agent_id: AGENT, ...data });
  return [body, { "X-EKL-Signature-2": signRequest(KEY, t, eventId, AGENT, body) }];
}

function make(onEvent, options = {}) {
  return receiver(onEvent, { key: KEY.toString("hex"), agentId: AGENT, dir: folder(), ...options });
}

test("the MACs match the protocol's callback-v2 vectors", () => {
  const vectors = join(fileURLToPath(new URL("../../contracts/vectors/callback-v2.json", import.meta.url)));
  if (!existsSync(vectors)) return;
  const v = JSON.parse(readFileSync(vectors, "utf8"));
  const key = callbackKey({ credential: v.agent_credential, key: "" });
  assert.equal(key.toString("hex"), v.callback_key);
  const r = v.request;
  assert.equal(signRequest(key, r.t, r.event_id, r.agent_id, r.body), r.header);
});

test("a retry whose first answer was lost gets the stored answer, and the handler ran once", async () => {
  let runs = 0;
  const receive = make(() => ({ reply: `answer ${++runs}` }));
  const [body, headers] = request("evt_1", { text: "hi" });
  const first = await receive(body, headers);
  const retry = await receive(body, request("evt_1", { text: "hi" }, Math.floor(Date.now() / 1000) + 1)[1]);
  assert.equal(runs, 1);
  assert.equal(retry.body.toString(), first.body.toString());
  const sig = Object.fromEntries(retry.headers["X-EKL-Response-Signature-2"].split(",").map((p) => p.split("=")));
  assert.equal(sig.d, sha256(body));
  const v = createHmac("sha256", KEY)
    .update(["ekl-cb-resp-v2", sig.t, "evt_1", AGENT, sha256(body), "200", sha256(retry.body)].join("\n"))
    .digest("hex");
  assert.equal(sig.v, v);
});

test("a replay after 300 s is refused, and the same event with another body conflicts", async () => {
  let runs = 0;
  const receive = make(() => ({ reply: String(++runs) }));
  const [body, old] = request("evt_1", { text: "hi" }, Math.floor(Date.now() / 1000) - 301);
  assert.equal((await receive(body, old)).status, 401);
  const [fresh, headers] = request("evt_1", { text: "hi" });
  assert.equal((await receive(fresh, headers)).status, 200);
  const [other, otherHeaders] = request("evt_1", { text: "changed" });
  const conflict = await receive(other, otherHeaders);
  assert.equal(conflict.status, 409);
  assert.match(conflict.body.toString(), /CONFLICT/);
  assert.equal(runs, 1);
});

test("deduplication survives a restart", async () => {
  const dir = folder();
  let runs = 0;
  const onEvent = () => ({ reply: String(++runs) });
  const [body, headers] = request("evt_1", { text: "hi" });
  const first = await receiver(onEvent, { key: KEY.toString("hex"), agentId: AGENT, dir })(body, headers);
  const again = await receiver(onEvent, { key: KEY.toString("hex"), agentId: AGENT, dir })(body, headers);
  assert.equal(runs, 1);
  assert.equal(again.body.toString(), first.body.toString());
});

test("a bad MAC is refused without a store row or the handler", async () => {
  const dir = folder();
  let runs = 0;
  const receive = receiver(() => ++runs, { key: KEY.toString("hex"), agentId: AGENT, dir });
  const [body, headers] = request("evt_1", { text: "hi" });
  const forged = { "X-EKL-Signature-2": headers["X-EKL-Signature-2"].slice(0, -4) + "0000" };
  assert.equal((await receive(body, forged)).status, 401);
  assert.equal(runs, 0);
  assert.deepEqual(readdirSync(dir), []);
});

test("two simultaneous deliveries run the handler once; the other gets IN_PROGRESS, then the answer", async () => {
  let release;
  const held = new Promise((r) => (release = r));
  let runs = 0;
  const receive = make(async () => {
    runs += 1;
    await held;
    return { reply: "once" };
  });
  const [body, headers] = request("evt_1", { text: "hi" });
  const first = receive(body, headers);
  await new Promise((r) => setTimeout(r, 20));
  const second = await receive(body, headers);
  assert.equal(second.status, 409);
  assert.equal(second.headers["retry-after"], "2");
  release();
  await first;
  assert.equal((await receive(body, headers)).body.toString(), JSON.stringify({ reply: "once" }));
  assert.equal(runs, 1);
});

test("a handler paused past its lease is taken over, and its own commit writes nothing", async () => {
  const dir = folder();
  let clock = 1_800_000_000;
  const now = () => clock;
  const store = fileStore(dir);
  let release;
  const held = new Promise((r) => (release = r));
  let calls = 0;
  const receive = receiver(
    async (_, ctx) => {
      calls += 1;
      if (calls === 1) {
        await held;
        ctx.write("who", "first");
        return { reply: "first" };
      }
      ctx.write("who", "second");
      return { reply: "second" };
    },
    { key: KEY.toString("hex"), agentId: AGENT, store, now, renewSeconds: 3600 },
  );
  const [body, headers] = request("evt_1", { text: "hi" }, clock);
  const first = receive(body, headers);
  await new Promise((r) => setTimeout(r, 20));
  clock += 61; // past the 60 s lease
  const [, retryHeaders] = request("evt_1", { text: "hi" }, clock);
  const second = await receive(body, retryHeaders);
  release();
  const resumed = await first;
  assert.equal(second.body.toString(), JSON.stringify({ reply: "second" }));
  assert.equal(resumed.status, 409);
  assert.equal((await receive(body, retryHeaders)).body.toString(), JSON.stringify({ reply: "second" }));
  assert.equal(await store.recall("who"), "second");
});

test("callbacks serves the receiver as a node:http route", async () => {
  const server = createServer(callbacks(() => ({ accept: true }), { key: KEY.toString("hex"), agentId: AGENT, dir: folder() }));
  await new Promise((r) => server.listen(0, "127.0.0.1", r));
  const [body, headers] = request("evt_9", {});
  try {
    const answer = await fetch(`http://127.0.0.1:${server.address().port}/`, { method: "POST", body, headers });
    assert.equal(answer.status, 200);
    assert.ok(answer.headers.get("x-ekl-response-signature-2"));
    assert.deepEqual(await answer.json(), { accept: true });
  } finally {
    server.close();
  }
});

test("the client finds the connector through .ekl/local-api.json, sends its token and no Origin", async () => {
  const seen = [];
  const server = createServer((req, res) => {
    seen.push(req.headers);
    res.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify({ candidates: [] }));
  });
  await new Promise((r) => server.listen(0, "127.0.0.1", r));
  const project = mkdtempSync(join(tmpdir(), "ekl-app-"));
  mkdirSync(join(project, ".ekl"));
  const url = `http://127.0.0.1:${server.address().port}`;
  writeFileSync(join(project, ".ekl", "local-api.json"), JSON.stringify({ url, pid: process.pid, started_at: "" }));
  try {
    assert.equal(localApiUrl({ cwd: project }), url);
    const agent = createAgent({ apiUrl: localApiUrl({ cwd: project }), live: true, token: "tok" });
    await agent.find("vat");
    assert.equal(seen[0].authorization, "Bearer tok");
    assert.equal(seen[0].origin, undefined);
  } finally {
    server.close();
  }
  writeFileSync(join(project, ".ekl", "local-api.json"), JSON.stringify({ url, pid: 99999999, started_at: "" }));
  assert.throws(() => localApiUrl({ cwd: project }), (e) => e.code === "CONNECTOR_NOT_RUNNING");
});

test("a 401 from the connector names the token to set", async () => {
  const agent = createAgent({
    apiUrl: "http://127.0.0.1:9",
    live: true,
    fetch: async () => new Response(JSON.stringify({ error: "UNAUTHORIZED" }), { status: 401 }),
  });
  await assert.rejects(agent.find("x"), (e) => e.code === "UNAUTHORIZED" && e.fix.includes("EKL_LOCAL_TOKEN"));
});
