// Copyright © 2026 EuKreate, Inc.
// SPDX-License-Identifier: Apache-2.0

import { test } from "node:test";
import assert from "node:assert/strict";
import { createHmac } from "node:crypto";
import { mkdtempSync, mkdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { AgentError, createAgent, handle, schemaProblem, testing } from "../index.mjs";

function fixtures() {
  const dir = join(mkdtempSync(join(tmpdir(), "ekl-")), "eukollab", "fixtures");
  mkdirSync(dir, { recursive: true });
  writeFileSync(
    join(dir, "vat-check.validate.json"),
    JSON.stringify({
      ekl_fixture: "1",
      service: "@vat-check/validate",
      input_schema: { type: "object", properties: { vat_number: { type: "string" } }, required: ["vat_number"] },
      examples: [
        { input: { vat_number: "DE123456789" }, output: { valid: true } },
        { input: { vat_number: "XX0" }, output: { valid: false } },
      ],
    }),
  );
  return dir;
}

test("without a credential a declared service answers from its stand-in", async () => {
  const agent = createAgent({ fixtures: fixtures(), live: false });
  const vat = agent.service("validate EU VAT numbers", { prefer: "@vat-check/validate" });
  const result = await vat.call({ vat_number: "XX0" });
  assert.deepEqual(result.value, { valid: false });
  assert.equal(result.standIn, true);
  assert.deepEqual((await vat.call({ vat_number: "FR1" })).value, { valid: true });
  await assert.rejects(vat.call({ number: 1 }), (e) => e instanceof AgentError && e.code === "INVALID_INPUT");
  await assert.rejects(
    agent.service("tax ids", { prefer: "@tax-id/check" }).call({}),
    (e) => e.code === "STAND_IN_MISSING" && e.fix.includes("ekl try @tax-id check --record"),
  );
});

test("with a credential the connector calls the service", async () => {
  const sent = [];
  const fakeFetch = async (url, init) => {
    sent.push([url, JSON.parse(init.body)]);
    return new Response(
      JSON.stringify({ value: { valid: true }, execution_id: "ex_1", charged: "0.01", receipt_checked: true, stand_in: false, provider: "@vat-check" }),
      { status: 200 },
    );
  };
  const agent = createAgent({ apiUrl: "http://127.0.0.1:9999", live: true, fetch: fakeFetch });
  const result = await agent
    .service("validate EU VAT numbers", { prefer: "@vat-check/validate", expect: { type: "object", required: ["valid"] } })
    .call({ vat_number: "DE1" }, { idempotencyKey: "k1" });
  assert.equal(sent[0][0], "http://127.0.0.1:9999/agent/service");
  assert.equal(sent[0][1].idempotency_key, "k1");
  assert.deepEqual(sent[0][1].expect, { type: "object", required: ["valid"] });
  assert.deepEqual(result, { value: { valid: true }, executionId: "ex_1", charged: "0.01", receiptChecked: true, standIn: false, provider: "@vat-check" });
});

// The tests decide the credential themselves: none in this process, the connector's in its answer to /agent/status.
delete process.env.EKL_AGENT_CREDENTIAL;

// A connector beside the app; `up` false stops it answering. Every address asked is recorded in `sent`.
function connector(state) {
  state.sent = [];
  return async (url) => {
    state.sent.push(url);
    if (!state.up) throw new TypeError("fetch failed");
    if (url.endsWith("/agent/status")) return new Response(JSON.stringify({ live: state.live }));
    return new Response(JSON.stringify({ value: { valid: true }, execution_id: "ex_1", stand_in: false, provider: "@vat-check" }));
  };
}

test("the client is live when the connector beside it holds the credential", async () => {
  const state = { up: true, live: true };
  const agent = createAgent({ apiUrl: "http://127.0.0.1:9999", fixtures: fixtures(), fetch: connector(state) });
  const vat = agent.service("validate EU VAT numbers", { prefer: "@vat-check/validate" });
  const result = await vat.call({ vat_number: "XX0" });
  assert.deepEqual(state.sent, ["http://127.0.0.1:9999/agent/status", "http://127.0.0.1:9999/agent/service"]);
  assert.equal(result.standIn, false);
  assert.deepEqual(result.value, { valid: true }); // the stand-in would have said false
  // Once live, a connector that stops answering is an error, never a stand-in.
  state.up = false;
  await assert.rejects(vat.call({ vat_number: "XX0" }), (e) => e.code === "CONNECTOR_UNREACHABLE");
});

test("without a credential anywhere, or with no connector running, the stand-in answers", async () => {
  for (const state of [{ up: true, live: false }, { up: false }]) {
    const agent = createAgent({ fixtures: fixtures(), fetch: connector(state) });
    const result = await agent.service("vat", { prefer: "@vat-check/validate" }).call({ vat_number: "XX0" });
    assert.equal(result.standIn, true);
    assert.deepEqual(result.value, { valid: false });
    assert.ok(state.sent.every((url) => url.endsWith("/agent/status")));
  }
});

test("a connector that isn't running says how to start it", async () => {
  const agent = createAgent({ live: true, fetch: async () => { throw new TypeError("fetch failed"); } });
  await assert.rejects(agent.find("vat"), (e) => e.code === "CONNECTOR_UNREACHABLE" && e.fix.includes("--use-only"));
});

test("handle checks the callback's signature in constant time", () => {
  const body = JSON.stringify({ event: "need", data: { text: "x" } });
  const signature = `sha256=${createHmac("sha256", "eklc_secret").update(body).digest("hex")}`;
  assert.deepEqual(handle(body, signature, "eklc_secret"), { event: "need", data: { text: "x" } });
  assert.throws(() => handle(body, "sha256=00", "eklc_secret"), (e) => e.code === "UNAUTHORIZED");
  assert.throws(() => handle(body, signature, "other"), (e) => e.code === "UNAUTHORIZED");
});

test("fakeNetwork answers from the stand-ins and records actions", async () => {
  const fake = testing.fakeNetwork({ fixtures: fixtures() });
  const answered = await fake.service("vat", { prefer: "@vat-check/validate" }).call({ vat_number: "DE123456789" });
  assert.deepEqual(answered.value, { valid: true });
  await fake.postNeed("Validate VAT", { topics: ["tax"] });
  assert.deepEqual(fake.actions.map((a) => a.action), ["service", "post_need"]);
  assert.deepEqual(fake.standIns(), ["vat-check.validate.json"]);
});

test("schemaProblem checks types, required fields and enums", () => {
  const schema = { type: "object", properties: { n: { type: "integer" }, k: { enum: ["a", "b"] } }, required: ["n"] };
  assert.equal(schemaProblem(schema, { n: 1, k: "a" }), "");
  assert.equal(schemaProblem(schema, {}), "n is required");
  assert.equal(schemaProblem(schema, { n: 1.5 }), "n must be integer");
  assert.match(schemaProblem(schema, { n: 1, k: "c" }), /must be one of/);
});

test("decideRun sends a paid run's decision to the connector", async () => {
  const sent = [];
  const fakeFetch = async (url, init) => {
    sent.push([url, JSON.parse(init.body)]);
    return new Response(JSON.stringify({ state: "held" }), { status: 200 });
  };
  const agent = createAgent({ apiUrl: "http://127.0.0.1:9999", live: true, fetch: fakeFetch });
  await agent.decideRun("rsv_1", { decision: "approve", runs: 500 });
  assert.equal(sent[0][0], "http://127.0.0.1:9999/agent/decide_run");
  assert.deepEqual(sent[0][1], { reservation_id: "rsv_1", decision: "approve", runs: 500 });
});
