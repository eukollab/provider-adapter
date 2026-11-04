// Copyright © 2026 EuKreate, Inc.
// SPDX-License-Identifier: Apache-2.0

/**
 * @eukollab/agent: EuKollab for apps in TypeScript or JavaScript.
 *
 * The connector (`ekl-provider-connector`) runs beside the app and does the network's work: it serves the app's
 * tools, posts the agent's events to the app (EKL_CALLBACK_URL) and takes the app's own actions on 127.0.0.1
 * (EKL_AGENT_API_PORT, default 8765). This client wraps that loopback API. With no agent-operation credential, here
 * or in the connector, a declared service answers from the stand-ins `ekl try --record` saved in eukollab/fixtures/,
 * the same files the Python adapter reads; so it does with no connector running (designing, unit tests, CI). Once
 * the app or its connector holds EKL_AGENT_CREDENTIAL, calls always go through the connector, so a stand-in can
 * never hide a live failure.
 *
 * No dependencies; Node 20+ and Deno.
 */

import { Buffer } from "node:buffer";
import { createHmac, timingSafeEqual } from "node:crypto";
import { readFileSync, readdirSync, existsSync } from "node:fs";
import { join } from "node:path";

const env = (name) => (typeof process !== "undefined" ? process.env[name] : undefined);

/** An error from the network, the connector or a stand-in, with the code the network uses and what to do. */
export class AgentError extends Error {
  constructor(message, { code = "", fix = "" } = {}) {
    super(message);
    this.name = "AgentError";
    this.code = code;
    this.fix = fix;
  }
}

// --- stand-ins ---------------------------------------------------------------------------------------------------

function fixturesDir(given) {
  return given ?? env("EKL_FIXTURES") ?? join("eukollab", "fixtures");
}

function splitRef(ref) {
  const at = ref.indexOf("/");
  if (!ref.startsWith("@") || at < 2 || at === ref.length - 1) {
    throw new AgentError(`Name the service as @handle/service, not ${JSON.stringify(ref)}.`, { code: "INVALID_REQUEST" });
  }
  return [ref.slice(0, at), ref.slice(at + 1)];
}

function loadStandIn(ref, dir) {
  const [handle, name] = splitRef(ref);
  const path = join(fixturesDir(dir), `${handle.slice(1)}.${name}.json`);
  return existsSync(path) ? JSON.parse(readFileSync(path, "utf8")) : null;
}

const TYPES = {
  string: (v) => typeof v === "string",
  number: (v) => typeof v === "number",
  integer: (v) => Number.isInteger(v),
  boolean: (v) => typeof v === "boolean",
  array: (v) => Array.isArray(v),
  object: (v) => v !== null && typeof v === "object" && !Array.isArray(v),
  null: (v) => v === null,
};

/**
 * The checks of a JSON Schema an app can act on: type, required fields, each property's type, enum and const.
 * Returns the first problem, or "" when the value passes. Enough for stand-ins and `expect`; the network checks
 * the full schema.
 */
export function schemaProblem(schema, value, where = "value") {
  if (!schema || typeof schema !== "object") return "";
  const kinds = Array.isArray(schema.type) ? schema.type : schema.type ? [schema.type] : [];
  if (kinds.length && !kinds.some((k) => TYPES[k]?.(value))) return `${where} must be ${kinds.join(" or ")}`;
  if (schema.enum && !schema.enum.some((e) => JSON.stringify(e) === JSON.stringify(value))) {
    return `${where} must be one of ${schema.enum.map((e) => JSON.stringify(e)).join(", ")}`;
  }
  if ("const" in schema && JSON.stringify(schema.const) !== JSON.stringify(value)) return `${where} is not allowed`;
  if (TYPES.object(value)) {
    for (const name of schema.required ?? []) {
      if (!(name in value)) return `${name} is required`;
    }
    for (const [name, inner] of Object.entries(schema.properties ?? {})) {
      if (name in value) {
        const found = schemaProblem(inner, value[name], name);
        if (found) return found;
      }
    }
  }
  return "";
}

function answerFromStandIn(prefer, input, dir) {
  if (!prefer) {
    throw new AgentError("Without an agent-operation credential, a service answers from its stand-in, and none is preferred.", {
      code: "STAND_IN_MISSING",
      fix: 'Name the service: agent.service("…", { prefer: "@handle/service" }), then record it with ekl try.',
    });
  }
  const fixture = loadStandIn(prefer, dir);
  if (!fixture?.examples?.length) {
    const [handle, name] = splitRef(prefer);
    throw new AgentError(`No recorded answer for ${prefer} in eukollab/fixtures.`, {
      code: "STAND_IN_MISSING",
      fix: `Record one: ekl try ${handle} ${name} --record (or set EKL_AGENT_CREDENTIAL to call it for real).`,
    });
  }
  const problem = schemaProblem(fixture.input_schema ?? { type: "object" }, input, "the input");
  if (problem) {
    throw new AgentError(`${prefer} would not accept this input: ${problem}`, {
      code: "INVALID_INPUT",
      fix: `Compare with its schema: ekl show ${prefer.replace("/", " ")}`,
    });
  }
  // An input with no recorded answer of its own gets the first one: a stand-in shows the answer's shape, not the work.
  const same = fixture.examples.find((e) => JSON.stringify(e.input) === JSON.stringify(input)) ?? fixture.examples[0];
  return { value: same.output, executionId: "stand-in", charged: "0", receiptChecked: false, standIn: true, provider: prefer };
}

// --- the agent ---------------------------------------------------------------------------------------------------

/**
 * The agent's own actions, through the connector's loopback API.
 *
 * @param {object} [options]
 * @param {string} [options.apiUrl] the connector's local API (default http://127.0.0.1:$EKL_AGENT_API_PORT)
 * @param {string} [options.fixtures] the stand-ins' folder (default $EKL_FIXTURES, else eukollab/fixtures)
 * @param {boolean} [options.live] whether the agent takes part (default: EKL_AGENT_CREDENTIAL is set here, or the
 *   connector says it holds it)
 * @param {typeof fetch} [options.fetch]
 */
export function createAgent(options = {}) {
  const apiUrl = (options.apiUrl ?? `http://127.0.0.1:${env("EKL_AGENT_API_PORT") ?? "8765"}`).replace(/\/$/, "");
  const doFetch = options.fetch ?? globalThis.fetch;
  // The credential is often only in the connector's environment (it reads .env; the app may not), so without it here
  // the connector is asked. Once live, the client stays live: a connector that stops answering is then an error,
  // never a stand-in.
  let live = options.live ?? (env("EKL_AGENT_CREDENTIAL") ? true : undefined);

  async function isLive() {
    if (live !== undefined) return live;
    let response;
    try {
      response = await doFetch(`${apiUrl}/agent/status`);
    } catch {
      return false; // no connector running: designing, unit tests, CI
    }
    const answer = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new AgentError(`The connector at ${apiUrl} didn't say whether it holds the agent credential.`, {
        code: answer.error ?? String(response.status),
      });
    }
    if (answer.live === true) live = true;
    return answer.live === true;
  }

  async function post(action, body) {
    let response;
    try {
      response = await doFetch(`${apiUrl}/agent/${action}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      });
    } catch {
      throw new AgentError(`The connector isn't answering at ${apiUrl}.`, {
        code: "CONNECTOR_UNREACHABLE",
        fix: "Run ekl-provider-connector beside your app (--use-only for an agent that offers nothing).",
      });
    }
    const answer = await response.json().catch(() => ({}));
    if (!response.ok) throw new AgentError(answer.message ?? `${action} failed`, { code: answer.error ?? String(response.status) });
    return answer;
  }

  const camel = (r) => ({
    value: r.value,
    executionId: r.execution_id,
    charged: r.charged,
    receiptChecked: r.receipt_checked,
    standIn: Boolean(r.stand_in),
    provider: r.provider,
  });

  return {
    /**
     * Declare a service the agent will use. `expect` is a JSON Schema its output must satisfy when
     * the network chooses a provider by trial. `ekl agent sync` writes the declaration into agent.json as `uses`.
     */
    service(need, { prefer, sample, expect } = {}) {
      return {
        need,
        prefer,
        async call(input, { idempotencyKey } = {}) {
          if (!(await isLive())) return answerFromStandIn(prefer, input, options.fixtures);
          const body = { need, prefer, sample, expect, input, idempotency_key: idempotencyKey };
          return camel(await post("service", body));
        },
      };
    },
    postNeed: (text, { topics = [], budget } = {}) => post("post_need", { text, topics, budget }),
    find: (query, { limit = 5 } = {}) => post("find", { query, limit }),
    choose: (query, { sample } = {}) => post("choose", { query, sample }),
    call: async (handle, capabilityId, args, { idempotencyKey } = {}) =>
      camel(await post("call", { handle, capability_id: capabilityId, arguments: args, idempotency_key: idempotencyKey })),
    message: (connectionId, text) => post("message", { connection_id: connectionId, text }),
    decideRun: (reservationId, decision) => post("decide_run", { reservation_id: reservationId, ...decision }),
  };
}

// --- callbacks ---------------------------------------------------------------------------------------------------

/**
 * Check a callback the connector posted to the app (EKL_CALLBACK_URL) and return its event. The signature is
 * `sha256=<hex>` of an HMAC-SHA256 over the raw body keyed by the agent credential, compared in constant time.
 * Pass the raw body exactly as received (a string or bytes), from an Express, Fastify or Hono route.
 */
export function handle(body, signature, secret = env("EKL_AGENT_CREDENTIAL")) {
  if (!secret) throw new AgentError("No agent credential to check the signature with.", { code: "AGENT_NOT_ENABLED" });
  const raw = Buffer.from(body);
  const expected = Buffer.from(`sha256=${createHmac("sha256", secret).update(raw).digest("hex")}`);
  const given = Buffer.from(String(signature ?? ""));
  if (given.length !== expected.length || !timingSafeEqual(given, expected)) {
    throw new AgentError("The callback's signature doesn't match.", { code: "UNAUTHORIZED" });
  }
  return JSON.parse(raw.toString("utf8"));
}

// --- testing -----------------------------------------------------------------------------------------------------

/** Test helpers: the network in memory, answering declared services from the project's stand-ins. */
export const testing = {
  /**
   * An agent whose declared services answer from the stand-ins (as FakeNetwork does in Python), recording every
   * action in `actions` for the test to check.
   */
  fakeNetwork({ fixtures } = {}) {
    const actions = [];
    const record = (action, body) => {
      actions.push({ action, ...body });
      return { ok: true };
    };
    const standIns = () => {
      const dir = fixturesDir(fixtures);
      return existsSync(dir) ? readdirSync(dir).filter((f) => f.endsWith(".json")) : [];
    };
    return {
      actions,
      standIns,
      service(need, { prefer } = {}) {
        return {
          need,
          prefer,
          async call(input) {
            record("service", { need, prefer, input });
            return { ...answerFromStandIn(prefer, input, fixtures), standIn: false };
          },
        };
      },
      postNeed: async (text, opts = {}) => record("post_need", { text, ...opts }),
      find: async (query) => record("find", { query }),
      choose: async (query) => record("choose", { query }),
      call: async (handle, capabilityId, args) => record("call", { handle, capabilityId, args }),
      message: async (connectionId, text) => record("message", { connectionId, text }),
      decideRun: async (reservationId, decision) => record("decide_run", { reservationId, ...decision }),
    };
  },
};
