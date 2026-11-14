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
import { createHash, createHmac, randomBytes, timingSafeEqual } from "node:crypto";
import {
  chmodSync,
  closeSync,
  existsSync,
  mkdirSync,
  openSync,
  readFileSync,
  readdirSync,
  renameSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { dirname, join, resolve } from "node:path";

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

// --- the connector's local API -------------------------------------------------------------------------------------

const headersFor = (token) => (token ? { authorization: `Bearer ${token}` } : {});

function alive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return error.code === "EPERM"; // alive, under another user
  }
}

/**
 * The connector's local API: EKL_AGENT_API_URL, else the address the connector wrote to .ekl/local-api.json (here,
 * or beside EKL_AGENT_FILE) once it listened, else 127.0.0.1:$EKL_AGENT_API_PORT (8765). A file naming a process that
 * is gone means the connector isn't running: that is said, never papered over with the default port.
 */
export function localApiUrl({ cwd = process.cwd() } = {}) {
  const given = env("EKL_AGENT_API_URL");
  if (given) return given.replace(/\/$/, "");
  const folders = [cwd, env("EKL_AGENT_FILE") ? dirname(resolve(env("EKL_AGENT_FILE"))) : undefined].filter(Boolean);
  for (const folder of folders) {
    const file = join(folder, ".ekl", "local-api.json");
    if (!existsSync(file)) continue;
    const found = JSON.parse(readFileSync(file, "utf8"));
    if (!alive(Number(found.pid))) {
      throw new AgentError("The connector isn't running.", {
        code: "CONNECTOR_NOT_RUNNING",
        fix: "Start it beside your app: ekl agent run, in the project's folder.",
      });
    }
    return String(found.url).replace(/\/$/, "");
  }
  return `http://127.0.0.1:${env("EKL_AGENT_API_PORT") ?? "8765"}`;
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
  const doFetch = options.fetch ?? globalThis.fetch;
  const token = options.token ?? env("EKL_LOCAL_TOKEN");
  let resolved = options.apiUrl?.replace(/\/$/, "");
  // Found at the first call, so an app may start before its connector: EKL_AGENT_API_URL, else the address the
  // connector wrote to .ekl/local-api.json once it listened, else the default port.
  const apiUrl = () => (resolved ??= localApiUrl());
  // The credential is often only in the connector's environment (it reads .env; the app may not), so without it here
  // the connector is asked. Once live, the client stays live: a connector that stops answering is then an error,
  // never a stand-in.
  let live = options.live ?? (env("EKL_AGENT_CREDENTIAL") ? true : undefined);

  async function isLive() {
    if (live !== undefined) return live;
    let response;
    try {
      response = await doFetch(`${apiUrl()}/agent/status`, { headers: headersFor(token) });
    } catch {
      return false; // no connector running: designing, unit tests, CI
    }
    const answer = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new AgentError(`The connector at ${apiUrl()} didn't say whether it holds the agent credential.`, {
        code: answer.error ?? String(response.status),
      });
    }
    if (answer.live === true) live = true;
    return answer.live === true;
  }

  async function post(action, body) {
    const base = apiUrl(); // a connector known to have stopped says so here
    let response;
    try {
      response = await doFetch(`${base}/agent/${action}`, {
        method: "POST",
        headers: { "content-type": "application/json", ...headersFor(token) },
        body: JSON.stringify(body),
      });
    } catch {
      throw new AgentError(`The connector isn't answering at ${base}.`, {
        code: "CONNECTOR_UNREACHABLE",
        fix: "Run the connector beside your app: ekl agent run, in the project's folder.",
      });
    }
    const answer = await response.json().catch(() => ({}));
    if (response.status === 401) {
      throw new AgentError(answer.message ?? "The connector's local API refused the request.", {
        code: "UNAUTHORIZED",
        fix: "Set EKL_LOCAL_TOKEN as the connector's .env holds it (ekl agent run writes it).",
      });
    }
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
    // What screening on this machine found in the result: {category, rules_version} each, never the text.
    safetyFlags: r.safety_flags ?? [],
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
    closeAgreement: (handle, capabilityId, { outcome = "completed" } = {}) =>
      post("close_agreement", { handle, capability_id: capabilityId, outcome }),
    decideRun: (reservationId, decision) => post("decide_run", { reservation_id: reservationId, ...decision }),
  };
}

// --- callbacks: protocol v2 ----------------------------------------------------------------------------------------

/*
 * The connector signs every event it posts to the app (EKL_CALLBACK_URL) with EKL_CALLBACK_KEY, a key derived from
 * the agent credential so the app never holds the credential: a request MAC over its time, event ID, agent ID and the
 * body's digest, and the app signs its answer, bound to the request it answers, under a different label. The receiver
 * checks the request MAC before it looks anything up, keeps each event 7 days in a durable store so a retry after a
 * lost answer gets the same answer back, and runs the handler at most once at a time under a renewed, fenced lease.
 *
 * "Once" covers the stored answer and what the handler writes through `ctx.write`: they commit together, only while
 * the handler still owns the event. Anything else it does (an email, another API, a paid call) must be keyed by
 * ctx.eventId so a repeat does nothing, e.g. agent.call(..., { idempotencyKey: `call-${ctx.eventId}` }).
 */

const REQUEST_HEADER = "x-ekl-signature-2";
const RESPONSE_HEADER = "X-EKL-Response-Signature-2";
const LEGACY_HEADER = "x-ekl-signature";
const KEY_LABEL = "ekl-callback-v2";
const WINDOW_SECONDS = 300;
const MAX_BODY = 1_048_576;
const KEPT_SECONDS = 7 * 86400;
// Unsigned v1 events from older connectors are accepted, with a warning, until this day.
const LEGACY_UNTIL = Date.parse("2027-02-11T00:00:00Z");

const sha256 = (data) => createHash("sha256").update(data).digest("hex");
const mac = (key, parts) => createHmac("sha256", key).update(parts.join("\n"), "utf8").digest("hex");

/** k_cb: EKL_CALLBACK_KEY as raw bytes, else derived from a credential in the environment (an older .env). */
export function callbackKey({ key = env("EKL_CALLBACK_KEY"), credential = env("EKL_AGENT_CREDENTIAL") || env("EKL_PROVIDER_CREDENTIAL") } = {}) {
  if (key) return Buffer.from(key, "hex");
  if (credential) return createHmac("sha256", credential).update(KEY_LABEL).digest();
  throw new AgentError("No key to check callbacks with: set EKL_CALLBACK_KEY, as .env holds it.", { code: "AGENT_NOT_ENABLED" });
}

/** The X-EKL-Signature-2 value for a request body: what the connector sends, for tests and other senders. */
export function signRequest(key, t, eventId, agentId, body) {
  const v = mac(key, ["ekl-cb-req-v2", String(t), eventId, agentId, sha256(Buffer.from(body))]);
  return `t=${t},id=${eventId},v=${v}`;
}

/** The X-EKL-Response-Signature-2 value for an answer to the request whose body digest is `requestDigest`. */
export function signResponse(key, t, eventId, agentId, requestDigest, status, body) {
  const v = mac(key, ["ekl-cb-resp-v2", String(t), eventId, agentId, requestDigest, String(status), sha256(Buffer.from(body))]);
  return `t=${t},id=${eventId},d=${requestDigest},v=${v}`;
}

function fields(header) {
  const found = {};
  for (const part of String(header ?? "").split(",")) {
    const at = part.indexOf("=");
    if (at > 0) found[part.slice(0, at).trim()] = part.slice(at + 1).trim();
  }
  return found;
}

function equal(a, b) {
  const x = Buffer.from(a);
  const y = Buffer.from(b);
  return x.length === y.length && timingSafeEqual(x, y);
}

/**
 * The default durable store: one JSON file per event under `.ekl/callbacks/` (folder 0700, files 0600). Inserting is
 * atomic across processes (an exclusive create); conditional updates are serialised within one process and written by
 * rename, so a reader never sees half a row. Several processes answering the same events at once should use a store
 * with transactions instead (pass `store` to `callbacks`), with the same five methods.
 */
export function fileStore(dir = join(".ekl", "callbacks")) {
  mkdirSync(dir, { recursive: true, mode: 0o700 });
  chmodSync(dir, 0o700);
  const path = (eventId) => join(dir, `${sha256(eventId)}.json`);
  let queue = Promise.resolve();
  const locked = (work) => {
    const run = queue.then(work);
    queue = run.catch(() => {});
    return run;
  };
  const read = (eventId) => (existsSync(path(eventId)) ? JSON.parse(readFileSync(path(eventId), "utf8")) : null);
  const write = (row) => {
    const tmp = `${path(row.eventId)}.${process.pid}.tmp`;
    writeFileSync(tmp, JSON.stringify(row), { mode: 0o600 });
    renameSync(tmp, path(row.eventId));
  };
  return {
    async get(eventId) {
      return read(eventId);
    },
    /** Insert the row unless one exists for its event: false when another delivery got there first. */
    insert: (row) =>
      locked(() => {
        let fd;
        try {
          fd = openSync(path(row.eventId), "wx", 0o600);
        } catch (error) {
          if (error.code === "EEXIST") return false;
          throw error;
        }
        writeFileSync(fd, JSON.stringify(row));
        closeSync(fd);
        return true;
      }),
    /** Apply `changes` only while the row matches `expect` (fence, state, and optionally a lease that has lapsed). */
    update: (eventId, expect, changes) =>
      locked(() => {
        const row = read(eventId);
        if (!row || row.fence !== expect.fence || row.state !== expect.state) return false;
        if (expect.leaseBefore !== undefined && !(row.leaseUntil < expect.leaseBefore)) return false;
        write({ ...row, ...(typeof changes === "function" ? changes(row) : changes) });
        return true;
      }),
    /** The latest value a handler wrote with ctx.write under `key`, committed with its answer. */
    async recall(key) {
      let found;
      let at = -1;
      for (const name of readdirSync(dir).filter((n) => n.endsWith(".json"))) {
        const row = JSON.parse(readFileSync(join(dir, name), "utf8"));
        if (row.state === "completed" && row.writes && key in row.writes && row.completedAt > at) {
          found = row.writes[key];
          at = row.completedAt;
        }
      }
      return found;
    },
    purge: (before) =>
      locked(() => {
        for (const name of readdirSync(dir).filter((n) => n.endsWith(".json"))) {
          const row = JSON.parse(readFileSync(join(dir, name), "utf8"));
          if (row.created < before) unlinkSync(join(dir, name));
        }
      }),
  };
}

/**
 * Receive one callback: the raw body and the request's headers in, `{status, headers, body}` out, for any framework.
 * `onEvent(event, ctx)` returns the answer ({reply}, {accept: true}, {decline}, {review}, {skip: true}, a run's
 * decision, or nothing); `ctx.write(key, value)` keeps a value with the answer, committed only with it.
 */
export function receiver(onEvent, options = {}) {
  const store = options.store ?? fileStore(options.dir);
  const now = options.now ?? (() => Date.now() / 1000);
  const newOwner = options.newOwner ?? (() => randomBytes(8).toString("hex"));
  const leaseSeconds = options.leaseSeconds ?? 60;
  const renewSeconds = options.renewSeconds ?? 20;
  const legacyUntil = options.legacyUntil ?? LEGACY_UNTIL;
  const json = (status, value, headers = {}) => ({
    status,
    headers: { "content-type": "application/json", ...headers },
    body: Buffer.from(value === undefined ? "" : JSON.stringify(value)),
  });

  async function legacy(raw, signature) {
    const credential = env("EKL_AGENT_CREDENTIAL") || env("EKL_PROVIDER_CREDENTIAL");
    if (!credential || now() * 1000 >= legacyUntil) return json(401);
    const expected = `sha256=${createHmac("sha256", credential).update(raw).digest("hex")}`;
    if (!equal(String(signature), expected)) return json(401);
    console.warn("@eukollab/agent: an unsigned v1 event from an older connector; upgrade it (ekl agent deploy)");
    const answer = await onEvent(JSON.parse(raw.toString("utf8")), { eventId: "", write() {} });
    return json(200, answer ?? undefined);
  }

  return async function receive(body, headers) {
    const raw = Buffer.from(body);
    if (raw.length > MAX_BODY) return json(413);
    const lower = Object.fromEntries(Object.entries(headers ?? {}).map(([k, v]) => [k.toLowerCase(), String(v)]));
    if (!lower[REQUEST_HEADER]) return lower[LEGACY_HEADER] ? legacy(raw, lower[LEGACY_HEADER]) : json(401);
    let key;
    let event;
    try {
      key = options.key ? Buffer.from(options.key, "hex") : callbackKey();
      event = JSON.parse(raw.toString("utf8"));
    } catch {
      return json(401);
    }
    const sig = fields(lower[REQUEST_HEADER]);
    const eventId = sig.id ?? "";
    const t = Number(sig.t);
    const agentId = options.agentId ?? env("EKL_AGENT_ID") ?? String(event?.agent_id ?? "");
    const digest = sha256(raw);
    const expected = mac(key, ["ekl-cb-req-v2", String(t), eventId, agentId, digest]);
    // Checked before anything is looked up: an unsigned or replayed request never touches the store.
    if (!Number.isInteger(t) || Math.abs(now() - t) > WINDOW_SECONDS || !equal(expected, sig.v ?? "")) return json(401);
    if (event?.event_id !== eventId) return json(401);

    const signed = (status, payload) => {
      const at = Math.floor(now());
      return {
        status,
        headers: {
          "content-type": "application/json",
          [RESPONSE_HEADER]: signResponse(key, at, eventId, agentId, digest, status, payload),
        },
        body: payload,
      };
    };
    const inProgress = () => json(409, { error: "IN_PROGRESS" }, { "retry-after": "2" });

    await store.purge(now() - KEPT_SECONDS);
    const owner = newOwner();
    let fence = 1;
    const inserted = await store.insert({
      eventId,
      requestDigest: digest,
      state: "in_progress",
      fence,
      owner,
      leaseUntil: now() + leaseSeconds,
      created: now(),
    });
    if (!inserted) {
      const row = await store.get(eventId);
      if (!row) return inProgress();
      if (row.requestDigest !== digest) return json(409, { error: "CONFLICT" });
      if (row.state === "completed") return signed(row.status, Buffer.from(row.body ?? "", "base64"));
      // A lease that hasn't lapsed may belong to a handler still running: only a lapsed one is taken over.
      const taken = await store.update(eventId, { fence: row.fence, state: "in_progress", leaseBefore: now() }, (r) => ({
        owner,
        fence: r.fence + 1,
        leaseUntil: now() + leaseSeconds,
      }));
      if (!taken) return inProgress();
      fence = row.fence + 1;
    }

    const writes = {};
    const renewing = setInterval(() => {
      store.update(eventId, { fence, state: "in_progress" }, { leaseUntil: now() + leaseSeconds }).catch(() => {});
    }, renewSeconds * 1000);
    renewing.unref?.();
    let answer;
    try {
      answer = await onEvent(event, { eventId, write: (k, v) => void (writes[k] = v) });
    } catch (error) {
      // Let the connector's next retry run it again at once rather than wait out the lease.
      await store.update(eventId, { fence, state: "in_progress" }, { leaseUntil: 0 }).catch(() => {});
      throw error;
    } finally {
      clearInterval(renewing);
    }
    const payload = Buffer.from(answer === undefined || answer === null ? "" : JSON.stringify(answer));
    const committed = await store.update(eventId, { fence, state: "in_progress" }, {
      state: "completed",
      status: 200,
      body: payload.toString("base64"),
      writes,
      completedAt: now(),
    });
    // Taken over while it ran: the other handler's answer is the one kept, and this one writes nothing.
    return committed ? signed(200, payload) : inProgress();
  };
}

/**
 * A request handler for node:http (or anything with the same request and response) at EKL_CALLBACK_URL: each event
 * goes through `receiver` and its answer back to the connector. An error in `onEvent` answers 500, so the connector
 * sends the event again.
 *
 *   createServer(callbacks(onEvent)).listen(3000, "127.0.0.1")
 */
export function callbacks(onEvent, options = {}) {
  const receive = receiver(onEvent, options);
  return async (request, response) => {
    if (request.method !== "POST") {
      response.writeHead(405).end();
      return;
    }
    const chunks = [];
    let size = 0;
    for await (const chunk of request) {
      size += chunk.length;
      if (size > MAX_BODY) {
        response.writeHead(413).end();
        return;
      }
      chunks.push(Buffer.from(chunk));
    }
    try {
      const answer = await receive(Buffer.concat(chunks), request.headers);
      response.writeHead(answer.status, answer.headers).end(answer.body);
    } catch {
      response.writeHead(500).end();
    }
  };
}

// --- declarations in the server's own code ---------------------------------------------------------------------

const given = (fields) => Object.fromEntries(Object.entries(fields).filter(([, v]) => v !== undefined));

/**
 * The agent-wide declaration, for a TypeScript MCP server: what MCP's standard fields can't say (plans, data
 * handling, terms, interests, payment). Pass it as the server's experimental capabilities, and `ekl agent sync`
 * writes it into agent.json from the running server:
 *
 *   new McpServer({ name, version }, { capabilities: { experimental: eukollab({ plans: [{ type: "free", daily_limit: 200 }] }) } })
 *
 * `data` and `plans` apply to every service unless a service declares its own; giving `data` declares how the agent
 * handles data. The same JSON the Python adapter's EuKollab(...) extension advertises.
 */
export function eukollab(settings = {}) {
  const { data, plans, data_declared, ...rest } = settings;
  const defaults = given({ data, plans });
  const declared = given({
    ...rest,
    data_declared: data_declared ?? (data !== undefined ? true : undefined),
    defaults: Object.keys(defaults).length ? defaults : undefined,
  });
  return { [AGENT_DECLARATION]: declared };
}

/**
 * One tool's service declaration, for its `_meta`: its class (read_only, transformation or external_action, which
 * settles a tool whose annotations don't), category, limitations, plans, example and the rest. The same JSON as the
 * Python adapter's service(...).
 *
 *   server.registerTool("get_rate", { ..., _meta: service({ class: "read_only", category: "finance" }) }, handler)
 */
export function service(declaration = {}) {
  return { [SERVICE_DECLARATION]: given(declaration) };
}

export const AGENT_DECLARATION = "ai.eukollab/agent";
export const SERVICE_DECLARATION = "ai.eukollab/service";

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
      closeAgreement: async (handle, capabilityId, { outcome = "completed" } = {}) =>
        record("close_agreement", { handle, capabilityId, outcome }),
      decideRun: async (reservationId, decision) => record("decide_run", { reservationId, ...decision }),
    };
  },
};
