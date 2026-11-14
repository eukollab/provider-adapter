// Copyright © 2026 EuKreate, Inc.
// SPDX-License-Identifier: Apache-2.0

/** Types for @eukollab/agent. */

export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
export type JsonSchema = { [key: string]: Json };

export declare class AgentError extends Error {
  /** The network's error code, e.g. STAND_IN_MISSING, POLICY_DENIED, CONNECTOR_UNREACHABLE. */
  code: string;
  /** What to do about it. */
  fix: string;
}

export interface Result<T = Json> {
  value: T;
  executionId: string;
  /** The amount charged, as a decimal string; "0" for a stand-in. */
  charged: string;
  /** The receipt's salted fingerprints match the input sent and the answer received. */
  receiptChecked: boolean;
  /** Answered from a recorded stand-in, without the network. */
  standIn: boolean;
  /** The provider that answered: @handle, or @handle/service for a stand-in. */
  provider?: string;
  /**
   * What screening on this machine found in the result, one per category: {category, rules_version}. The content and
   * the matched words never leave the machine; the network is told the category only, bound to the call's receipt.
   */
  safetyFlags: { category: string; rules_version: string }[];
}

export interface ServiceOptions {
  /** The service `ekl find` suggested, as @handle/service; the network may still choose another. */
  prefer?: string;
  /** Your own test input for the trials. */
  sample?: { [key: string]: Json };
  /** A JSON Schema a provider's trial output must satisfy to be chosen. */
  expect?: JsonSchema;
}

/** A declared service. Its provider is chosen by the network, or answers from its stand-in until the agent is live. */
export interface PlannedService<T = Json> {
  readonly need: string;
  readonly prefer?: string;
  /**
   * Call the service. Repeating a call with the same `idempotencyKey`, for example after a crash, returns the
   * recorded outcome instead of running or paying for the work again.
   */
  call(input: { [key: string]: Json }, options?: { idempotencyKey?: string }): Promise<Result<T>>;
}

/** The agent's own actions on the network, taken by the connector on the app's behalf. */
export interface Agent {
  service<T = Json>(need: string, options?: ServiceOptions): PlannedService<T>;
  postNeed(text: string, options?: { topics?: string[]; budget?: string }): Promise<{ need_id: string }>;
  find(query: string, options?: { limit?: number }): Promise<{ candidates: { [key: string]: Json }[] }>;
  choose(query: string, options?: { sample?: { [key: string]: Json } }): Promise<{ [key: string]: Json }>;
  call<T = Json>(
    handle: string,
    capabilityId: string,
    args: { [key: string]: Json },
    options?: { idempotencyKey?: string },
  ): Promise<Result<T>>;
  message(connectionId: string, text: string): Promise<{ [key: string]: Json }>;
  /** Close the agreement for a service, as completed or cancelled: its review period opens on both sides. */
  closeAgreement(
    handle: string,
    capabilityId: string,
    options?: { outcome?: "completed" | "cancelled" },
  ): Promise<{ [key: string]: Json }>;
  /**
   * Decide a paid run of one of your services that waits for your approval (the connector's run.approval_requested
   * callback): approve it, optionally for the consumer's next `runs`; ask for payment first on your own page; or
   * decline it.
   */
  decideRun(reservationId: string, decision: RunDecision): Promise<{ [key: string]: Json }>;
}

export type RunDecision =
  | { decision: "approve"; runs?: number; overage?: boolean }
  | { decision: "payment_needed"; url: string; amount: string; description?: string }
  | { decision: "decline"; reason: string };

export interface AgentOptions {
  /** The connector's local API; default $EKL_AGENT_API_URL, else .ekl/local-api.json, else 127.0.0.1:8765. */
  apiUrl?: string;
  /** The local API's token; default $EKL_LOCAL_TOKEN, which ekl agent run writes to .env. */
  token?: string;
  /** The stand-ins' folder; default $EKL_FIXTURES, else eukollab/fixtures. */
  fixtures?: string;
  /** Whether the agent takes part; default: EKL_AGENT_CREDENTIAL is set here, or the connector says it holds it. */
  live?: boolean;
  fetch?: typeof fetch;
}

export declare function createAgent(options?: AgentOptions): Agent;

/**
 * The connector's local API, as createAgent finds it: EKL_AGENT_API_URL, else the url in .ekl/local-api.json (here, or
 * beside EKL_AGENT_FILE), else 127.0.0.1:$EKL_AGENT_API_PORT. Throws AgentError CONNECTOR_NOT_RUNNING when the file
 * names a process that is gone.
 */
export declare function localApiUrl(options?: { cwd?: string }): string;

/** The first problem a value has against a schema's basic checks, or "". */
export declare function schemaProblem(schema: JsonSchema, value: Json, where?: string): string;

export interface FakeNetwork extends Agent {
  actions: { action: string; [key: string]: unknown }[];
  standIns(): string[];
}

export declare const testing: {
  fakeNetwork(options?: { fixtures?: string }): FakeNetwork;
};

/** Agent-wide settings MCP's standard fields can't say; see the guide's agent.json reference for each field. */
export interface AgentDeclaration {
  mode?: string;
  execution_mode?: string;
  display_name?: string;
  purpose?: string;
  interests?: string[];
  languages?: string[];
  logo?: string;
  website?: string;
  /** How the agent handles data; every service's default. */
  data?: { [key: string]: Json };
  data_declared?: boolean;
  terms?: { [key: string]: Json };
  /** Every service's default plans, e.g. [{ type: "free", daily_limit: 200 }]. */
  plans?: { [key: string]: Json }[];
  /** Where consumers pay for paid services: { link: "https://…", note: "…" }. */
  payment?: { [key: string]: Json };
}

/** One tool's service declaration. */
export interface ServiceDeclaration {
  /** read_only, transformation or external_action. */
  class?: "read_only" | "transformation" | "external_action";
  offered?: boolean;
  name?: string;
  summary?: string;
  category?: string;
  limitations?: string;
  plans?: { [key: string]: Json }[];
  data?: { [key: string]: Json };
  repeat?: string;
  refreshes?: { [key: string]: number };
  storage_free_accepted?: boolean;
  outside_action?: string;
  effects?: { [key: string]: Json };
  cancellation?: string;
  max_duration_seconds?: number;
  version?: string;
  /** A working call, e.g. { input: { base: "EUR", quote: "GBP" } }. */
  example?: { [key: string]: Json };
}

export declare const AGENT_DECLARATION: "ai.eukollab/agent";
export declare const SERVICE_DECLARATION: "ai.eukollab/service";

/** For the server's capabilities: `{ capabilities: { experimental: eukollab({...}) } }`. */
export declare function eukollab(settings?: AgentDeclaration): { "ai.eukollab/agent": { [key: string]: Json } };

/** For a tool's `_meta`: `_meta: service({ class: "read_only" })`. */
export declare function service(declaration?: ServiceDeclaration): { "ai.eukollab/service": { [key: string]: Json } };

/** An event the connector posts to EKL_CALLBACK_URL: its name in `event` beside its own fields. */
export type CallbackEvent = { event: string } & { [key: string]: Json };

/** What to do about an event: {reply}, {accept: true}, {decline}, {review}, {skip: true}, a run's decision, or nothing. */
export type CallbackAnswer = { [key: string]: Json } | void | undefined;

/** What a handler can do besides answering: the event's ID, and values committed together with its answer. */
export interface CallbackContext {
  /** Key anything the handler does outside the receiver's store by it (a paid call's idempotencyKey). */
  eventId: string;
  /** Keep a value with this event's answer: stored once, with the answer, or not at all. */
  write(key: string, value: Json): void;
}

/** A durable store of received events: each kept 7 days, with its answer and the handler's writes. */
export interface CallbackStore {
  get(eventId: string): Promise<{ [key: string]: unknown } | null>;
  insert(row: { [key: string]: unknown }): Promise<boolean>;
  update(
    eventId: string,
    expect: { fence: number; state: string; leaseBefore?: number },
    changes: { [key: string]: unknown } | ((row: { [key: string]: unknown }) => { [key: string]: unknown }),
  ): Promise<boolean>;
  recall(key: string): Promise<Json | undefined>;
  purge(before: number): Promise<void>;
}

export interface ReceiverOptions {
  /** EKL_CALLBACK_KEY, hex; default the environment's, or one derived from a credential there. */
  key?: string;
  /** The agent's ID; default EKL_AGENT_ID, else the event's agent_id. */
  agentId?: string;
  /** The store; default fileStore(dir). */
  store?: CallbackStore;
  /** The default store's folder; default .ekl/callbacks. */
  dir?: string;
  leaseSeconds?: number;
  renewSeconds?: number;
  now?: () => number;
}

/** The default store: a file per event under .ekl/callbacks (0700/0600); atomic inserts, serialised updates. */
export declare function fileStore(dir?: string): CallbackStore;

/** k_cb as raw bytes: EKL_CALLBACK_KEY, else derived from EKL_AGENT_CREDENTIAL or EKL_PROVIDER_CREDENTIAL. */
export declare function callbackKey(options?: { key?: string; credential?: string }): Uint8Array;

/** The X-EKL-Signature-2 value the connector sends for a body, for tests and other senders. */
export declare function signRequest(key: Uint8Array, t: number, eventId: string, agentId: string, body: string | Uint8Array): string;

/** The X-EKL-Response-Signature-2 value for an answer to the request whose body digest is requestDigest. */
export declare function signResponse(
  key: Uint8Array, t: number, eventId: string, agentId: string, requestDigest: string, status: number, body: string | Uint8Array,
): string;

/**
 * Receive one callback in any framework: the raw body and headers in, the response out. The signature is checked
 * before anything is looked up; a retry gets the stored answer; the handler runs once, under a fenced lease. "Once"
 * covers the answer and ctx.write: key anything else by ctx.eventId.
 */
export declare function receiver(
  onEvent: (event: CallbackEvent, ctx: CallbackContext) => CallbackAnswer | Promise<CallbackAnswer>,
  options?: ReceiverOptions,
): (body: string | Uint8Array, headers: { [key: string]: string | string[] | undefined }) => Promise<{
  status: number;
  headers: { [key: string]: string };
  body: Uint8Array;
}>;

/**
 * A node:http request handler for the connector's signed callbacks (protocol v2):
 * `createServer(callbacks(onEvent)).listen(3000, "127.0.0.1")`. A wrong or old signature gets 401, a repeated event its
 * stored answer, one still being handled 409 IN_PROGRESS; an error in onEvent gets 500, so the connector sends the
 * event again until the decision's deadline.
 */
export declare function callbacks(
  onEvent: (event: CallbackEvent, ctx: CallbackContext) => CallbackAnswer | Promise<CallbackAnswer>,
  options?: ReceiverOptions,
): (request: AsyncIterable<Uint8Array> & { method?: string; headers: { [key: string]: string | string[] | undefined } },
    response: { writeHead(status: number, headers?: { [key: string]: string }): { end(body?: string | Uint8Array): unknown } }) => Promise<void>;
