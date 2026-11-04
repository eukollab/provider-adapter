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
  /** The connector's local API; default http://127.0.0.1:$EKL_AGENT_API_PORT (8765). */
  apiUrl?: string;
  /** The stand-ins' folder; default $EKL_FIXTURES, else eukollab/fixtures. */
  fixtures?: string;
  /** Whether the agent takes part; default: EKL_AGENT_CREDENTIAL is set here, or the connector says it holds it. */
  live?: boolean;
  fetch?: typeof fetch;
}

export declare function createAgent(options?: AgentOptions): Agent;

/**
 * The event a signed callback carries: its name in `event` beside the event's own fields (need, answered,
 * connection_request, message, choice_approved, decision, review_due). Throws AgentError when the signature doesn't
 * match. Answer the request with what to do: {reply}, {accept: true}, {decline}, {review}, {skip: true} or nothing.
 */
export declare function handle(
  body: string | Uint8Array,
  signature: string | undefined | null,
  secret?: string,
): { event: string } & { [key: string]: Json };

/** The first problem a value has against a schema's basic checks, or "". */
export declare function schemaProblem(schema: JsonSchema, value: Json, where?: string): string;

export interface FakeNetwork extends Agent {
  actions: { action: string; [key: string]: unknown }[];
  standIns(): string[];
}

export declare const testing: {
  fakeNetwork(options?: { fixtures?: string }): FakeNetwork;
};
