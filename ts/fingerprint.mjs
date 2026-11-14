// Copyright © 2026 EuKreate, Inc.
// SPDX-License-Identifier: Apache-2.0
// RFC 8785 canonical JSON (ekl-json-1) and the salted call fingerprints of the adapter profile (EKP §15.3), with no dependencies.
// Every implementation must reproduce the shared vectors byte for byte, so this follows RFC 8785 exactly:
// ECMAScript number serialisation (which JSON.stringify already uses), minimal string escaping, and object keys
// ordered by UTF-16 code units (JavaScript's default string comparison).
import { createHash, randomBytes } from "node:crypto";

const SALT_BYTES = 32;
// EKP 1.2 §2.5: every array or object adds a level; deeper than this is refused before anything hashes it.
export const MAX_DEPTH = 64;

/** Input nested deeper than MAX_DEPTH: refused with INPUT_TOO_DEEP, and the receiver keeps serving. */
export class InputTooDeepError extends Error {
  constructor() {
    super(`nested deeper than ${MAX_DEPTH}`);
    this.name = "InputTooDeepError";
    this.code = "INPUT_TOO_DEEP";
  }
}

// RFC 8785 makes invalid Unicode, such as a lone surrogate, an error.
function string(value) {
  if (!value.isWellFormed()) throw new Error("a lone surrogate is not valid Unicode");
  return JSON.stringify(value);
}

export function canonical(value, depth = 0) {
  if (value !== null && typeof value === "object" && depth >= MAX_DEPTH) throw new InputTooDeepError();
  if (value === null || typeof value === "boolean") return JSON.stringify(value);
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new Error("NaN and Infinity have no JSON form");
    // Integers beyond 2^53 are refused where JSON is parsed: once parsed, a number cannot say it was one.
    return JSON.stringify(value); // -0 becomes "0", as RFC 8785 requires
  }
  if (typeof value === "string") return string(value);
  if (Array.isArray(value)) return "[" + value.map((v) => canonical(v, depth + 1)).join(",") + "]";
  if (typeof value === "object") {
    const keys = Object.keys(value).sort(); // default sort compares UTF-16 code units
    return "{" + keys.map((k) => string(k) + ":" + canonical(value[k], depth + 1)).join(",") + "}";
  }
  throw new Error(`${typeof value} is not a JSON value`);
}

export function fingerprint(saltHex, value) {
  const salt = Buffer.from(saltHex, "hex");
  // Buffer.from stops quietly at the first character that is not hex, so the text's length is checked as well.
  if (salt.length !== SALT_BYTES || saltHex.length !== SALT_BYTES * 2) throw new Error("the salt is 32 bytes, hex");
  return createHash("sha256").update(salt).update(Buffer.from(canonical(value), "utf8")).digest("hex");
}

// What the output fingerprint covers: structuredContent when present, otherwise the content array without each
// block's _meta. isError and the result's own _meta are never hashed.
export function resultBody(result) {
  if (result.structuredContent !== undefined && result.structuredContent !== null) return result.structuredContent;
  return (result.content ?? []).map((block) => {
    const { _meta, ...rest } = block;
    return rest;
  });
}

export function newSalt() {
  return randomBytes(SALT_BYTES).toString("hex");
}

const UPSTREAM_FIELDS = ["name", "description", "inputSchema", "outputSchema", "annotations"];

// EKP 1.2 §5.1: the tools the server lists that the agent declares, sorted by name (UTF-16 code units), each projected
// to its five interface fields with absent ones null, hashed without a salt.
export function upstreamFingerprint(toolsList, declared) {
  const names = new Set(declared);
  const projected = toolsList
    .filter((t) => names.has(t.name))
    .sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0))
    .map((t) => Object.fromEntries(UPSTREAM_FIELDS.map((f) => [f, t[f] ?? null])));
  return createHash("sha256").update(canonical(projected), "utf8").digest("hex");
}
