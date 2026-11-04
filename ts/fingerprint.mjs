// Copyright © 2026 EuKreate, Inc.
// SPDX-License-Identifier: Apache-2.0
// RFC 8785 canonical JSON (ekl-json-1) and the salted call fingerprints of the adapter profile (EKP §15.3), with no dependencies.
// Every implementation must reproduce the shared vectors byte for byte, so this follows RFC 8785 exactly:
// ECMAScript number serialisation (which JSON.stringify already uses), minimal string escaping, and object keys
// ordered by UTF-16 code units (JavaScript's default string comparison).
import { createHash, randomBytes } from "node:crypto";

const SALT_BYTES = 32;

export function canonical(value) {
  if (value === null || typeof value === "boolean") return JSON.stringify(value);
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new Error("NaN and Infinity have no JSON form");
    // Integers beyond 2^53 are refused where JSON is parsed: once parsed, a number cannot say it was one.
    return JSON.stringify(value); // -0 becomes "0", as RFC 8785 requires
  }
  if (typeof value === "string") return JSON.stringify(value);
  if (Array.isArray(value)) return "[" + value.map(canonical).join(",") + "]";
  if (typeof value === "object") {
    const keys = Object.keys(value).sort(); // default sort compares UTF-16 code units
    return "{" + keys.map((k) => JSON.stringify(k) + ":" + canonical(value[k])).join(",") + "}";
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
