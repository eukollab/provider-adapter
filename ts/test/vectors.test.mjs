// Copyright © 2026 EuKreate, Inc.
// SPDX-License-Identifier: Apache-2.0
// The TypeScript implementation reproduces the shared fingerprint vectors exactly.
import { test } from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { InputTooDeepError, canonical, fingerprint, newSalt, resultBody, upstreamFingerprint } from "../fingerprint.mjs";

const here = (p) => fileURLToPath(new URL(p, import.meta.url));
// The adapter's vectors sit in tests/ of the public repository and in provider-adapter/tests/ beside this module.
const files = [here("../../tests/vectors.json"), here("../../provider-adapter/tests/vectors.json")].filter(existsSync);
// The protocol's vectors: a checkout of eukollab/ekp named by EKP_DIR, else the contracts beside this module.
const contracts = process.env.EKP_DIR ?? here("../../contracts");
const published = join(contracts, "vectors", "fingerprints.json");

for (const file of files) {
  const vectors = JSON.parse(readFileSync(file, "utf8"));
  for (const v of vectors.values ?? []) {
    test(`${file.split("/").slice(-2).join("/")}: ${v.name}`, () => {
      assert.equal(canonical(v.value), v.jcs);
      assert.equal(fingerprint(vectors.salt, v.value), v.fingerprint);
    });
  }
  for (const v of vectors.results ?? []) {
    test(`${file.split("/").slice(-2).join("/")}: result ${v.name}`, () => {
      assert.equal(fingerprint(vectors.salt, resultBody(v.result)), v.fingerprint);
    });
  }
}

test("the adapter's vectors are found", () => assert.ok(files.length));

test("the network's published fingerprint vectors", () => {
  const vectors = JSON.parse(readFileSync(published, "utf8"));
  for (const c of vectors.cases) {
    assert.equal(canonical(c.value), c.jcs, c.name);
    assert.equal(fingerprint(c.salt, c.value), c.fingerprint, c.name);
  }
  const s = vectors.result_selection;
  assert.equal(fingerprint(vectors.cases[0].salt, resultBody(s.result)), s.fingerprint);
});

test("ekl-json-1 canonical vectors", () => {
  const ekl = JSON.parse(readFileSync(join(contracts, "vectors", "ekl-json-1.json"), "utf8"));
  for (const v of ekl.valid) assert.equal(canonical(JSON.parse(v.input)), v.canonical, v.name);
  // JSON.parse resolves the other invalid inputs (duplicate keys, large integers) before canonical sees them; a lone
  // surrogate survives parsing, so canonical itself must refuse it.
  const lone = ekl.invalid.find((v) => v.name === "lone surrogate");
  assert.throws(() => canonical(JSON.parse(lone.input)), /lone surrogate/);
});

// EKP 1.2: the upstream fingerprint an adapter reports with its presence. Absent from a checkout of EKP 1.1.
const upstream = join(contracts, "vectors", "upstream-fingerprint.json");
test("upstream fingerprint vectors", { skip: !existsSync(upstream) }, () => {
  const fields = ["name", "description", "inputSchema", "outputSchema", "annotations"];
  for (const c of JSON.parse(readFileSync(upstream, "utf8")).cases) {
    // String comparison in JavaScript is by UTF-16 code units, as the definition asks.
    const chosen = c.tools_list.filter((t) => c.declared.includes(t.name)).sort((a, b) => (a.name < b.name ? -1 : 1));
    const projected = chosen.map((t) => Object.fromEntries(fields.map((f) => [f, t[f] ?? null])));
    assert.equal(canonical(projected), c.canonical, c.name);
    assert.equal(createHash("sha256").update(canonical(projected), "utf8").digest("hex"), c.upstream_fingerprint, c.name);
    assert.equal(upstreamFingerprint(c.tools_list, c.declared), c.upstream_fingerprint, c.name);
  }
});

test("input nested deeper than 64 is refused with INPUT_TOO_DEEP", () => {
  const ekl = JSON.parse(readFileSync(join(contracts, "vectors", "ekl-json-1.json"), "utf8"));
  if (!ekl.depth) return;
  for (const v of ekl.depth.accepted) canonical(JSON.parse(v.input));
  for (const v of ekl.depth.refused) {
    assert.throws(() => canonical(JSON.parse(v.input)), (e) => e instanceof InputTooDeepError && e.code === "INPUT_TOO_DEEP");
  }
});

test("a salt is 32 bytes", () => {
  assert.equal(newSalt().length, 64);
  assert.throws(() => fingerprint("00", {}));
});
