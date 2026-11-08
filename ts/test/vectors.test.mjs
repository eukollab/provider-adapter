// Copyright © 2026 EuKreate, Inc.
// SPDX-License-Identifier: Apache-2.0
// The TypeScript implementation reproduces the shared fingerprint vectors exactly.
import { test } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { canonical, fingerprint, newSalt, resultBody } from "../fingerprint.mjs";

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

test("a salt is 32 bytes", () => {
  assert.equal(newSalt().length, 64);
  assert.throws(() => fingerprint("00", {}));
});
