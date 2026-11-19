import assert from "node:assert/strict";
import { test } from "node:test";
import { makeRedactor } from "./probe-prod.ts";

test("redactor removes secrets and JWT-shaped strings", () => {
  const redact = makeRedactor(["secret-bearer-123", "binding-token-xyz"]);
  const out = redact('{"a":"secret-bearer-123","b":"x binding-token-xyz","c":"eyJabcdefghij.klm.nop"}');
  assert.equal(out.includes("secret-bearer"), false);
  assert.equal(out.includes("binding-token"), false);
  assert.equal(out.includes("eyJ"), false);
});
