import assert from "node:assert/strict";
import { test } from "node:test";
import { decodeFrame, encodeFrame, type RequestFrame } from "./frames.ts";
import { tokenBoundRunnerId } from "./tunnel.ts";

test("tokenBoundRunnerId matches omnigent.runner.identity.token_bound_runner_id", () => {
  // Vector from: python -c "from omnigent.runner.identity import token_bound_runner_id as f; print(f('spike-test-vector'))"
  assert.equal(tokenBoundRunnerId("spike-test-vector"), "runner_token_b7eb1951fea4edc0b87354cb94b9fb95");
  assert.equal(tokenBoundRunnerId("  spike-test-vector\n"), "runner_token_b7eb1951fea4edc0b87354cb94b9fb95");
  assert.throws(() => tokenBoundRunnerId("   "));
});

test("request frame round-trips and fills Python defaults", () => {
  const wire = JSON.stringify({ kind: "request", id: "r1", method: "POST", path: "/v1/sessions", body: "{}" });
  const decoded = decodeFrame(wire) as RequestFrame;
  assert.deepEqual(
    { qs: decoded.query_string, headers: decoded.headers, enc: decoded.encoding, stream: decoded.stream },
    { qs: "", headers: [], enc: "utf-8", stream: false },
  );
  assert.deepEqual(decodeFrame(encodeFrame(decoded)), decoded);
  assert.throws(() => decodeFrame(JSON.stringify({ kind: "bogus" })));
});
