"""Hermetic stdio ACP agent for tests that launch a real external agent process."""

from __future__ import annotations

import json

FAKE_ACP_AGENT_REPLY = "ACP agent reply: hello from fake-agent"
FAKE_ACP_AGENT_USAGE_SNAKE = {
    "input_tokens": 1200,
    "output_tokens": 340,
    "total_tokens": 1540,
    "cache_read_input_tokens": 900,
    "cache_creation_input_tokens": 50,
}

# Speaks the Agent Client Protocol over stdio without a permission round-trip:
# initialize -> session/new -> session/prompt streams one agent_message_chunk and
# completes the turn with vit-shaped camelCase ``result.usage`` and no model
# (FAKE_ACP_AGENT_USAGE_SNAKE is what Omnigent records for it). Stdlib only,
# so any interpreter on the host can run it.
FAKE_ACP_AGENT_SCRIPT = r"""
import sys, json

def send(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    msg = json.loads(line)
    mid, method = msg.get("id"), msg.get("method")
    if method == "initialize":
        send({"jsonrpc": "2.0", "id": mid, "result": {
            "protocolVersion": 1,
            "agentCapabilities": {"promptCapabilities": {"image": False}},
        }})
    elif method == "session/new":
        send({"jsonrpc": "2.0", "id": mid, "result": {"sessionId": "fake-acp-session-1"}})
    elif method == "session/prompt":
        sid = msg["params"]["sessionId"]
        send({"jsonrpc": "2.0", "method": "session/update",
              "params": {"sessionId": sid, "update": {
                  "sessionUpdate": "agent_message_chunk",
                  "content": {"type": "text", "text": "__REPLY__"}}}})
        send({"jsonrpc": "2.0", "id": mid, "result": {
            "stopReason": "end_turn",
            "usage": {"inputTokens": 1200, "outputTokens": 340, "cachedReadTokens": 900,
                      "cachedWriteTokens": 50, "totalTokens": 1540},
        }})
""".replace('"__REPLY__"', json.dumps(FAKE_ACP_AGENT_REPLY))

if "__REPLY__" in FAKE_ACP_AGENT_SCRIPT:
    raise ValueError("reply placeholder was not substituted")
