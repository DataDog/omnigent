"""E2E: a generic ACP agent's per-turn usage reaches ``GET /v1/sessions/{id}``.

Drives the real path with a hermetic fake ACP agent (``tests/_helpers/acp_agent``)
whose ``session/prompt`` result carries vit-shaped camelCase ``usage`` and no
model: acp_executor -> executor adapter -> ``response.completed`` -> server
usage accumulation -> session snapshot. Without a model the turn is neither
attributed (``usage_by_model``) nor priced (``total_cost_usd``), so the flat
``token_usage`` totals are the only read path for those tokens (VIT-2027).
No browser is needed: the assertion is on the HTTP API.
"""

from __future__ import annotations

import time

import httpx

from tests._helpers.acp_agent import FAKE_ACP_AGENT_REPLY, FAKE_ACP_AGENT_USAGE_SNAKE
from tests.e2e_ui.files.test_files_tab_survives_acp_reply import (  # noqa: F401  # pytest fixture
    acp_launcher_session,
)


def test_acp_model_less_usage_surfaces_as_session_token_usage(
    acp_launcher_session: tuple[str, str],  # noqa: F811
) -> None:
    """A model-less ACP turn's usage reads back as ``token_usage``."""
    base_url, session_id = acp_launcher_session
    httpx.post(
        f"{base_url}/v1/sessions/{session_id}/events",
        json={
            "type": "message",
            "data": {"role": "user", "content": [{"type": "input_text", "text": "hi"}]},
        },
        timeout=10.0,
    ).raise_for_status()

    snapshot: dict[str, object] = {}
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        snapshot = httpx.get(f"{base_url}/v1/sessions/{session_id}", timeout=10.0).json()
        replied = FAKE_ACP_AGENT_REPLY in str(snapshot.get("items"))
        if replied and snapshot.get("token_usage"):
            break
        time.sleep(0.5)

    assert snapshot["harness"] == "acp"
    assert snapshot["usage_by_model"] is None
    assert snapshot["total_cost_usd"] is None
    assert snapshot["token_usage"] == {**FAKE_ACP_AGENT_USAGE_SNAKE, "total_cost_usd": None}
