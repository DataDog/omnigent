"""Tests for lifecycle status in child-session summaries."""

from __future__ import annotations

import pytest

from omnigent._wrapper_labels import ACP_SUBAGENT_ID_LABEL_KEY
from omnigent.entities import Conversation
from omnigent.server.routes._sessions import helpers
from omnigent.server.routes._sessions.helpers import _child_session_summary_from_conversation
from omnigent.server.schemas import AcpSessionObservation


def _child(
    *, labels: dict[str, str], live_status: str | None, kind: str = "sub_agent"
) -> Conversation:
    """Build a child row with the supplied lifecycle state."""
    return Conversation(
        id="conv_status_child",
        created_at=100,
        updated_at=200,
        kind=kind,
        root_conversation_id="conv_parent",
        title="tool:child-task",
        agent_id="ag_test",
        labels=labels,
        live_status=live_status,
    )


@pytest.mark.parametrize("live_status", ["running", "idle", "failed"])
def test_acp_child_summary_does_not_project_persisted_status_without_observation(
    monkeypatch: pytest.MonkeyPatch,
    live_status: str,
) -> None:
    """Persisted compatibility status cannot establish current ACP activity."""
    child = _child(
        labels={ACP_SUBAGENT_ID_LABEL_KEY: "native-subagent-1"},
        live_status=live_status,
    )
    monkeypatch.delitem(helpers._session_status_cache, child.id, raising=False)

    summary = _child_session_summary_from_conversation(child, "conv_parent", None)

    assert summary.current_task_status is None
    assert summary.busy is False


def test_acp_child_summary_ignores_cached_and_edge_status_without_observation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cached status edge cannot replace missing structured ACP evidence."""
    child = _child(
        labels={ACP_SUBAGENT_ID_LABEL_KEY: "native-subagent-1"},
        live_status="idle",
    )
    monkeypatch.setitem(helpers._session_status_cache, child.id, "running")

    cached_summary = _child_session_summary_from_conversation(child, "conv_parent", None)
    edge_summary = _child_session_summary_from_conversation(
        child, "conv_parent", None, cached_status="failed"
    )

    assert cached_summary.current_task_status is None
    assert cached_summary.busy is False
    assert edge_summary.current_task_status is None
    assert edge_summary.busy is False


@pytest.mark.parametrize(
    ("observation_data", "expected_status", "expected_busy"),
    [
        (
            {
                "source_current": True,
                "state": {"present": True, "value": {"state": "running"}},
                "transport": {"state": "connected"},
            },
            "in_progress",
            True,
        ),
        (
            {
                "source_current": True,
                "state": {
                    "present": True,
                    "value": {"state": "idle", "stopReason": "unknown"},
                },
                "transport": {"state": "connected"},
            },
            None,
            False,
        ),
        (
            {
                "source_current": True,
                "state": {"present": True, "value": {"state": "requires_action"}},
                "transport": {"state": "connected"},
            },
            "in_progress",
            True,
        ),
        (
            {
                "source_current": True,
                "state": {"present": False},
                "transport": {"state": "connected"},
            },
            None,
            False,
        ),
        (
            {
                "source_current": True,
                "state": {"present": True, "value": None},
                "transport": {"state": "connected"},
            },
            None,
            False,
        ),
        (
            {
                "source_current": True,
                "state": {"present": True, "value": {"state": "future_state"}},
                "transport": {"state": "connected"},
            },
            None,
            False,
        ),
        (
            {
                "source_current": False,
                "state": {"present": True, "value": {"state": "running"}},
                "transport": {"state": "connected"},
            },
            None,
            False,
        ),
        (
            {
                "source_current": True,
                "state": {"present": True, "value": {"state": "running"}},
                "transport": {"state": "disconnected", "reason": "process exited"},
            },
            None,
            False,
        ),
        (
            {
                # Older or sparse observations fail closed when source eligibility is absent.
                "state": {"present": True, "value": {"state": "running"}},
            },
            None,
            False,
        ),
    ],
)
def test_acp_child_summary_projects_only_current_confirmed_activity(
    monkeypatch: pytest.MonkeyPatch,
    observation_data: dict[str, object],
    expected_status: str | None,
    expected_busy: bool,
) -> None:
    """The raw observation is retained while summary status fails closed."""
    child = _child(
        labels={ACP_SUBAGENT_ID_LABEL_KEY: "native-subagent-1"},
        # A stale live value must not override an unknown, historical, or idle observation.
        live_status="running",
    )
    monkeypatch.setitem(helpers._session_status_cache, child.id, "running")
    observation = AcpSessionObservation.model_validate(
        {**observation_data, "last_ingress_sequence": 7}
    )

    summary = _child_session_summary_from_conversation(
        child,
        "conv_parent",
        None,
        acp_session_observation=observation,
    )

    assert summary.current_task_status == expected_status
    assert summary.busy is expected_busy
    assert summary.acp_session_observation == observation


def test_acp_child_idle_then_running_reuses_child_identity() -> None:
    """Idle reports no task outcome, and the same mirror can become active again."""
    child = _child(
        labels={ACP_SUBAGENT_ID_LABEL_KEY: "native-subagent-1"},
        live_status="idle",
    )
    idle_observation = AcpSessionObservation.model_validate(
        {
            "source_current": True,
            "state": {
                "present": True,
                "value": {"state": "idle", "stopReason": "completed"},
            },
            "last_ingress_sequence": 8,
        }
    )
    running_observation = AcpSessionObservation.model_validate(
        {
            "source_current": True,
            "state": {"present": True, "value": {"state": "running"}},
            "last_ingress_sequence": 9,
        }
    )

    idle = _child_session_summary_from_conversation(
        child, "conv_parent", None, acp_session_observation=idle_observation
    )
    running = _child_session_summary_from_conversation(
        child, "conv_parent", None, acp_session_observation=running_observation
    )

    assert idle.id == running.id == child.id
    assert idle.busy is False
    assert idle.current_task_status is None
    assert running.busy is True
    assert running.current_task_status == "in_progress"


@pytest.mark.parametrize(
    ("kind", "labels"),
    [
        ("sub_agent", {}),
        ("default", {ACP_SUBAGENT_ID_LABEL_KEY: "native-subagent-1"}),
    ],
)
def test_non_acp_child_summary_does_not_use_persisted_status(
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
    labels: dict[str, str],
) -> None:
    """Persisted-status fallback stays limited to ACP mirror children."""
    child = _child(labels=labels, live_status="running", kind=kind)
    monkeypatch.delitem(helpers._session_status_cache, child.id, raising=False)

    summary = _child_session_summary_from_conversation(child, "conv_parent", None)

    assert summary.current_task_status is None
    assert summary.busy is False
