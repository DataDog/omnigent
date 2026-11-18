"""Compatibility lifecycle for non-standard ACP subagent dialects.

ACP's v1 subagents preview is decoded by :mod:`acp_standard_subagents` and
persists reusable sessions and transcript entities. The older
:class:`SubAgentStart` / :class:`SubAgentEnd` lifecycle remains only as a
separate compatibility seam for any existing vendor adapter; its terminal
events are not used by standard ACP subagent reporting.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable


def acp_child_key(
    parent_session_id: str,
    native_epoch: str,
    native_session_id: str,
    run_id: str,
) -> str:
    """Return the stable key encoding used by the retired run-report reader."""
    parts = (parent_session_id, native_epoch, native_session_id, run_id)
    if any(not isinstance(part, str) or not part for part in parts):
        raise ValueError("ACP child-key components must be non-empty strings")
    encoded_parts = [part.encode("utf-8") for part in parts]
    payload = b"".join(len(part).to_bytes(4, "big") + part for part in encoded_parts)
    return "acp_" + hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True)
class SubAgentStart:
    """A sub-agent began. Normalized output of an :class:`AcpSubAgentSource`.

    :param child_key: Stable id for the sub-agent, unique within the parent
        turn. Correlates a later :class:`SubAgentEnd` and is the idempotency key
        when the child session is minted.
    :param title: Short human label for the row, e.g. ``"mathutils"``.
    :param task: The instruction the sub-agent was given, shown on the row.
    """

    child_key: str
    title: str
    task: str = ""


@dataclass(frozen=True)
class SubAgentEnd:
    """A previously-started sub-agent finished.

    :param child_key: Matches the originating :attr:`SubAgentStart.child_key`.
    :param ok: Whether the sub-agent reported success.
    :param summary: The sub-agent's closing summary, shown in the child chat.
    """

    child_key: str
    ok: bool = True
    summary: str = ""


@dataclass(frozen=True)
class SubAgentActivity:
    """A tool call a sub-agent ran, to route into its child transcript.

    Distinct from the parent's own tool calls: this is a call the sub-agent made
    *inside* its delegated work, which belongs in the child session's chat rather
    than the parent stream. A source emits one only for frames it can attribute to
    a specific sub-agent (Devin tags them with the owning ``parentAgentId``).

    :param child_key: The owning sub-agent — matches the originating
        :attr:`SubAgentStart.child_key`, so the runner can address the child.
    :param call_id: The tool call's id, used as the child item's ``call_id``.
    :param name: Human tool label for the card, e.g. ``"Wrote mathutils.py"``.
    :param args: The tool's raw input, rendered as the call's arguments.
    """

    child_key: str
    call_id: str
    name: str
    args: Mapping[str, Any] = field(default_factory=dict)


SubAgentEvent = SubAgentStart | SubAgentEnd | SubAgentActivity


@runtime_checkable
class AcpSubAgentSource(Protocol):
    """Maps one agent's sub-agent dialect onto the normalized lifecycle.

    Pure and stateless: given a single ACP ``session/update`` payload (the
    ``params.update`` object, which carries ``sessionUpdate`` and any ``_meta``),
    return the sub-agent lifecycle events it carries — almost always none.

    An implementation MUST self-gate on its own dialect's markers rather than
    assume it is only handed its own vendor's traffic. Two reasons: a source is
    the only thing that knows its dialect, and self-gating means a source stays
    correct if an agent's fork or a future multi-dialect wrap sends it frames it
    does not recognize.
    """

    def read(self, update: Mapping[str, Any]) -> Sequence[SubAgentEvent]: ...


def read_subagent_events(
    update: Mapping[str, Any],
    sources: Sequence[AcpSubAgentSource],
) -> list[SubAgentEvent]:
    """Run every source over one ``session/update``; return all events found.

    :param update: The ACP ``params.update`` object.
    :param sources: Dialects to try, from the executor's
        :class:`~omnigent.inner.acp_extension.AcpExtension`. Empty (the generic
        ACP harness) short-circuits to no events.
    :returns: All sub-agent lifecycle events the sources recognized (usually
        empty).
    """
    out: list[SubAgentEvent] = []
    for source in sources:
        out.extend(source.read(update))
    return out
