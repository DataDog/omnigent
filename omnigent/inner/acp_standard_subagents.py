"""ACP v1 subagent-preview frame decoding and trusted session identities.

The standard preview is negotiated independently of vendor extensions. This
module only recognizes and normalizes ACP frames; the executor supplies the
trusted native epoch, root conversation, context generation, and association
registry before persisting or routing them.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

REPORT_PROTOCOL = "acp-session-reports"
REPORT_PROTOCOL_VERSION = 3
ACTION_PROTOCOL = "acp-session-actions"
ACTION_PROTOCOL_VERSION = 2

# Private runner transport metadata. This key is never read from native ACP
# `_meta`; the executor creates it after validating a root tool call.
ACP_TOOL_CALL_SCOPE_METADATA_KEY = "_omnigent_acp_tool_call_scope"
ACP_TOOL_OUTPUT_METADATA_KEY = "_omnigent_acp_tool_output"

STANDARD_SUBAGENT_OPERATIONS = frozenset(
    {
        "association.patch",
        "observation.patch",
        "entity.upsert",
        "entity.patch",
        "entity.append",
        "tool.references",
    }
)

REPORT_FEATURES = frozenset(
    {
        "standard-acp-subagents-v1",
        "session-entities-v1",
        "session-owned-actions-v2",
        "root-source-epochs-v1",
    }
)
ACTION_FEATURES = frozenset({"standard-acp-child-requests-v1"})

_ASSOCIATION_FIELDS = {
    "title": "title",
    "description": "description",
    "capabilities": "capabilities",
    "state": "state",
    "cwd": "cwd",
    "additionalDirectories": "additionalDirectories",
    "_meta": "metadata",
}
_MESSAGE_FIELDS = {
    "senderSessionId": "sender_session_id",
    "recipientSessionId": "recipient_session_id",
    "senderMessageId": "sender_message_id",
    "recipientMessageId": "recipient_message_id",
    "content": "content",
    "_meta": "metadata",
}

MAX_ACP_NATIVE_ID_UTF8_BYTES = 512
MAX_ACP_NATIVE_EPOCH_UTF8_BYTES = 512


@dataclass(frozen=True)
class AcpSessionReportIdentity:
    """Trusted identity scope attached to a normalized standard ACP report."""

    server_identity: str
    root_conversation_id: str
    native_epoch: str
    context_generation: str
    root_native_session_id: str
    target_session_id: str
    parent_session_id: str | None


@dataclass(frozen=True)
class AcpSessionOperation:
    """One frame operation before trusted executor scope is attached."""

    target_session_id: str
    parent_session_id: str | None
    kind: str
    operation: Mapping[str, Any]


@dataclass(frozen=True)
class AcpSessionReport:
    """One replayable normalized operation sourced from a native ACP frame."""

    identity: AcpSessionReportIdentity
    ingress_sequence: int
    source_id: str
    kind: str
    operation: Mapping[str, Any]


@dataclass(frozen=True)
class AcpSessionReportRead:
    """Frame ownership and normalized report, or a fail-closed decode error."""

    recognized: bool
    operation: AcpSessionOperation | None = None
    error: str | None = None
    target_session_id: str | None = None
    parent_session_id: str | None = None


@dataclass(frozen=True)
class AcpSessionActionIdentity:
    """Untrusted protocol identity for an ACP request attributed to a session."""

    target_session_id: str
    request_id: str | int
    method: str


@dataclass(frozen=True)
class AcpSessionActionBinding:
    """Trusted immutable origin and effective filesystem overrides.

    The path and directory values are resolved through the validated native
    ancestry before asynchronous handling. Present fields are the immutable
    effective snapshot; an empty directory tuple is an explicit empty set.
    """

    root_conversation_id: str
    native_epoch: str
    context_generation: str
    root_native_session_id: str
    target_session_id: str
    parent_session_id: str
    association_source_id: str
    origin_token: str
    origin_agent: str | None
    origin_generation: str
    request_id: str | int
    method: str
    request_invocation_id: int = 1
    cwd_present: bool = False
    cwd: str | None = None
    additional_directories_present: bool = False
    additional_directories: tuple[str, ...] | None = None

    def __post_init__(self) -> None:
        if (
            isinstance(self.request_id, bool)
            or not isinstance(self.request_id, (str, int))
            or (isinstance(self.request_id, str) and not self.request_id)
            or len(str(self.request_id)) > 512
        ):
            raise ValueError("ACP child request id is invalid")
        if (
            isinstance(self.request_invocation_id, bool)
            or not isinstance(self.request_invocation_id, int)
            or not 1 <= self.request_invocation_id <= 2**63 - 1
        ):
            raise ValueError("ACP child request invocation id is invalid")


@dataclass(frozen=True)
class AcpSessionActionRead:
    """Whether a server-initiated JSON-RPC request names a session owner."""

    recognized: bool
    identity: AcpSessionActionIdentity | None = None
    error: str | None = None


def acp_standard_report_source_id(
    server_identity: str,
    root_conversation_id: str,
    native_epoch: str,
    ingress_sequence: int,
) -> str:
    """Build a bounded, unambiguous id for a native ingress operation."""
    if isinstance(ingress_sequence, bool) or ingress_sequence < 1:
        raise ValueError("ACP ingress sequence must be a positive integer")
    components = (
        server_identity,
        root_conversation_id,
        native_epoch,
        str(ingress_sequence),
    )
    if any(not isinstance(part, str) or not part for part in components):
        raise ValueError("ACP report source components must be non-empty strings")
    encoded = [part.encode("utf-8") for part in components]
    payload = b"".join(len(part).to_bytes(4, "big") + part for part in encoded)
    return "acp2:" + hashlib.sha256(payload).hexdigest()


def acp_standard_child_key(
    parent_session_id: str,
    native_epoch: str,
    native_session_id: str,
) -> str:
    """Return the stable Omnigent key for one reusable ACP session child."""
    components = (parent_session_id, native_epoch, native_session_id, "session")
    if any(not isinstance(part, str) or not part for part in components):
        raise ValueError("ACP child-key components must be non-empty strings")
    encoded = [part.encode("utf-8") for part in components]
    payload = b"".join(len(part).to_bytes(4, "big") + part for part in encoded)
    return "acp_" + hashlib.sha256(payload).hexdigest()


def acp_standard_tool_entity_id(native_tool_call_id: str) -> str:
    """Map an opaque native tool ID into its disjoint internal entity domain."""
    validate_acp_native_identity("toolCallId", native_tool_call_id)
    return "acp2t_" + hashlib.sha256(native_tool_call_id.encode("utf-8")).hexdigest()


def acp_standard_tool_output_entity_id(native_tool_call_id: str) -> str:
    """Map an opaque native tool ID into its disjoint output entity domain."""
    validate_acp_native_identity("toolCallId", native_tool_call_id)
    return "acp2o_" + hashlib.sha256(native_tool_call_id.encode("utf-8")).hexdigest()


def acp_standard_root_tool_reference_kind(
    kind: str,
    data: Mapping[str, Any],
) -> str | None:
    """Classify one exact root tool identity anchor or explicit references update."""
    operation = data.get("operation")
    if (
        kind != "tool.references"
        or data.get("target_session_id") != data.get("root_native_session_id")
        or data.get("parent_session_id") is not None
        or not isinstance(operation, Mapping)
    ):
        return None
    call_id = operation.get("call_id")
    if not isinstance(call_id, str) or not call_id or len(call_id) > MAX_ACP_NATIVE_ID_UTF8_BYTES:
        return None
    try:
        if len(call_id.encode("utf-8")) > MAX_ACP_NATIVE_ID_UTF8_BYTES:
            return None
        expected_entity_id = acp_standard_tool_entity_id(call_id)
    except (TypeError, ValueError, UnicodeError):
        return None
    if operation.get("entity_id") != expected_entity_id:
        return None
    if set(operation) == {"entity_id", "call_id", "response_id"}:
        response_id = operation.get("response_id")
        if isinstance(response_id, str) and response_id and len(response_id) <= 64:
            return "identity_only"
    if set(operation) == {"entity_id", "call_id", "references"}:
        references = operation.get("references")
        if isinstance(references, list) and len(references) <= 128:
            return "references"
    return None


def acp_standard_canonical_tool_item_id(
    root_conversation_id: str,
    source_id: str,
    item_type: str,
) -> str:
    """Build the canonical root item ID for one anchored ACP tool role."""
    if (
        not isinstance(root_conversation_id, str)
        or not root_conversation_id
        or len(root_conversation_id) > 512
    ):
        raise ValueError("ACP canonical tool item root identity is invalid")
    if (
        not isinstance(source_id, str)
        or len(source_id) != 69
        or not source_id.startswith("acp2:")
        or any(char not in "0123456789abcdef" for char in source_id[5:])
    ):
        raise ValueError("ACP canonical tool item anchor is invalid")
    if not isinstance(item_type, str) or item_type not in {
        "function_call",
        "function_call_output",
    }:
        raise ValueError("ACP canonical tool item role is invalid")
    components = (
        "acp-standard-canonical-tool-item-v1",
        root_conversation_id,
        source_id,
        item_type,
    )
    encoded = [component.encode("utf-8") for component in components]
    payload = b"".join(len(component).to_bytes(4, "big") + component for component in encoded)
    return hashlib.sha256(payload).hexdigest()[:32]


def read_standard_subagent_frame(params: Mapping[str, Any]) -> AcpSessionReportRead:
    """Claim and decode one standard preview update from ``session/update``.

    The enclosing ACP session ID is the immediate owner for an association
    announcement, and the target child for message upserts/chunks. A malformed
    recognized discriminator is returned as claimed with an error so it cannot
    fall through to ordinary root event translation.
    """
    update = params.get("update")
    if not isinstance(update, Mapping):
        return AcpSessionReportRead(recognized=False)
    discriminator = update.get("sessionUpdate")
    if not isinstance(discriminator, str) or discriminator not in {
        "subagent_update",
        "session_message",
        "session_message_chunk",
    }:
        return AcpSessionReportRead(recognized=False)

    outer_session_id = params.get("sessionId")
    if (
        not isinstance(outer_session_id, str)
        or not outer_session_id
        or len(outer_session_id) > 512
    ):
        return AcpSessionReportRead(
            recognized=True,
            error="standard ACP subagent update has no enclosing sessionId",
        )

    try:
        validate_acp_native_identity("epoch", outer_session_id)
    except ValueError as exc:
        return AcpSessionReportRead(recognized=True, error=str(exc))

    if discriminator == "subagent_update":
        child_session_id = update.get("sessionId")
        if (
            not isinstance(child_session_id, str)
            or not child_session_id
            or len(child_session_id) > 512
        ):
            return AcpSessionReportRead(
                recognized=True,
                error="standard ACP subagent association has no child sessionId",
            )
        try:
            validate_acp_native_identity("epoch", child_session_id)
        except ValueError as exc:
            return AcpSessionReportRead(
                recognized=True,
                error=str(exc),
                target_session_id=child_session_id,
                parent_session_id=outer_session_id,
            )
        patch: dict[str, Any] = {}
        for wire_name, field_name in _ASSOCIATION_FIELDS.items():
            if wire_name in update:
                patch[field_name] = update[wire_name]
        protocol_extensions = _unknown_fields(
            update, {"sessionUpdate", "sessionId", *_ASSOCIATION_FIELDS}
        )
        if protocol_extensions:
            patch["protocol_extensions"] = protocol_extensions
        try:
            validate_acp_association_patch(patch)
        except ValueError as exc:
            return AcpSessionReportRead(
                recognized=True,
                error=str(exc),
                target_session_id=child_session_id,
                parent_session_id=outer_session_id,
            )
        return AcpSessionReportRead(
            recognized=True,
            operation=AcpSessionOperation(
                target_session_id=child_session_id,
                parent_session_id=outer_session_id,
                kind="association.patch",
                operation={
                    "session_id": child_session_id,
                    "parent_session_id": outer_session_id,
                    "patch": patch,
                },
            ),
            target_session_id=child_session_id,
            parent_session_id=outer_session_id,
        )

    message_id = update.get("messageId")
    if not isinstance(message_id, str) or not message_id or len(message_id) > 512:
        return AcpSessionReportRead(
            recognized=True,
            error="standard ACP session message has no messageId",
            target_session_id=outer_session_id,
            parent_session_id=None,
        )
    try:
        validate_acp_native_identity("epoch", outer_session_id, message_id)
    except ValueError as exc:
        return AcpSessionReportRead(
            recognized=True,
            error=str(exc),
            target_session_id=outer_session_id,
            parent_session_id=None,
        )

    if discriminator == "session_message":
        patch: dict[str, Any] = {
            "role": "assistant",
            "acp_agent_identity": "unknown",
        }
        patch.update(
            {
                normalized: update[wire]
                for wire, normalized in _MESSAGE_FIELDS.items()
                if wire in update
            }
        )
        directed_fields = {
            key: patch.pop(key)
            for key in (
                "sender_session_id",
                "recipient_session_id",
                "sender_message_id",
                "recipient_message_id",
            )
            if key in patch
        }
        patch["acp_session_message"] = {"kind": "directed", **directed_fields}
        protocol_extensions = _unknown_fields(
            update, {"sessionUpdate", "messageId", *_MESSAGE_FIELDS}
        )
        if protocol_extensions:
            patch["protocol_extensions"] = protocol_extensions
        error = _validate_message_patch(patch)
        if error is not None:
            return AcpSessionReportRead(
                recognized=True,
                error=error,
                target_session_id=outer_session_id,
            )
        return AcpSessionReportRead(
            recognized=True,
            operation=AcpSessionOperation(
                target_session_id=outer_session_id,
                parent_session_id=None,
                kind="entity.patch",
                operation={
                    "entity_type": "message",
                    "entity_id": message_id,
                    "patch": patch,
                },
            ),
            target_session_id=outer_session_id,
        )

    content = update.get("content")
    if not isinstance(content, Mapping):
        return AcpSessionReportRead(
            recognized=True,
            error="standard ACP session message chunk has invalid content",
            target_session_id=outer_session_id,
        )
    chunk_metadata: dict[str, Any] = {}
    if "_meta" in update:
        chunk_metadata["_meta"] = update["_meta"]
    protocol_extensions = _unknown_fields(
        update, {"sessionUpdate", "messageId", "content", "_meta"}
    )
    if protocol_extensions:
        chunk_metadata["protocol_extensions"] = protocol_extensions
    return AcpSessionReportRead(
        recognized=True,
        operation=AcpSessionOperation(
            target_session_id=outer_session_id,
            parent_session_id=None,
            kind="entity.append",
            operation={
                "entity_type": "message",
                "entity_id": message_id,
                "content": [dict(content)],
                **({"chunk_metadata": chunk_metadata} if chunk_metadata else {}),
            },
        ),
        target_session_id=outer_session_id,
    )


def validate_acp_association_patch(patch: Mapping[str, Any]) -> None:
    """Validate the normalized association patch shared by all report consumers."""
    if not isinstance(patch, Mapping):
        raise ValueError("standard ACP association patch must be an object")
    for field_name in ("title", "description"):
        value = patch.get(field_name)
        if field_name in patch and value is not None and not isinstance(value, str):
            raise ValueError(f"standard ACP association {field_name} must be a string or null")
    for field_name in ("capabilities", "state", "metadata"):
        value = patch.get(field_name)
        if field_name in patch and value is not None and not isinstance(value, Mapping):
            raise ValueError(f"standard ACP association {field_name} must be an object or null")
    cwd = patch.get("cwd")
    if "cwd" in patch and cwd is not None:
        if not isinstance(cwd, str) or not cwd or not Path(cwd).is_absolute():
            raise ValueError("standard ACP association cwd must be an absolute path or null")
    directories = patch.get("additionalDirectories")
    if "additionalDirectories" in patch and directories is not None:
        if not isinstance(directories, list) or any(
            not isinstance(path, str) or not path or not Path(path).is_absolute()
            for path in directories
        ):
            raise ValueError(
                "standard ACP association additionalDirectories must contain absolute paths"
            )


def validate_acp_native_identity(
    native_epoch: str,
    *native_session_ids: str,
) -> None:
    """Keep composite ACP database keys below PostgreSQL's btree tuple limit."""
    if not isinstance(native_epoch, str) or not native_epoch:
        raise ValueError("ACP native epoch must be a non-empty string")
    if (
        len(native_epoch) > 128
        or len(native_epoch.encode("utf-8")) > MAX_ACP_NATIVE_EPOCH_UTF8_BYTES
    ):
        raise ValueError("ACP native epoch exceeds its storage limit")
    for native_session_id in native_session_ids:
        if (
            not isinstance(native_session_id, str)
            or not native_session_id
            or len(native_session_id) > 512
            or len(native_session_id.encode("utf-8")) > MAX_ACP_NATIVE_ID_UTF8_BYTES
        ):
            raise ValueError("ACP native session id exceeds its storage limit")


def _validate_message_patch(patch: dict[str, Any]) -> str | None:
    for field_name in (
        "sender_session_id",
        "recipient_session_id",
        "sender_message_id",
        "recipient_message_id",
    ):
        value = patch.get(field_name)
        if field_name in patch and value is not None and not isinstance(value, str):
            return f"standard ACP session message {field_name} must be a string or null"
        if field_name in patch and isinstance(value, str) and len(value) > 512:
            return f"standard ACP session message {field_name} exceeds its size limit"
    if "content" in patch:
        content = patch["content"]
        if content is not None and not isinstance(content, list):
            return "standard ACP session message content must be an array or null"
        if isinstance(content, list) and any(not isinstance(block, Mapping) for block in content):
            return "standard ACP session message content contains an invalid block"
        if isinstance(content, list):
            patch["content"] = [dict(block) for block in content]
    if "metadata" in patch and patch["metadata"] is not None:
        if not isinstance(patch["metadata"], Mapping):
            return "standard ACP session message metadata must be an object or null"
        patch["metadata"] = dict(patch["metadata"])
    directed = patch.get("acp_session_message")
    if isinstance(directed, Mapping):
        for field_name in (
            "sender_session_id",
            "recipient_session_id",
            "sender_message_id",
            "recipient_message_id",
        ):
            value = directed.get(field_name)
            if field_name in directed and value is not None and not isinstance(value, str):
                return f"standard ACP session message {field_name} must be a string or null"
            if field_name in directed and isinstance(value, str) and len(value) > 512:
                return f"standard ACP session message {field_name} exceeds its size limit"
        patch["acp_session_message"] = dict(directed)
    return None


def _unknown_fields(update: Mapping[str, Any], known_fields: set[str]) -> dict[str, Any]:
    """Retain future optional fields as opaque data with no policy authority."""
    return {
        key: value
        for key, value in update.items()
        if key not in known_fields and isinstance(key, str)
    }


def read_standard_session_action(request: Mapping[str, Any]) -> AcpSessionActionRead:
    """Decode the ACP session owner and RPC identity from a client request."""
    method = request.get("method")
    if not isinstance(method, str) or not method:
        return AcpSessionActionRead(recognized=False)
    params = request.get("params")
    if not isinstance(params, Mapping):
        return AcpSessionActionRead(recognized=False)
    if "sessionId" not in params:
        return AcpSessionActionRead(recognized=False)
    target_session_id = params.get("sessionId")
    if not isinstance(target_session_id, str) or not target_session_id:
        return AcpSessionActionRead(
            recognized=True,
            error="ACP session-owned request has an invalid sessionId",
        )
    if len(target_session_id) > 512:
        return AcpSessionActionRead(
            recognized=True,
            error="ACP session-owned request sessionId exceeds its size limit",
        )
    try:
        validate_acp_native_identity("epoch", target_session_id)
    except ValueError as exc:
        return AcpSessionActionRead(recognized=True, error=str(exc))
    request_id = request.get("id")
    if isinstance(request_id, bool) or not isinstance(request_id, (str, int)):
        return AcpSessionActionRead(
            recognized=True,
            error="ACP session-owned request has an invalid JSON-RPC id",
        )
    if (isinstance(request_id, str) and not request_id) or len(str(request_id)) > 512:
        return AcpSessionActionRead(
            recognized=True,
            error="ACP session-owned request id is invalid",
        )
    return AcpSessionActionRead(
        recognized=True,
        identity=AcpSessionActionIdentity(
            target_session_id=target_session_id,
            request_id=request_id,
            method=method,
        ),
    )


def read_session_tool_references(
    update: Mapping[str, Any],
) -> tuple[str, ...] | None:
    """Return session IDs carried by standard tool content, or ``None`` if omitted."""
    if "content" not in update:
        return None
    content = update["content"]
    if content is None:
        return ()
    if not isinstance(content, list):
        raise ValueError("ACP tool content must be an array or null")
    references: list[str] = []
    for block in content:
        if not isinstance(block, Mapping) or block.get("type") != "session":
            continue
        session_id = block.get("sessionId")
        if not isinstance(session_id, str) or not session_id:
            raise ValueError("ACP session tool reference has no sessionId")
        validate_acp_native_identity("epoch", session_id)
        references.append(session_id)
    return tuple(references)
