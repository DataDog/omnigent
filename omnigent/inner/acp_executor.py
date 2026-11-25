"""AcpExecutor: drive *any* agent that speaks the Agent Client Protocol (ACP).

ACP (agentclientprotocol.com) is an open, editor-agnostic protocol: a JSON-RPC
2.0 conversation over newline-delimited JSON on a subprocess's stdin/stdout. Its
whole premise is that the *client* need not know which agent it drives — Goose
(``goose acp``), Qwen Code (``qwen --acp``), Gemini CLI
(``gemini --experimental-acp``), Zed's Claude Code bridge
(``@zed-industries/claude-code-acp``) and any in-house agent all speak the same
wire.

This executor is the **generic** counterpart to the vendor-specific
:class:`~omnigent.inner.goose_executor.GooseExecutor` /
:class:`~omnigent.inner.qwen_executor.QwenExecutor`: it spawns whatever command a
user configured (:class:`AcpAgentConfig.command`) and speaks ACP against it. The
handful of things those two hardcode become config knobs here:

* ``command``            — the argv to launch (``shlex``-split; never a shell).
* ``session_id_mode``    — ``"server"`` (agent assigns the id, Goose-style) or
                           ``"client"`` (we generate it, Qwen-style).
* ``send_model_in_session_new`` / ``model`` — send a non-standard ``model`` field
                           in ``session/new`` (Qwen accepts it; most agents take
                           the model from their own config / the command's flags).

Protocol flow (identical for every ACP agent):
  1. ``initialize``     — handshake; learn ``agentCapabilities`` (image support).
  2. ``session/new``    — create/adopt a session id + ``cwd`` + ``mcpServers``.
  3. ``session/prompt`` — send a user turn; consume streaming ``session/update``
     notifications (``agent_message_chunk``, ``agent_thought_chunk``,
     ``tool_call`` / ``tool_call_update``), answer any server-initiated
     ``session/request_permission`` / ``fs/*`` requests, then read the final
     response (``stopReason`` + optional ``usage``).
  4. Re-use the same session id for later turns (the agent retains context).

The agent runs its own agent loop, tool execution, context window and compaction
internally. This executor translates the ACP event stream into Omnigent
:class:`ExecutorEvent`s and routes the agent's permission requests through
Omnigent's TOOL_CALL policy + human-consent elicitation.

Vs. the Goose executor this generalizes, it additionally: renders the agent's
tool calls as Omnigent tool cards (``tool_call`` → ``ToolCallRequest``,
``tool_call_update`` → ``ToolCallComplete``), forwards reasoning
(``agent_thought_chunk`` → ``ReasoningChunk``), and honors interrupts via the ACP
``session/cancel`` notification.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import math
import os
import secrets
import shlex
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from pathlib import Path
from typing import Any, Protocol, TypeAlias

from omnigent.debug_logging import debug_event
from omnigent.inner import _proc
from omnigent.inner._acp_metadata import _META_INPUT_ITEM_ID
from omnigent.inner._acp_omnigent_mcp import OmnigentAcpMcp
from omnigent.inner.acp_extension import NO_ACP_EXTENSION, AcpExtension
from omnigent.inner.acp_standard_subagents import (
    ACP_TOOL_CALL_SCOPE_METADATA_KEY,
    ACP_TOOL_OUTPUT_METADATA_KEY,
    ACTION_FEATURES,
    REPORT_FEATURES,
    AcpSessionActionBinding,
    AcpSessionActionRead,
    AcpSessionOperation,
    AcpSessionReportIdentity,
    acp_standard_report_source_id,
    acp_standard_tool_entity_id,
    acp_standard_tool_output_entity_id,
    read_session_tool_references,
    read_standard_session_action,
    read_standard_subagent_frame,
)
from omnigent.inner.acp_subagents import (
    SubAgentActivity,
    SubAgentStart,
    read_subagent_events,
)
from omnigent.inner.agent_env import clean_agent_env, declared_passthrough
from omnigent.inner.datamodel import OSEnvSandboxSpec, OSEnvSpec
from omnigent.inner.executor import (
    Executor,
    ExecutorConfig,
    ExecutorError,
    ExecutorEvent,
    Message,
    ReasoningChunk,
    SessionReport,
    SessionReportSink,
    SessionReportStatus,
    SubAgentCompleted,
    SubAgentStarted,
    SubAgentToolCall,
    TextChunk,
    ToolCallComplete,
    ToolCallRequest,
    ToolCallStatus,
    ToolSpec,
    TurnComplete,
    describe_exception,
)
from omnigent.inner.os_env import OSEnvironment, create_os_environment
from omnigent.inner.sandbox import reachable_roots
from omnigent.process_logging import current_process_log_path, display_log_path
from omnigent.runner.identity import strip_runner_auth_secrets

logger = logging.getLogger(__name__)

ACP_CANCEL_GRACE_SECONDS = 1.0
_ACP_ACTION_ORIGIN_CHECK_PHASE = "ACP_ACTION_ORIGIN_CHECK"
_ACP_TERMINATION_TIMEOUT_SECONDS = 5.0
_ACP_CLEANUP_HEADROOM_SECONDS = 1.0
ACP_REAP_TIMEOUT_SECONDS = (
    ACP_CANCEL_GRACE_SECONDS + _ACP_TERMINATION_TIMEOUT_SECONDS + _ACP_CLEANUP_HEADROOM_SECONDS
)

# ACP's JSON-RPC schema is agent-owned and extensible; consumers narrow fields
# before use.
_AcpJsonObject: TypeAlias = dict[str, Any]  # type: ignore[explicit-any]


class _PolicyVerdict(Protocol):
    action: str


_PolicyEvaluator: TypeAlias = Callable[[str, _AcpJsonObject], Awaitable[_PolicyVerdict]]
_ElicitationHandler: TypeAlias = Callable[[str, _AcpJsonObject], Awaitable[bool]]
# Choice-aware elicitation: offers the agent's own permission options and returns
# the chosen label (``None`` = declined). Optional; falls back to the yes/no form.
_ElicitationChoiceHandler: TypeAlias = Callable[
    [str, _AcpJsonObject, Sequence[str]], Awaitable[str | None]
]
_ToolExecutor: TypeAlias = Callable[[str, _AcpJsonObject], Awaitable[_AcpJsonObject]]

# ACP error code an agent maps to a filesystem "not found" (ENOENT) when a
# delegated ``fs/read_text_file`` misses — the reference ACP client lib special-
# cases exactly this code. Any other code surfaces raw.
_ACP_RESOURCE_NOT_FOUND_CODE = -32002

# ACP protocol constants (JSON-RPC 2.0 method names).
_AGENT_METHOD_INITIALIZE = "initialize"
_AGENT_METHOD_AUTHENTICATE = "authenticate"
_AGENT_METHOD_SESSION_NEW = "session/new"
_AGENT_METHOD_SESSION_PROMPT = "session/prompt"
# Browser/device-login method ids that cannot complete on a headless sandbox.
_ACP_INTERACTIVE_AUTH_METHODS = frozenset({"grok.com"})
# ACP error code an agent returns when a request requires ``authenticate``
# first (Grok Build's ``session/new`` uses it for "Authentication required").
_ACP_AUTH_REQUIRED_CODE = -32000

# Notification sent *from* the agent to the client (streaming progress).
_CLIENT_NOTIFICATION_SESSION_UPDATE = "session/update"
# Notification sent *from* the client to the agent to abort the current turn.
_CLIENT_NOTIFICATION_SESSION_CANCEL = "session/cancel"

# Server-initiated request methods (agent → client).
_AGENT_REQUEST_REQUEST_PERMISSION = "session/request_permission"

# session/update.update.sessionUpdate discriminator values we map.
_UPDATE_AGENT_MESSAGE_CHUNK = "agent_message_chunk"
_UPDATE_AGENT_THOUGHT_CHUNK = "agent_thought_chunk"
_UPDATE_TOOL_CALL = "tool_call"
_UPDATE_TOOL_CALL_UPDATE = "tool_call_update"
_UPDATE_USAGE = "usage_update"
_UPDATE_CONFIG_OPTION = "config_option_update"

# ACP ``session/set_config_option`` — the standard warm-switch method. The
# option id for the model, and the param name the agent expects (``configId``,
# not ``optionId``).
_AGENT_METHOD_SET_CONFIG_OPTION = "session/set_config_option"
_CONFIG_OPTION_MODEL = "model"

# ACP ``session/set_model`` — the model-selection API an agent advertises by
# returning a ``models`` object from ``session/new`` (``availableModels`` plus
# ``currentModelId``) instead of exposing a ``model`` config option. Cline speaks
# this shape and never sends ``config_option_update``, so an agent selected this
# way is unreachable through ``session/set_config_option`` alone.
_AGENT_METHOD_SET_MODEL = "session/set_model"

# ACP tool-call lifecycle statuses (the terminal ones close a tool card).
_TOOL_STATUS_COMPLETED = "completed"
_TOOL_STATUS_FAILED = "failed"

# Idle (time-without-progress) timeout for a prompt turn, in seconds.
# Some ACP agents stay silent while an external interaction is pending, so
# this is configurable. Parsing is import-time and fail-loud: a malformed,
# non-positive, or non-finite value aborts the ACP child at startup.
_PROMPT_TIMEOUT_ENV = "HARNESS_ACP_PROMPT_TIMEOUT_S"
_PROMPT_TIMEOUT_ERR = f"{_PROMPT_TIMEOUT_ENV} must be a positive finite number of seconds"
try:
    _PROMPT_TIMEOUT_SECONDS = float(os.environ.get(_PROMPT_TIMEOUT_ENV, "300"))
except ValueError as exc:
    raise ValueError(_PROMPT_TIMEOUT_ERR) from exc
if not math.isfinite(_PROMPT_TIMEOUT_SECONDS) or _PROMPT_TIMEOUT_SECONDS <= 0:
    raise ValueError(_PROMPT_TIMEOUT_ERR)

# Idle timeout for the initial ACP handshake (initialize / session setup).
_INIT_TIMEOUT_SECONDS = 30.0

# Agent stderr kept for diagnostics: how many trailing lines to retain, how many
# to quote in a turn error, and the per-line cap (a chatty CLI can emit one
# enormous line, and the error goes in a UI toast).
_STDERR_RING_LINES = 20
_STDERR_QUOTED_LINES = 5
_STDERR_LINE_LIMIT = 500
_STDERR_QUOTED_LIMIT = 1000

# ACP protocol version this executor targets (matches Goose 1.38 / Qwen).
_PROTOCOL_VERSION = 1
_MAX_STANDARD_SESSIONS = 50_000
_MAX_STANDARD_REPORTS = 50_000
_MAX_ROOT_TOOL_CALL_SCOPES = _MAX_STANDARD_REPORTS
_MAX_DETACHED_ACP_REQUESTS = 128


@dataclass
class _StandardSessionAssociation:
    parent_session_id: str | None
    association_source_id: str
    origin: tuple[str, str | None, str, str] | None
    cwd_present: bool = False
    cwd: str | None = None
    additional_directories_present: bool = False
    additional_directories: tuple[str, ...] | None = None


@dataclass
class _SessionTranslationContext:
    tool_names: dict[str, str] = dataclass_field(default_factory=dict)
    tool_inputs: dict[str, _AcpJsonObject] = dataclass_field(default_factory=dict)
    response_ids: dict[tuple[str, str], str] = dataclass_field(default_factory=dict)
    active_message_id: str | None = None


@dataclass(frozen=True)
class AcpAgentConfig:
    """Identity of the ACP agent this executor drives.

    :param command: The command to launch, e.g. ``"gemini --experimental-acp"``.
        Split with :func:`shlex.split` into an argv and exec'd directly (never
        via a shell), so quoting works but ``$VAR`` / pipes / redirects do not.
    :param name: Human label for logs / elicitation cards (e.g. ``"Gemini CLI"``).
    :param model: Optional model id, applied to the live session when a turn
        carries no per-turn pick — via ``session/set_model`` for agents that
        advertise a model catalog in ``session/new``, else via the ``model``
        session config option. With :attr:`send_model_in_session_new` it is
        also sent in ``session/new`` itself. Unset means the agent keeps the
        model from its own config or from flags in ``command``.
    :param session_id_mode: ``"server"`` — the agent assigns the session id and
        we adopt it (Goose); ``"client"`` — we generate the id and send it
        (Qwen). Defaults to ``"server"``, the ACP-idiomatic shape.
    :param send_model_in_session_new: Send a non-standard ``model`` field in
        ``session/new``. Off by default because a strict agent may reject unknown
        params; enable per-agent for Qwen-shaped agents that honor it.
    :param omnigent_mcp: Expose Omnigent's builtin tools to the agent via
        ``session/new.mcpServers`` (the shared ``serve-mcp`` relay). On by
        default; the global ``OMNIGENT_ACP_MCP=0`` kill switch also disables it.
    :param env_passthrough: Environment variable *names* this agent may read at
        spawn, e.g. ``("XAI_API_KEY",)``. The spawn env is deny-by-default and
        this executor drives an arbitrary agent, so it cannot infer the family
        the agent authenticates with — an agent that reads a variable must name
        it here (or in ``os_env.sandbox.env_passthrough``) or it starts
        unauthenticated. Names only; values come from the host environment.
    :param permission_mode: Omnigent permission stance, e.g. ``"auto"``
        (default) or ``"bypassPermissions"``. Only the latter changes anything:
        it skips the human approval card for a request no policy had an opinion
        on, matching claude-sdk's ``can_use_tool`` gate. Policy still runs in
        every mode, so a DENY still blocks and an explicit ASK still prompts.
    :param inject_system_prompt: Fold the Omnigent system prompt into the first
        ACP turn (ACP has no dedicated system-prompt field). On by default. Set
        to ``False`` for agents like Pi forks (``omp``) that fully own their own
        system prompt: those agents prepend Omnigent's text to the user message,
        which can confuse their internal Claude model into emitting XML tool-call
        fragments (``</function></tool_call>``) when there is no MCP relay
        backing the described tools. Disabling injection leaves the first turn as
        plain user content and lets the agent's own system prompt take effect
        unmodified. When ``omnigent_mcp`` is ``False`` and the agent manages its
        own context, setting this to ``False`` is strongly recommended.
    :param available_models: Curated model ids from the agent's explicitly bound
        provider (``HARNESS_ACP_MODEL_LIST``). Empty (the default) means nothing was
        curated: every model the agent accepts is allowed. When set, warm
        ``session/set_config_option`` switches to ids outside the list are
        withheld — the ACP counterpart of pi-native's ``enabledModels`` scoping
        (the vendor CLI owns its own picker, so the gate sits at the switch).
    :param env_unset: Environment variable *names* stripped from the spawn env
        handed to the vendor CLI (``HARNESS_ACP_ENV_UNSET``). Operator-declared:
        some CLIs activate built-in providers on the mere presence of a
        credential variable (any value), e.g. dummy tokens other harnesses
        project — scrubbing them keeps the CLI's own picker from bypassing the
        curated set. Applied on top of the deny-by-default allowlist, so a name
        can be both passed through and later removed deliberately. Empty (the
        default) means no scrubbing.
    :param default_model: Model to restore when a per-turn override is cleared.
        ``None`` preserves the launch-model fallback; an empty string uses the
        model originally reported by the ACP session.
    """

    command: str
    name: str = "ACP agent"
    model: str | None = None
    session_id_mode: str = "server"
    send_model_in_session_new: bool = False
    omnigent_mcp: bool = True
    env_passthrough: tuple[str, ...] = ()
    permission_mode: str = "auto"
    inject_system_prompt: bool = True
    available_models: tuple[str, ...] = ()
    env_unset: tuple[str, ...] = ()
    default_model: str | None = None


class _AcpModelSwitchError(RuntimeError):
    """A rejected model selection, before the ACP prompt is sent."""


class _AcpRequestError(Exception):
    """A handler failure to return as a JSON-RPC error on a server request.

    Carries the JSON-RPC ``code`` / ``message`` so the dispatch in
    :meth:`AcpExecutor._respond_to_agent_request` can build the error reply
    without each handler assembling the wire envelope itself.
    """

    def __init__(self, code: int, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def _looks_like_missing_file(message: str) -> bool:
    """Heuristic: does an os_env error message indicate a missing path?

    The os_env helper returns failures as ``{"error": "<str>"}`` rather than
    typed exceptions, so the message text is the only signal that a read missed
    because the file is absent. Used to map onto the ENOENT code so the model
    sees "file not found".
    """
    lowered = message.lower()
    return (
        "no such file" in lowered
        or "errno 2" in lowered
        or "not found" in lowered
        or "does not exist" in lowered
    )


def _inline_text_file_data(file_data: object) -> str:
    """Decode a text ``input_file`` ``file_data`` data URI into inline text.

    ``input_file`` blocks may carry a ``data:<mime>;base64,<payload>`` URI. Text
    files are decoded so the model sees their content; binary files (PDF, images)
    can't be inlined and return ``""``. A bare, non-data-URI string is treated as
    already-inline text.
    """
    if not isinstance(file_data, str) or not file_data:
        return ""
    if not file_data.startswith("data:"):
        return file_data
    try:
        import base64

        meta, b64 = file_data.split(",", 1)
        mime = meta.split(";")[0].replace("data:", "")
        if not mime.startswith("text/"):
            return ""
        return base64.b64decode(b64).decode("utf-8", errors="replace")
    except Exception:  # noqa: BLE001 — best-effort; never break a turn on a bad URI
        return ""


def _parse_image_data_uri(data_uri: object) -> tuple[str, str] | None:
    """Split an ``image/*`` ``data:`` URI into ``(mime_type, base64_payload)``.

    Returns ``None`` for anything that isn't an inline ``image/*`` data URI
    (external URLs are never fetched — SSRF).
    """
    if not isinstance(data_uri, str) or not data_uri.startswith("data:"):
        return None
    try:
        meta, payload = data_uri.split(",", 1)
    except ValueError:
        return None
    mime = meta.split(";")[0].replace("data:", "")
    if not mime.startswith("image/") or not payload:
        return None
    return mime, payload


def _is_auth_required_error(error: object) -> bool:
    """True when a JSON-RPC error object says authentication is required.

    Matches the ACP auth-required error code and, as a fallback for agents
    that use a generic code, an "authentication required" message.
    """
    if not isinstance(error, dict):
        return False
    if error.get("code") == _ACP_AUTH_REQUIRED_CODE:
        return True
    message = error.get("message")
    return isinstance(message, str) and "authentication required" in message.lower()


def _unattended_auth_method_id(initialize_result: _AcpJsonObject) -> str | None:
    """Pick a non-browser ACP auth method, or ``None`` if none is advertised.

    Prefers ``_meta.defaultAuthMethodId`` (Grok sets this to ``cached_token``
    when ``auth.json`` loaded) and otherwise the first method that is not a
    known interactive login id. Entries without a string ``id`` are skipped.
    """
    methods = initialize_result.get("authMethods")
    if not isinstance(methods, list) or not methods:
        return None
    ids = [
        method.get("id")
        for method in methods
        if isinstance(method, dict) and isinstance(method.get("id"), str)
    ]
    meta = initialize_result.get("_meta")
    default = meta.get("defaultAuthMethodId") if isinstance(meta, dict) else None
    if (
        isinstance(default, str)
        and default in ids
        and default not in _ACP_INTERACTIVE_AUTH_METHODS
    ):
        return default
    for method_id in ids:
        if method_id not in _ACP_INTERACTIVE_AUTH_METHODS:
            return method_id
    return None


def _resolve_cwd(cwd: str | None, *, session_id: str | None = None) -> str:
    """Resolve and validate the working directory for the ACP agent subprocess.

    Called from :meth:`AcpExecutor.__init__` (where *cwd* may be ``None``) and
    from :meth:`AcpExecutor._start_process` (where *cwd* is always a string, to
    re-validate before every spawn and respawn). Raises a clear
    :class:`RuntimeError` so the failure goes through the normal startup-error
    path and surfaces with the harness-log suffix.

    On Linux, reads ``/proc/self/cwd`` to recover a deleted inherited path
    (shown as ``<path> (deleted)``).

    :param cwd: Caller-supplied directory, or ``None`` to inherit from the process.
    :param session_id: Session id for the structured WARNING, when available.
    :returns: The resolved working directory path.
    :raises RuntimeError: When the directory is missing or the process cwd was deleted.
    """
    if cwd is not None:
        if not os.path.isdir(cwd):
            logger.warning(
                "acp executor cwd does not exist: %s",
                cwd,
                extra=debug_event(
                    "acp_executor_cwd_missing",
                    session_id=session_id,
                    cwd=cwd,
                    reason="not_a_directory",
                ),
            )
            raise RuntimeError(f"ACP executor working directory does not exist: {cwd!r}")
        return cwd

    try:
        return os.getcwd()
    except FileNotFoundError:
        deleted_path: str | None = None
        with contextlib.suppress(OSError):
            deleted_path = os.readlink("/proc/self/cwd").removesuffix(" (deleted)")
        path_detail = f" ({deleted_path})" if deleted_path else ""
        logger.warning(
            "acp executor inherited working directory was deleted%s",
            path_detail,
            extra=debug_event(
                "acp_executor_cwd_missing",
                session_id=session_id,
                cwd=deleted_path,
                reason="inherited_cwd_deleted",
            ),
        )
        raise RuntimeError(
            f"Runner working directory was deleted{path_detail}; "
            "restart the runner in a valid directory or pass an explicit cwd"
        ) from None


class AcpExecutor(Executor):
    """Executor that drives any ACP agent over JSON-RPC 2.0 on stdio."""

    def __init__(
        self,
        config: AcpAgentConfig,
        cwd: str | None = None,
        os_env: OSEnvSpec | None = None,
        *,
        extension: AcpExtension = NO_ACP_EXTENSION,
    ) -> None:
        """Initialize the generic ACP executor.

        :param config: The agent to drive (command + protocol knobs).
        :param cwd: Working directory for the agent subprocess. ``None`` inherits
            the caller's cwd.
        :param os_env: Environment / sandbox spec. When its ``sandbox`` is not
            ``"none"``, the whole agent process tree is wrapped in the platform
            sandbox (bwrap/seatbelt) at spawn — see :meth:`_sandbox_launch_path`.
        :param extension: Vendor behavior for the agent being driven, injected by
            that vendor's harness wrap (e.g.
            a vendor wrap). The default is protocol-only,
            so the generic ``acp`` harness reads no vendor field.
        """
        self._config = config
        self._extension = extension
        self._cwd = _resolve_cwd(cwd)
        self._os_env = os_env
        # Advertise ``clientCapabilities.fs`` so the agent delegates file
        # reads/writes back to us (executed through the Omnigent OSEnvironment,
        # which enforces the spec's sandbox read/write roots). Enabled only when
        # an os_env is configured and it isn't a ``fork`` env — a forked env
        # operates on a *copied* tree whose path diverges from the agent's cwd.
        self._fs_delegation: bool = os_env is not None and not bool(getattr(os_env, "fork", False))
        self._os_environment: OSEnvironment | None = None

        # Session config options the agent advertises (``mode``, ``model``, …)
        # and the live model value, both learned from ``config_option_update``.
        self._config_option_ids: set[str] = set()
        self._active_model: str | None = None
        self._model_config_option_id: str | None = None
        self._initial_model: str | None = None
        # Latches off once an agent proves it can't warm-switch, so we don't
        # retry a failing request on every turn.
        self._model_switch_supported: bool = True

        # Parsed argv; the first token is the binary we resolve / sandbox.
        self._argv: list[str] = shlex.split(config.command)
        if not self._argv:
            raise ValueError("AcpAgentConfig.command is empty")

        self._proc: asyncio.subprocess.Process | None = None
        self._queue: asyncio.Queue[_AcpJsonObject] = asyncio.Queue()
        self._reader_task: asyncio.Task[None] | None = None
        self._stderr_task: asyncio.Task[None] | None = None
        # Last few agent stderr lines, attached to a startup failure. An agent
        # that dies or stalls during the handshake usually explains why on
        # stderr ("no API key", "unknown flag"), and that text is the whole
        # diagnosis; without it the turn error names only the stalled RPC.
        self._recent_stderr: deque[str] = deque(maxlen=_STDERR_RING_LINES)
        # Serializes stdin writes: run_turn (prompt / request replies) and the
        # adapter's interrupt_session() write from different tasks.
        self._write_lock = asyncio.Lock()

        self._rpc_id: int = 0
        self._pending: dict[int, asyncio.Future[_AcpJsonObject]] = {}
        self._active_prompt_request_id: int | None = None
        self._active_prompt_response_received = False
        self._cancel_grace_deadline: float | None = None

        self._session_id: str | None = None
        self._native_epoch: str | None = None
        self._session_report_sink: SessionReportSink | None = None
        self._standard_subagents_enabled = False
        self._session_reports_negotiated = False
        self._report_prompt_started = False
        self._standard_associations: dict[str, _StandardSessionAssociation] = {}
        self._session_translation_contexts: dict[str, _SessionTranslationContext] = {}
        # Native root tool call ID -> (trusted anchor source ID, action origin,
        # owning response ID). Each entry consumes one bounded report and is
        # reset with the native epoch.
        self._root_tool_call_scopes: dict[
            str, tuple[str, tuple[str, str | None, str, str], str]
        ] = {}
        self._native_context_generation: str | None = None
        self._next_ingress_sequence = 1
        self._report_event_count = 0
        self._active_session_action_origin: tuple[str, str | None, str, str] | None = None
        self._active_session_action_response_id: str | None = None
        self._report_failure: BaseException | None = None
        self._report_failure_shutdown_task: asyncio.Task[None] | None = None
        self._session_action_failure_task: asyncio.Task[None] | None = None
        self._detached_request_tasks: set[asyncio.Task[None]] = set()
        self._next_child_request_invocation_id = 0
        # Installed by ExecutorAdapter after report v3 and action v2 capability
        # negotiation. Child requests carry a validated immutable origin binding.
        self._session_action_request_handler: (
            Callable[[_AcpJsonObject, AcpSessionActionBinding], Awaitable[None]] | None
        ) = None
        self._session_action_failure_handler: Callable[[], Awaitable[None]] | None = None
        self._initialized: bool = False
        self._image_supported: bool = False
        # One-way latch per subprocess: set after a successful ACP
        # ``authenticate``; reset on restart so a fresh process re-auths.
        self._authenticated: bool = False
        # ``initialize.result`` snapshot advertising auth (``authMethods`` +
        # ``_meta.defaultAuthMethodId``); consumed only when ``session/new``
        # actually reports that authentication is required.
        self._auth_advertisement: _AcpJsonObject = {}
        self._system_prompt_sent: bool = False
        # Model ids the agent listed in ``session/new.models.availableModels``.
        # Non-empty means the agent selects models through ``session/set_model``
        # rather than a ``model`` session config option.
        self._session_model_ids: set[str] = set()

        # ACP toolCallId → tool name / rawInput from the originating tool_call, so
        # a later tool_call_update can close the right tool card, and a permission
        # request that names only the id can still say what is about to run.
        self._tool_names: dict[str, str] = {}
        self._tool_inputs: dict[str, _AcpJsonObject] = {}

        # Context-window size (tokens) reported via ``usage_update``; surfaced by
        # :meth:`max_context_tokens` so the UI context meter fills.
        self._context_window: int | None = None

        # Bridges the ExecutorAdapter installs (by attribute) so the agent's
        # mid-turn ``session/request_permission`` routes through Omnigent's
        # TOOL_CALL policy + human-consent elicitation. ``None`` → no bridge
        # wired (standalone / unit tests) → permission falls back to allow.
        self._policy_evaluator: _PolicyEvaluator | None = None
        self._elicitation_handler: _ElicitationHandler | None = None
        self._elicitation_choice_handler: _ElicitationChoiceHandler | None = None
        # Adapter-injected tool-execution bridge (the same ``_tool_executor``
        # attribute the SDK harnesses use); backs the Omnigent MCP relay.
        self._tool_executor: _ToolExecutor | None = None

        # Omnigent-tool MCP bridge — exposes builtin tools to the agent via
        # session/new.mcpServers (lazily started at first session; torn down in
        # :meth:`close`). ``_omnigent_tools`` is captured each turn for the relay.
        self._mcp = OmnigentAcpMcp(label=config.name)
        self._omnigent_tools: list[ToolSpec] = []
        # Tool names the agent can use to reach the Omnigent MCP bridge, snapshotted
        # from what session/new advertised. Empty means nothing is a bridge call.
        self._bridge_tool_aliases: frozenset[str] = frozenset()

    def supports_session_reports(self) -> bool:
        """Whether this executor implements the trusted report-sink contract."""
        return True

    def enable_standard_subagents(
        self, report_features: Sequence[str], action_features: Sequence[str]
    ) -> None:
        """Enable the ACP preview after report v3 and action v2 are proven."""
        if self._initialized or self._session_id is not None:
            raise RuntimeError("standard ACP subagents must be enabled before initialization")
        if not set(REPORT_FEATURES).issubset(report_features):
            raise RuntimeError("ACP report channel lacks required standard-session v3 features")
        if not set(ACTION_FEATURES).issubset(action_features):
            raise RuntimeError("ACP action channel lacks required standard-session v2 features")
        self._standard_subagents_enabled = True

    def bind_session_action_origin(
        self,
        token: str | None,
        agent: str | None,
        generation: str | None = None,
        context_generation: str | None = None,
        response_id: str | None = None,
    ) -> None:
        """Bind trusted request authority, turn identity, and security context."""
        if token is None:
            if self._standard_subagents_enabled and any(
                session_id != self._session_id for session_id in self._standard_associations
            ):
                raise RuntimeError(
                    "ACP parent turn cannot continue with owned children without a trusted context"
                )
            self._active_session_action_origin = None
            self._active_session_action_response_id = None
            return
        if generation is None and self._session_report_sink is None:
            generation = "unbound"
        if context_generation is None and self._session_report_sink is None:
            context_generation = "unbound"
        if (
            not token
            or len(token) > 256
            or not generation
            or len(generation) > 256
            or not context_generation
            or len(context_generation) > 256
        ):
            raise ValueError("ACP session-action origin token is malformed")
        if response_id is not None and (
            not isinstance(response_id, str) or not response_id or len(response_id) > 64
        ):
            raise ValueError("ACP session-action response identity is malformed")
        if self._standard_subagents_enabled and response_id is None:
            raise ValueError("ACP session-action origin is missing its response identity")
        if (
            self._native_context_generation is not None
            and self._native_context_generation != context_generation
        ):
            raise RuntimeError(
                "ACP native session is bound to a different trusted security context"
            )
        self._active_session_action_origin = (token, agent, generation, context_generation)
        self._active_session_action_response_id = response_id
        if self._session_id is not None and self._native_context_generation is None:
            self._native_context_generation = context_generation
        if self._standard_subagents_enabled and self._session_id is not None:
            root_association = self._standard_associations.get(self._session_id)
            if root_association is not None and root_association.origin is None:
                root_association.origin = self._active_session_action_origin

    def clear_session_action_origin(self, token: str | None) -> None:
        """Clear only the active origin installed by the ending parent turn."""
        current = self._active_session_action_origin
        if current is not None and current[0] == token:
            self._active_session_action_origin = None
            self._active_session_action_response_id = None

    def set_session_report_sink(self, sink: SessionReportSink) -> None:
        """Attach the trusted runtime journal before ACP session setup begins."""
        if not self._standard_subagents_enabled:
            raise RuntimeError(
                "standard ACP reports require negotiated report v3 and action v2 channels"
            )
        if self._initialized or self._session_id is not None:
            raise RuntimeError("the ACP report sink must be installed before initialization")
        if self._session_report_sink is not None and self._session_report_sink is not sink:
            raise RuntimeError("the ACP report sink is already bound")
        if getattr(sink, "protocol_version", None) != 3:
            raise RuntimeError("standard ACP reports require private report protocol v3")
        if not set(REPORT_FEATURES).issubset(getattr(sink, "features", ())):
            raise RuntimeError("standard ACP reports require all negotiated report features")
        if not getattr(sink, "server_identity", None) or not getattr(
            sink, "root_conversation_id", None
        ):
            raise RuntimeError("standard ACP report sink has no trusted identity scope")
        self._session_report_sink = sink

    def session_report_status(self) -> SessionReportStatus | None:
        """Expose producer health without hiding reports already saved."""
        if not self._standard_subagents_enabled:
            return None
        if self._report_failure is not None:
            return SessionReportStatus(
                state="failed",
                error=describe_exception(self._report_failure),
                prompt_started=self._report_prompt_started,
            )
        return SessionReportStatus(
            state="active" if self._session_reports_negotiated else "starting",
            prompt_started=self._report_prompt_started,
        )

    # ------------------------------------------------------------------
    # Low-level ACP transport
    # ------------------------------------------------------------------

    async def _start_process(self) -> None:
        """Start the configured ACP agent as an asyncio subprocess.

        The StreamReader limit is raised to 16 MiB so a large ``session/new``
        response or tool-output line can't hit the default 64 KiB per-line cap.
        """
        # Re-validate cwd before every spawn and respawn: the workspace may have
        # been deleted after __init__, and this error flows through
        # _startup_error_message so the UI also shows the harness-log path.
        _resolve_cwd(self._cwd, session_id=self._session_id)
        # If the cwd is relative the spawn resolves it against the process cwd;
        # fail early with a clear message if that directory was also deleted.
        if not os.path.isabs(self._cwd):
            _resolve_cwd(None, session_id=self._session_id)
        # Reset handshake state: this may be a restart after the previous
        # subprocess died. ``_initialized`` is a one-way latch.
        self._initialized = False
        self._session_reports_negotiated = False
        self._report_prompt_started = False
        self._image_supported = False
        self._authenticated = False
        self._auth_advertisement = {}
        env = self._build_spawn_env()
        launch_path, argv = self._sandbox_launch(tuple(env.keys()))
        _STREAM_LIMIT = 16 * 1024 * 1024
        self._proc = await asyncio.create_subprocess_exec(
            launch_path,
            *argv,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
            cwd=self._cwd,
            limit=_STREAM_LIMIT,
            # Own session/group: the sandbox launcher forks the real agent,
            # and without a group boundary teardown reaches only the wrapper.
            **_proc.spawn_kwargs(),
        )
        self._reader_task = asyncio.create_task(self._read_stdout())
        self._stderr_task = asyncio.create_task(self._read_stderr())

    def _sandbox_launch(self, spawn_env_names: tuple[str, ...]) -> tuple[str, list[str]]:
        """Return ``(launch_path, argv)`` — sandbox launcher or the bare binary.

        When ``os_env.sandbox`` requests confinement, wraps the agent binary in
        the platform sandbox so its whole process tree runs confined to the
        spec's read/write roots. Falls back to the bare binary (never blocks
        startup) when no sandbox is requested or the backend is unavailable.

        ponytail: a sandboxed *generic* agent gets only its binary dir (read),
        the cwd, and private scratch space (write) — we can't know an arbitrary agent's config
        dir. An agent that must write its own config under a sandbox needs
        ``sandbox: none`` (the default) for now; per-agent write roots is a
        documented follow-up.
        """
        binary = self._argv[0]
        rest = self._argv[1:]
        os_env = self._os_env
        if os_env is None:
            return binary, rest
        sandbox_spec = os_env.sandbox or OSEnvSandboxSpec()
        if sandbox_spec.type == "none":
            return binary, rest
        try:
            from .sandbox import (
                create_exec_launcher,
                resolve_sandbox,
                with_additional_read_roots,
                with_spawn_env_allowlist,
            )

            cwd = Path(self._cwd or os.getcwd()).resolve(strict=False)
            sandbox = resolve_sandbox(os_env, cwd)
            if not sandbox.active:
                return binary, rest
            resolved_bin = Path(binary)
            if resolved_bin.parent != Path(".") and resolved_bin.exists():
                sandbox = with_additional_read_roots(sandbox, [resolved_bin.resolve().parent])
            sandbox = with_spawn_env_allowlist(sandbox, spawn_env_names)
            return create_exec_launcher(binary, sandbox), rest
        except (OSError, ImportError, NotImplementedError) as exc:
            logger.warning(
                "Could not apply sandbox for ACP agent %s; running unsandboxed: %s",
                self._config.name,
                exc,
            )
            return binary, rest

    def _startup_error_message(self, exc: BaseException) -> str:
        """Describe a handshake failure, quoting the agent's own stderr.

        Builds on :func:`describe_exception` (which keeps a bare
        ``TimeoutError`` from rendering blank), then appends what the agent
        printed — that text ("no API key", "unknown flag") is usually the actual
        diagnosis — and the log file so the full traceback is findable.
        """
        detail = describe_exception(exc)
        if self._recent_stderr:
            tail = " | ".join(list(self._recent_stderr)[-_STDERR_QUOTED_LINES:])
            # Cap here too, not just per line in the reader: this string ends up
            # in a UI toast, and the ring can hold several long lines.
            if len(tail) > _STDERR_QUOTED_LIMIT:
                tail = tail[:_STDERR_QUOTED_LIMIT] + "...[truncated]"
            detail = f"{detail}; {self._config.name} stderr: {tail}"
        log_path = current_process_log_path()
        if log_path is not None:
            detail = f"{detail} (harness log: {display_log_path(log_path)})"
        return detail

    async def _read_stderr(self) -> None:
        """Continuously drain the agent's stderr, logging each line at debug.

        Prevents a chatty CLI from filling the OS pipe buffer (~64 KiB) and
        stalling the turn. Also retains the trailing lines so a startup failure
        can quote what the agent said before it gave up.
        """
        assert self._proc and self._proc.stderr
        try:
            while True:
                raw_line = await self._proc.stderr.readline()
                if not raw_line:
                    break
                line = raw_line.decode("utf-8", errors="replace").rstrip()
                if line:
                    if len(line) > _STDERR_LINE_LIMIT:
                        line = line[:_STDERR_LINE_LIMIT] + "...[truncated]"
                    self._recent_stderr.append(line)
                    logger.debug("acp[%s] stderr: %s", self._config.name, line)
        except asyncio.CancelledError:
            # Expected: close() cancels this reader task on teardown.
            pass
        except Exception as exc:  # noqa: BLE001
            logger.debug("acp[%s] stderr reader stopped: %s", self._config.name, exc)

    async def _read_stdout(self) -> None:
        """Continuously read NDJSON lines from the agent's stdout.

        Responses resolve their RPC future. Negotiated session reports are
        durably reduced here, independently of a parent turn; other messages
        retain the existing turn-queue path.
        """
        assert self._proc and self._proc.stdout
        try:
            while True:
                raw_line = await self._proc.stdout.readline()
                if not raw_line:
                    # EOF — the subprocess exited. Wake in-flight futures so
                    # run_turn fails fast instead of blocking until idle timeout.
                    if self._session_reports_negotiated:
                        await self._record_transport_diagnostic("disconnected", "stdout_eof")
                        self._fail_report_delivery(
                            EOFError("ACP subprocess exited while session reporting was active")
                        )
                    for fut in self._pending.values():
                        if not fut.done():
                            fut.set_exception(EOFError("ACP subprocess closed stdout"))
                    break
                line = raw_line.decode("utf-8", errors="replace").strip()
                if not line:
                    continue
                try:
                    msg: _AcpJsonObject = json.loads(line)
                except json.JSONDecodeError:
                    logger.debug(
                        "acp[%s]: non-JSON stdout line: %r", self._config.name, line[:200]
                    )
                    continue

                msg_id = msg.get("id")
                # Match a response by "id + no method": the agent's own requests
                # (session/request_permission) also carry an id, so the method
                # check prevents a colliding request from mis-resolving our future.
                if msg_id is not None and "method" not in msg and msg_id in self._pending:
                    fut = self._pending.pop(msg_id)
                    if msg_id == self._active_prompt_request_id:
                        self._active_prompt_response_received = True
                        # run_turn may already be cancelled when the response arrives.
                        error = msg.get("error")
                        if isinstance(error, dict):
                            logger.debug(
                                "acp[%s] session/prompt %s error code=%s message=%s",
                                self._config.name,
                                msg_id,
                                error.get("code"),
                                error.get("message"),
                            )
                        else:
                            result = msg.get("result", {})
                            reason = result.get("stopReason") if isinstance(result, dict) else None
                            logger.debug(
                                "acp[%s] session/prompt %s stopReason=%s",
                                self._config.name,
                                msg_id,
                                reason,
                            )
                    if not fut.done():
                        fut.set_result(msg)
                else:
                    if (
                        msg.get("method") == _CLIENT_NOTIFICATION_SESSION_UPDATE
                        and self._session_id is not None
                        and await self._dispatch_session_report(msg)
                    ):
                        continue
                    if (
                        isinstance(msg.get("method"), str)
                        and msg_id is not None
                        and await self._dispatch_session_action_request(msg)
                    ):
                        continue
                    await self._queue.put(msg)
        except (asyncio.CancelledError, EOFError):
            # Expected during shutdown / after the subprocess closes stdout.
            pass
        except Exception as exc:
            logger.exception("acp[%s] stdout reader error: %s", self._config.name, exc)
            if self._session_reports_negotiated:
                await self._record_transport_diagnostic("unconfirmed", "stdout_reader_error")
                self._fail_report_delivery(exc)
            for fut in self._pending.values():
                if not fut.done():
                    fut.set_exception(exc)
            await self._queue.put({"type": "error", "message": describe_exception(exc)})

    async def _record_transport_diagnostic(self, state: str, reason: str) -> None:
        """Persist transport uncertainty without treating the last child state as current."""
        for native_session_id, association in tuple(self._standard_associations.items()):
            try:
                await self._append_standard_report(
                    self._report_identity(native_session_id, association.parent_session_id),
                    "observation.patch",
                    {
                        "observation": {
                            "state": {"present": False},
                            "transport": {"state": state, "reason": reason},
                        }
                    },
                )
            except Exception:
                logger.exception(
                    "acp[%s] could not persist transport loss for native session %s",
                    self._config.name,
                    native_session_id,
                )
                return

    def _schedule_report_failure_shutdown(self) -> None:
        """Stop owned ACP work after durable report delivery becomes unsafe."""
        if self._report_failure_shutdown_task is None:
            self._report_failure_shutdown_task = asyncio.create_task(
                self._shutdown_after_report_failure()
            )

    def _fail_report_delivery(self, exception: BaseException) -> None:
        """Latch a report failure and stop the process that could emit more work."""
        if self._report_failure is None:
            self._report_failure = exception
        self._cancel_detached_request_tasks()
        failure_handler = self._session_action_failure_handler
        if failure_handler is not None and self._session_action_failure_task is None:
            with contextlib.suppress(RuntimeError):

                async def run_failure_handler() -> None:
                    await failure_handler()

                self._session_action_failure_task = asyncio.create_task(
                    run_failure_handler(), name=f"acp-session-action-failure:{self._config.name}"
                )
        self._schedule_report_failure_shutdown()

    def _cancel_detached_request_tasks(self) -> None:
        """Cancel every parked child request without touching durable reports."""
        for task in tuple(self._detached_request_tasks):
            task.cancel()

    def _detached_request_done(self, task: asyncio.Task[None]) -> None:
        self._detached_request_tasks.discard(task)
        if task.cancelled():
            return
        exception = task.exception()
        if exception is not None:
            logger.warning(
                "acp[%s] detached child request failed",
                self._config.name,
                exc_info=(type(exception), exception, exception.__traceback__),
            )

    async def _call_report_sink(self, operation: Callable[..., Any], *args: Any) -> Any:
        """Run one synchronous journal mutation and fail closed if it fails."""
        try:
            return await asyncio.to_thread(operation, *args)
        except Exception as exc:
            self._fail_report_delivery(exc)
            raise

    async def _shutdown_after_report_failure(self) -> None:
        """Boundedly reap the ACP process tree without waiting on the reader task."""
        process = self._proc
        if process is None or process.returncode is not None:
            return
        logger.error(
            "acp[%s] stopping because session report delivery failed: %s",
            self._config.name,
            self._report_failure,
        )
        _proc.terminate_tree(process)
        try:
            await asyncio.wait_for(process.wait(), timeout=_ACP_TERMINATION_TIMEOUT_SECONDS)
        except TimeoutError:
            with contextlib.suppress(Exception):
                _proc.kill_tree(process)
            with contextlib.suppress(Exception):
                await asyncio.wait_for(process.wait(), timeout=1.0)

    async def _dispatch_session_report(self, message: _AcpJsonObject) -> bool:
        """Persist standard ACP operations and demultiplex known child traffic."""
        params = message.get("params")
        if not isinstance(params, dict):
            return False
        update = params.get("update")
        if not isinstance(update, dict):
            return False
        decoded = read_standard_subagent_frame(params)
        if decoded.recognized:
            if not self._standard_subagents_enabled:
                raise RuntimeError(
                    "standard ACP subagent traffic arrived without negotiated support"
                )
            self._require_standard_report_channel()
            if decoded.error is not None:
                raise RuntimeError(f"standard ACP subagent frame was rejected: {decoded.error}")
            operation = decoded.operation
            if operation is None:
                raise RuntimeError("recognized standard ACP frame has no normalized operation")
            parent_id = operation.parent_session_id
            target_id = operation.target_session_id
            if operation.kind == "association.patch":
                self._validate_association(operation)
                if not isinstance(parent_id, str) or not parent_id:
                    raise RuntimeError("standard ACP association has no validated parent")
                previous = self._standard_associations.get(target_id)
                if previous is None and len(self._standard_associations) >= _MAX_STANDARD_SESSIONS:
                    raise RuntimeError("standard ACP association limit reached")
                origin = (
                    previous.origin
                    if previous is not None
                    else self._association_action_origin(parent_id)
                )
                if origin is None:
                    raise RuntimeError(
                        "standard ACP association has no inherited trusted action origin"
                    )
                report_identity = self._report_identity(target_id, parent_id)
                source_id = await self._append_standard_report(
                    report_identity,
                    operation.kind,
                    dict(operation.operation),
                    action_origin=origin if previous is None else None,
                )
                patch = operation.operation.get("patch")
                if previous is None:
                    self._standard_associations[target_id] = _StandardSessionAssociation(
                        parent_session_id=parent_id,
                        association_source_id=source_id,
                        origin=origin,
                        cwd=(
                            patch.get("cwd")
                            if isinstance(patch, dict) and isinstance(patch.get("cwd"), str)
                            else None
                        ),
                        cwd_present=isinstance(patch, dict) and "cwd" in patch,
                        additional_directories_present=(
                            isinstance(patch, dict) and "additionalDirectories" in patch
                        ),
                        additional_directories=(
                            tuple(patch.get("additionalDirectories", ()))
                            if isinstance(patch, dict)
                            and isinstance(patch.get("additionalDirectories"), list)
                            else None
                        ),
                    )
                    self._session_translation_contexts[target_id] = _SessionTranslationContext()
                if previous is not None and isinstance(patch, dict):
                    if "cwd" in patch:
                        previous.cwd_present = True
                        previous.cwd = patch["cwd"] if isinstance(patch["cwd"], str) else None
                    if "additionalDirectories" in patch:
                        previous.additional_directories_present = True
                        directories = patch["additionalDirectories"]
                        previous.additional_directories = (
                            tuple(directories) if isinstance(directories, list) else None
                        )
                if isinstance(patch, dict) and "state" in patch:
                    await self._append_standard_report(
                        report_identity,
                        "observation.patch",
                        {"observation": {"state": {"present": True, "value": patch["state"]}}},
                    )
                state = patch.get("state") if isinstance(patch, dict) else None
                state_name = state.get("state") if isinstance(state, dict) else None
                if isinstance(state_name, str) and state_name in {
                    "running",
                    "requires_action",
                    "idle",
                    "unknown",
                }:
                    context = self._session_translation_contexts.get(target_id)
                    if context is not None:
                        context.active_message_id = None
                return True

            if target_id not in self._standard_associations:
                raise RuntimeError("standard ACP frame targets an unannounced native session")
            parent_id = self._standard_associations[target_id].parent_session_id
            report_identity = self._report_identity(target_id, parent_id)
            await self._append_standard_report(
                report_identity, operation.kind, dict(operation.operation)
            )
            return True

        if not self._standard_subagents_enabled:
            return False
        self._require_standard_report_channel()
        outer_session_id = params.get("sessionId")
        if not isinstance(outer_session_id, str) or not outer_session_id:
            return False
        if outer_session_id == self._session_id:
            references = self._tool_reference_operation(update)
            if self._active_prompt_request_id is None or self._active_prompt_response_received:
                operations = self._standard_update_operations(outer_session_id, update)
                if references is not None:
                    operations.append(("tool.references", references))
                call_id = update.get("toolCallId")
                root_scope = (
                    self._root_tool_call_scopes.get(call_id) if isinstance(call_id, str) else None
                )
                if root_scope is not None and isinstance(call_id, str):
                    self._scope_root_tool_operations(
                        operations, call_id=call_id, response_id=root_scope[2]
                    )
                for kind, normalized in operations:
                    await self._append_standard_report(
                        self._report_identity(outer_session_id, None),
                        kind,
                        normalized,
                    )
                return bool(operations)

            if update.get("sessionUpdate") not in {_UPDATE_TOOL_CALL, _UPDATE_TOOL_CALL_UPDATE}:
                return False
            call_id = update.get("toolCallId")
            if not isinstance(call_id, str) or not call_id:
                raise RuntimeError("ACP root tool update has no native call id")
            existing_scope = self._root_tool_call_scopes.get(call_id)
            root_scope = await self._ensure_root_tool_call_scope(outer_session_id, update)
            if root_scope is None:
                raise RuntimeError("ACP root tool update has no trusted call scope")
            operations = self._standard_update_operations(outer_session_id, update)
            self._scope_root_tool_operations(
                operations, call_id=call_id, response_id=root_scope[2]
            )
            if references is not None:
                operations.append(("tool.references", references))
            for kind, normalized in operations:
                await self._append_standard_report(
                    self._report_identity(outer_session_id, None),
                    kind,
                    normalized,
                )
            return (
                existing_scope is not None
                and root_scope[2] != self._active_session_action_response_id
            )
        association = self._standard_associations.get(outer_session_id)
        if association is None:
            raise RuntimeError("ACP update arrived for an unannounced native session")

        operations = self._standard_update_operations(outer_session_id, update)
        references = self._tool_reference_operation(update)
        if references is not None:
            operations.append(("tool.references", references))
        for kind, normalized in operations:
            await self._append_standard_report(
                self._report_identity(outer_session_id, association.parent_session_id),
                kind,
                normalized,
            )
        return True

    def _require_standard_report_channel(self) -> tuple[str, str, str]:
        native_epoch = self._native_epoch
        root_session_id = self._session_id
        sink = self._session_report_sink
        if (
            not self._standard_subagents_enabled
            or not self._session_reports_negotiated
            or native_epoch is None
            or root_session_id is None
            or sink is None
        ):
            raise RuntimeError(
                "standard ACP report arrived before the private v3 channel was ready"
            )
        return native_epoch, root_session_id, sink.server_identity

    def _active_origin_for_report(self) -> tuple[str, str | None, str, str]:
        origin = self._active_session_action_origin
        if origin is None:
            raise RuntimeError("standard ACP association has no trusted parent-turn origin")
        if self._native_context_generation is None:
            self._native_context_generation = origin[3]
        if self._native_context_generation != origin[3]:
            raise RuntimeError("standard ACP association changed trusted security context")
        return origin

    def _report_identity(
        self, target_session_id: str, parent_session_id: str | None
    ) -> AcpSessionReportIdentity:
        native_epoch, root_native_session_id, server_identity = (
            self._require_standard_report_channel()
        )
        context_generation = self._native_context_generation
        if context_generation is None:
            origin = self._active_origin_for_report()
            context_generation = origin[3]
        sink = self._session_report_sink
        assert sink is not None
        return AcpSessionReportIdentity(
            server_identity=server_identity,
            root_conversation_id=sink.root_conversation_id,
            native_epoch=native_epoch,
            context_generation=context_generation,
            root_native_session_id=root_native_session_id,
            target_session_id=target_session_id,
            parent_session_id=parent_session_id,
        )

    async def _append_standard_report(
        self,
        identity: AcpSessionReportIdentity,
        kind: str,
        operation: _AcpJsonObject,
        *,
        action_origin: tuple[str, str | None, str, str] | None = None,
    ) -> str:
        sink = self._session_report_sink
        if sink is None:
            raise RuntimeError("standard ACP report sink is unavailable")
        if self._report_event_count >= _MAX_STANDARD_REPORTS:
            raise RuntimeError("standard ACP report limit reached")
        ingress_sequence = self._next_ingress_sequence
        if ingress_sequence >= 2**63 - 1:
            raise RuntimeError("standard ACP ingress sequence limit reached")
        self._next_ingress_sequence += 1
        source_id = acp_standard_report_source_id(
            identity.server_identity,
            identity.root_conversation_id,
            identity.native_epoch,
            ingress_sequence,
        )
        data: _AcpJsonObject = {
            "server_identity": identity.server_identity,
            "root_conversation_id": identity.root_conversation_id,
            "context_generation": identity.context_generation,
            "root_native_session_id": identity.root_native_session_id,
            "target_session_id": identity.target_session_id,
            "parent_session_id": identity.parent_session_id,
            "operation": operation,
        }
        if action_origin is not None:
            data["_omnigent_action_origin"] = {
                "token": action_origin[0],
                "agent": action_origin[1],
                "generation": action_origin[2],
            }
        await self._call_report_sink(
            sink.append,
            SessionReport(
                native_epoch=identity.native_epoch,
                ingress_sequence=ingress_sequence,
                source_id=source_id,
                kind=kind,
                data=data,
            ),
        )
        self._report_event_count += 1
        return source_id

    def _validate_association(self, operation: AcpSessionOperation) -> None:
        root_session_id = self._session_id
        target_id = operation.target_session_id
        parent_id = operation.parent_session_id
        if target_id in (root_session_id, parent_id):
            raise RuntimeError("standard ACP association contains a root/self cycle")
        if parent_id not in self._standard_associations:
            raise RuntimeError("standard ACP association has an unannounced immediate parent")
        existing = self._standard_associations.get(target_id)
        if existing is not None and existing.parent_session_id != parent_id:
            raise RuntimeError("standard ACP association attempted to reparent a native session")
        ancestor = parent_id
        visited = {target_id}
        while ancestor is not None:
            if ancestor in visited:
                raise RuntimeError("standard ACP association contains an ownership cycle")
            visited.add(ancestor)
            parent = self._standard_associations.get(ancestor)
            if parent is None:
                raise RuntimeError("standard ACP association ancestry is incomplete")
            ancestor = parent.parent_session_id

    def _association_action_origin(
        self, parent_session_id: str
    ) -> tuple[str, str | None, str, str] | None:
        """Return the immutable immediate-parent origin for a new association."""
        parent = self._standard_associations.get(parent_session_id)
        if parent is None:
            raise RuntimeError("standard ACP association parent is not registered")
        if (
            parent_session_id == self._session_id
            and self._active_prompt_request_id is not None
            and not self._active_prompt_response_received
        ):
            origin = self._active_session_action_origin
        else:
            origin = parent.origin
        if origin is None:
            return None
        if self._native_context_generation is None:
            self._native_context_generation = origin[3]
        if origin[3] != self._native_context_generation:
            raise RuntimeError("standard ACP association origin changed security context")
        return origin

    def _association_filesystem_overrides(
        self, target_session_id: str
    ) -> tuple[bool, str | None, bool, tuple[str, ...] | None]:
        """Resolve effective filesystem values before asynchronous dispatch.

        ACP ``null`` values inherit the immediate parent's effective value;
        an explicit empty directory list overrides the parent with no extras.
        The native root starts at the executor's cwd and an empty directory set.
        """
        current = self._standard_associations.get(target_session_id)
        if current is None:
            raise RuntimeError("ACP child action target is not registered")
        cwd = self._cwd
        directories: tuple[str, ...] = ()
        cwd_resolved = False
        directories_resolved = False
        visited: set[str] = set()
        while current is not None:
            if current.parent_session_id is not None:
                if current.parent_session_id in visited:
                    raise RuntimeError("ACP action filesystem ancestry contains a cycle")
                visited.add(current.parent_session_id)
            if not cwd_resolved and current.cwd is not None:
                cwd = current.cwd
                cwd_resolved = True
            if not directories_resolved and current.additional_directories is not None:
                directories = current.additional_directories
                directories_resolved = True
            if cwd_resolved and directories_resolved:
                break
            parent_id = current.parent_session_id
            current = self._standard_associations.get(parent_id) if parent_id is not None else None
            if parent_id is not None and current is None:
                raise RuntimeError("ACP action filesystem ancestry is incomplete")
        return True, cwd, True, directories

    def _tool_reference_operation(self, update: _AcpJsonObject) -> _AcpJsonObject | None:
        update_type = update.get("sessionUpdate")
        if not isinstance(update_type, str) or update_type not in {
            _UPDATE_TOOL_CALL,
            _UPDATE_TOOL_CALL_UPDATE,
        }:
            return None
        content = update.get("content")
        if content is not None and not isinstance(content, list):
            return None
        try:
            references = read_session_tool_references(update)
        except ValueError as exc:
            raise RuntimeError(str(exc)) from exc
        if references is None:
            return None
        call_id = update.get("toolCallId")
        if not isinstance(call_id, str) or not call_id:
            if references:
                raise RuntimeError("ACP session tool reference has no owning tool id")
            return None
        if len(call_id) > 512:
            raise RuntimeError("ACP session tool reference tool id exceeds its size limit")
        return {
            "entity_id": acp_standard_tool_entity_id(call_id),
            "call_id": call_id,
            "references": list(references),
        }

    async def _ensure_root_tool_call_scope(
        self,
        root_session_id: str,
        update: _AcpJsonObject,
    ) -> tuple[str, tuple[str, str | None, str, str], str] | None:
        """Persist one identity and response anchor before a root call can be routed."""
        if not self._session_reports_negotiated:
            return None
        update_type = update.get("sessionUpdate")
        if update_type not in {_UPDATE_TOOL_CALL, _UPDATE_TOOL_CALL_UPDATE}:
            return None
        call_id = update.get("toolCallId")
        if not isinstance(call_id, str) or not call_id or len(call_id) > 512:
            raise RuntimeError("ACP root tool call has no valid native call id")
        entity_id = acp_standard_tool_entity_id(call_id)
        existing = self._root_tool_call_scopes.get(call_id)
        if existing is not None:
            return existing
        if len(self._root_tool_call_scopes) >= _MAX_ROOT_TOOL_CALL_SCOPES:
            raise RuntimeError("ACP root tool-call scope limit reached")
        if self._active_prompt_request_id is None or self._active_prompt_response_received:
            raise RuntimeError("ACP root tool call first appeared outside its active prompt")

        origin = self._active_origin_for_report()
        response_id = self._active_session_action_response_id
        if not isinstance(response_id, str) or not response_id or len(response_id) > 64:
            raise RuntimeError("ACP root tool call has no trusted response identity")
        anchor_operation = {
            "entity_id": entity_id,
            "call_id": call_id,
            "response_id": response_id,
        }
        source_id = await self._append_standard_report(
            self._report_identity(root_session_id, None),
            "tool.references",
            anchor_operation,
            action_origin=origin,
        )
        scope = (source_id, origin, response_id)
        self._root_tool_call_scopes[call_id] = scope
        return scope

    @staticmethod
    def _scope_root_tool_operations(
        operations: list[tuple[str, _AcpJsonObject]],
        *,
        call_id: str,
        response_id: str,
    ) -> None:
        """Pin root tool entity operations to the response that owns their anchor."""
        for kind, operation in operations:
            if kind not in {"entity.upsert", "entity.patch"}:
                continue
            entity_id = operation.get("entity_id")
            if entity_id not in {
                acp_standard_tool_entity_id(call_id),
                acp_standard_tool_output_entity_id(call_id),
            }:
                continue
            operation["call_id"] = call_id
            if kind != "entity.upsert":
                continue
            value = operation.get("value")
            if not isinstance(value, dict) or value.get("item_type") not in {
                "function_call",
                "function_call_output",
            }:
                continue
            value["response_id"] = response_id

    def _root_tool_call_scope_metadata(self, call_id: str) -> _AcpJsonObject | None:
        if not self._session_reports_negotiated:
            return None
        scope = self._root_tool_call_scopes.get(call_id)
        if scope is None:
            raise RuntimeError("ACP root tool call has no persisted identity anchor")
        source_id, origin, response_id = scope
        return {
            "source_id": source_id,
            "native_call_id": call_id,
            "action_origin_token": origin[0],
            "action_origin_generation": origin[2],
            "response_id": response_id,
        }

    def _standard_update_operations(
        self, session_id: str, update: _AcpJsonObject
    ) -> list[tuple[str, _AcpJsonObject]]:
        update_type = update.get("sessionUpdate")
        context = self._session_translation_contexts.setdefault(
            session_id, _SessionTranslationContext()
        )
        if update_type == _UPDATE_AGENT_MESSAGE_CHUNK:
            content = update.get("content")
            if not isinstance(content, dict):
                raise RuntimeError("ACP child message chunk has invalid content")
            native_message_id = update.get("messageId")
            if native_message_id is not None:
                if (
                    not isinstance(native_message_id, str)
                    or not native_message_id
                    or len(native_message_id) > 512
                ):
                    raise RuntimeError("ACP child message chunk has an invalid messageId")
                entity_id = native_message_id
                context.active_message_id = None
            else:
                if context.active_message_id is None:
                    context.active_message_id = f"acp2m_{secrets.token_urlsafe(16)}"
                entity_id = context.active_message_id

            response_key = ("message", entity_id)
            is_new_message = response_key not in context.response_ids
            response_id = self._standard_response_id(context, *response_key)
            operations: list[tuple[str, _AcpJsonObject]] = []
            if is_new_message:
                operations.append(
                    (
                        "entity.upsert",
                        {
                            "entity_type": "message",
                            "entity_id": entity_id,
                            "value": {
                                "item_type": "message",
                                "item_data": {
                                    "role": "assistant",
                                    "content": [],
                                    "acp_agent_identity": "unknown",
                                },
                                "response_id": response_id,
                            },
                        },
                    )
                )
            operations.append(
                (
                    "entity.append",
                    {
                        "entity_type": "message",
                        "entity_id": entity_id,
                        "content": [dict(content)],
                    },
                )
            )
            return operations
        if update_type == _UPDATE_AGENT_THOUGHT_CHUNK:
            content = update.get("content")
            if not isinstance(content, dict):
                raise RuntimeError("ACP child reasoning chunk has invalid content")
            entity_id = f"acp2i_{secrets.token_urlsafe(16)}"
            text = self._standard_content_text(content)
            return [
                (
                    "entity.upsert",
                    {
                        "entity_type": "item",
                        "entity_id": entity_id,
                        "value": {
                            "item_type": "reasoning",
                            "item_data": {
                                "acp_agent_identity": "unknown",
                                "summary": [{"type": "summary_text", "text": text}],
                                "content": [{"type": "reasoning_text", "text": text}],
                            },
                            "response_id": self._standard_response_id(context, "item", entity_id),
                        },
                    },
                )
            ]
        if update_type == _UPDATE_TOOL_CALL:
            call_id = update.get("toolCallId")
            if not isinstance(call_id, str) or not call_id or len(call_id) > 512:
                raise RuntimeError("ACP child tool call has no toolCallId")
            name = update.get("title") or update.get("kind") or "tool"
            raw_input = update.get("rawInput")
            args = raw_input if isinstance(raw_input, dict) else {}
            status = update.get("status")
            if status in {_TOOL_STATUS_COMPLETED, _TOOL_STATUS_FAILED}:
                context.tool_names.pop(call_id, None)
                context.tool_inputs.pop(call_id, None)
            else:
                context.tool_names[call_id] = str(name)
                context.tool_inputs[call_id] = dict(args)
            context.active_message_id = None
            response_id = self._standard_response_id(context, "tool", call_id)
            operations = [
                (
                    "entity.upsert",
                    {
                        "entity_type": "item",
                        "entity_id": acp_standard_tool_entity_id(call_id),
                        "call_id": call_id,
                        "value": {
                            "item_type": "function_call",
                            "item_data": {
                                "acp_agent_identity": "unknown",
                                "call_id": call_id,
                                "name": str(name),
                                "arguments": json.dumps(
                                    args, ensure_ascii=False, separators=(",", ":")
                                ),
                                "tool_status": update.get("status", "in_progress"),
                            },
                            "response_id": response_id,
                        },
                    },
                )
            ]
            has_output_update = "content" in update or "rawOutput" in update
            if has_output_update:
                content = update.get("content")
                if content is not None and (
                    not isinstance(content, list)
                    or any(not isinstance(block, dict) for block in content)
                ):
                    raise RuntimeError("ACP child tool call has invalid content")
                output = content if "content" in update else update.get("rawOutput")
                output_entity_id = acp_standard_tool_output_entity_id(call_id)
                if output is None:
                    operations.append(
                        (
                            "entity.patch",
                            {
                                "entity_type": "item",
                                "entity_id": output_entity_id,
                                "call_id": call_id,
                                "patch": {
                                    "output": None,
                                    "content": None,
                                    "raw_output": update.get("rawOutput"),
                                    "acp_agent_identity": "unknown",
                                },
                            },
                        )
                    )
                else:
                    output_data: _AcpJsonObject = {
                        "call_id": call_id,
                        "output": self._standard_content_text(output),
                        "acp_agent_identity": "unknown",
                    }
                    if "content" in update:
                        output_data["content"] = content
                    if "rawOutput" in update:
                        output_data["raw_output"] = update["rawOutput"]
                    operations.append(
                        (
                            "entity.upsert",
                            {
                                "entity_type": "item",
                                "entity_id": output_entity_id,
                                "call_id": call_id,
                                "value": {
                                    "item_type": "function_call_output",
                                    "item_data": output_data,
                                    "response_id": response_id,
                                },
                            },
                        )
                    )
            return operations
        if update_type == _UPDATE_TOOL_CALL_UPDATE:
            call_id = update.get("toolCallId")
            if not isinstance(call_id, str) or not call_id or len(call_id) > 512:
                raise RuntimeError("ACP child tool update has no toolCallId")
            patch: _AcpJsonObject = {}
            if "status" in update:
                patch["tool_status"] = update["status"]
            has_output_update = "content" in update or "rawOutput" in update
            output = update.get("content") if "content" in update else update.get("rawOutput")
            if has_output_update:
                patch["output"] = None if output is None else self._standard_content_text(output)
            if "content" in update:
                content = update["content"]
                if content is not None and (
                    not isinstance(content, list)
                    or any(not isinstance(block, dict) for block in content)
                ):
                    raise RuntimeError("ACP child tool update has invalid content")
                patch["output_content"] = content
            if "rawOutput" in update:
                patch["raw_output"] = update["rawOutput"]
            if "title" in update:
                title = update["title"]
                if isinstance(title, str):
                    patch["name"] = title
            if "rawInput" in update:
                raw_input = update["rawInput"]
                if raw_input is None:
                    patch["arguments"] = "null"
                elif isinstance(raw_input, dict):
                    patch["arguments"] = json.dumps(
                        raw_input, ensure_ascii=False, separators=(",", ":")
                    )
                else:
                    raise RuntimeError("ACP child tool update has invalid rawInput")
            status = update.get("status")
            if isinstance(status, str) and status in {
                _TOOL_STATUS_COMPLETED,
                _TOOL_STATUS_FAILED,
            }:
                context.tool_names.pop(call_id, None)
                context.tool_inputs.pop(call_id, None)
            context.active_message_id = None
            operations = (
                [
                    (
                        "entity.patch",
                        {
                            "entity_type": "item",
                            "entity_id": acp_standard_tool_entity_id(call_id),
                            "call_id": call_id,
                            "patch": patch,
                        },
                    )
                ]
                if patch
                else []
            )
            if has_output_update:
                output_entity_id = acp_standard_tool_output_entity_id(call_id)
                if output is None:
                    operations.append(
                        (
                            "entity.patch",
                            {
                                "entity_type": "item",
                                "entity_id": output_entity_id,
                                "call_id": call_id,
                                "patch": {
                                    "output": None,
                                    "content": None,
                                    "raw_output": update.get("rawOutput"),
                                    "acp_agent_identity": "unknown",
                                },
                            },
                        )
                    )
                else:
                    output_data: _AcpJsonObject = {
                        "call_id": call_id,
                        "output": self._standard_content_text(output),
                        "acp_agent_identity": "unknown",
                    }
                    if isinstance(update.get("content"), list) or "content" in update:
                        output_data["content"] = update["content"]
                    if "rawOutput" in update:
                        output_data["raw_output"] = update["rawOutput"]
                    operations.append(
                        (
                            "entity.upsert",
                            {
                                "entity_type": "item",
                                "entity_id": output_entity_id,
                                "call_id": call_id,
                                "value": {
                                    "item_type": "function_call_output",
                                    "item_data": output_data,
                                    "response_id": self._standard_response_id(
                                        context, "tool", call_id
                                    ),
                                },
                            },
                        )
                    )
            return operations
        if update_type == _UPDATE_USAGE:
            observation = {key: value for key, value in update.items() if key != "sessionUpdate"}
            return [("observation.patch", {"observation": {"usage": observation}})]
        if update_type == _UPDATE_CONFIG_OPTION:
            return [
                (
                    "observation.patch",
                    {"observation": {"config_options": update.get("configOptions")}},
                )
            ]
        if update_type == "session_info_update":
            session_info = {key: value for key, value in update.items() if key != "sessionUpdate"}
            return [
                (
                    "observation.patch",
                    {"observation": {"session_info": session_info}},
                )
            ]
        if update_type == "current_mode_update":
            mode = {key: value for key, value in update.items() if key != "sessionUpdate"}
            return [
                (
                    "observation.patch",
                    {"observation": {"mode": mode}},
                )
            ]
        if update_type in {"plan", "plan_update"}:
            return [
                (
                    "observation.patch",
                    {
                        "observation": {
                            "plan": {
                                key: value
                                for key, value in update.items()
                                if key != "sessionUpdate"
                            }
                        }
                    },
                )
            ]
        if update_type == "plan_removed":
            return [("observation.patch", {"observation": {"plan": None}})]
        if update_type == "session_state_update":
            if "state" not in update:
                return [("observation.patch", {"observation": {"state": {"present": False}}})]
            state = update["state"]
            state_name = state.get("state") if isinstance(state, dict) else None
            if isinstance(state_name, str) and state_name in {
                "running",
                "requires_action",
                "idle",
                "unknown",
            }:
                context.active_message_id = None
            return [
                (
                    "observation.patch",
                    {"observation": {"state": {"present": True, "value": state}}},
                )
            ]
        if isinstance(update_type, str):
            opaque = {key: value for key, value in update.items() if key != "sessionUpdate"}
            return [
                (
                    "observation.patch",
                    {"observation": {"protocol_extensions": {update_type: opaque}}},
                )
            ]
        return []

    @staticmethod
    def _standard_response_id(
        context: _SessionTranslationContext, entity_type: str, entity_id: str
    ) -> str:
        """Return a stable per-entity response id within the native session."""
        key = (entity_type, entity_id)
        response_id = context.response_ids.get(key)
        if response_id is None:
            response_id = f"acp2r_{secrets.token_urlsafe(16)}"
            context.response_ids[key] = response_id
        return response_id

    @staticmethod
    def _standard_content_text(content: Any) -> str:
        """Make ACP content readable while retaining the original payload separately."""
        if isinstance(content, str):
            return content
        if isinstance(content, list) and any(
            isinstance(item, dict) and item.get("type") in {"content", "diff", "terminal"}
            for item in content
        ):
            # The ToolCallContent union renders like the non-standard tool path.
            from omnigent.runtime.harnesses._executor_adapter import _stringify_tool_payload

            return _stringify_tool_payload(content)
        if isinstance(content, dict):
            text = content.get("text")
            if isinstance(text, str):
                return text
            return json.dumps(content, ensure_ascii=False, separators=(",", ":"))
        if isinstance(content, list):
            return "\n".join(AcpExecutor._standard_content_text(item) for item in content)
        return json.dumps(content, ensure_ascii=False, separators=(",", ":"))

    def _standard_tool_output_metadata(self, update: _AcpJsonObject) -> _AcpJsonObject:
        """Retain output field presence and normalized text for the standard adapter."""
        present = "content" in update or "rawOutput" in update
        output = update.get("content") if "content" in update else update.get("rawOutput")
        cleared = present and output is None
        return {
            "present": present,
            "cleared": cleared,
            "text": self._standard_content_text(output) if present and not cleared else None,
            "content_present": "content" in update,
            "content": update.get("content"),
            "raw_output_present": "rawOutput" in update,
            "raw_output": update.get("rawOutput"),
        }

    async def _dispatch_session_action_request(self, request: _AcpJsonObject) -> bool:
        """Route an ACP child request using its immutable validated owner binding."""
        decoded: AcpSessionActionRead = read_standard_session_action(request)
        if decoded.recognized:
            if decoded.error is not None or decoded.identity is None:
                await self._send_agent_request_error(
                    request,
                    -32603,
                    decoded.error or "omnigent: ACP request identity is incomplete",
                )
                return True
            identity = decoded.identity
            if not self._standard_subagents_enabled:
                if identity.target_session_id != self._session_id:
                    await self._send_agent_request_error(
                        request,
                        -32603,
                        "omnigent: ACP child requests require negotiated standard-session support",
                    )
                    return True
                return False
            native_epoch = self._native_epoch
            root_native_session_id = self._session_id
            association = self._standard_associations.get(identity.target_session_id)
            if identity.target_session_id == root_native_session_id:
                return False
            if self._next_child_request_invocation_id >= (2**63 - 1):
                await self._send_agent_request_error(
                    request,
                    -32000,
                    "omnigent: ACP child request invocation capacity reached",
                )
                return True
            self._next_child_request_invocation_id += 1
            if (
                self._report_failure is not None
                or native_epoch is None
                or root_native_session_id is None
                or association is None
            ):
                await self._send_agent_request_error(
                    request,
                    -32603,
                    "omnigent: ACP request does not belong to an associated native session",
                )
                return True
            if not association.association_source_id:
                await self._send_agent_request_error(
                    request,
                    -32603,
                    "omnigent: ACP child request has no acknowledged association source",
                )
                return True
            if not association.parent_session_id:
                await self._send_agent_request_error(
                    request, -32603, "omnigent: ACP child has no validated immediate parent"
                )
                return True
            trusted_origin = association.origin
            if (
                trusted_origin is None
                or not trusted_origin[0]
                or not trusted_origin[2]
                or trusted_origin[3] != self._native_context_generation
            ):
                await self._send_agent_request_error(
                    request,
                    -32603,
                    "omnigent: ACP child request has no matching trusted security context",
                )
                return True
            if self._session_report_sink is None:
                await self._send_agent_request_error(
                    request,
                    -32603,
                    "omnigent: ACP child action channel is unavailable",
                )
                return True
            cwd_present, cwd, directories_present, directories = (
                self._association_filesystem_overrides(identity.target_session_id)
            )
            binding = AcpSessionActionBinding(
                root_conversation_id=self._session_report_sink.root_conversation_id,
                native_epoch=native_epoch,
                context_generation=trusted_origin[3],
                root_native_session_id=root_native_session_id,
                target_session_id=identity.target_session_id,
                parent_session_id=association.parent_session_id,
                association_source_id=association.association_source_id,
                origin_token=trusted_origin[0],
                origin_agent=trusted_origin[1],
                origin_generation=trusted_origin[2],
                request_id=identity.request_id,
                method=identity.method,
                request_invocation_id=self._next_child_request_invocation_id,
                cwd_present=cwd_present,
                cwd=cwd,
                additional_directories_present=directories_present,
                additional_directories=directories,
            )
            handler = self._session_action_request_handler
            if handler is None:
                await self._send_agent_request_error(
                    request, -32603, "omnigent: ACP child action bridge is unavailable"
                )
                return True
            if len(self._detached_request_tasks) >= _MAX_DETACHED_ACP_REQUESTS:
                await self._send_agent_request_error(
                    request, -32000, "omnigent: too many ACP child requests are pending"
                )
                return True

            async def handle_detached_request() -> None:
                await handler(request, binding)

            task = asyncio.create_task(
                handle_detached_request(),
                name=(
                    f"acp-child-request:{identity.target_session_id}:"
                    f"{self._next_child_request_invocation_id}"
                ),
            )
            self._detached_request_tasks.add(task)
            task.add_done_callback(self._detached_request_done)
            return True
        return False

    async def _send_agent_request_error(
        self, request: _AcpJsonObject, code: int, message: str
    ) -> None:
        """Reply to a rejected server-initiated ACP request."""
        await self._send(
            {
                "jsonrpc": "2.0",
                "id": request.get("id"),
                "error": {"code": code, "message": message},
            }
        )

    async def _send(self, msg: _AcpJsonObject) -> None:
        """Write one newline-terminated JSON message to the agent's stdin."""
        assert self._proc and self._proc.stdin
        encoded = (json.dumps(msg) + "\n").encode("utf-8")
        async with self._write_lock:
            self._proc.stdin.write(encoded)
            await self._proc.stdin.drain()

    async def _rpc(
        self,
        method: str,
        params: _AcpJsonObject,
        timeout: float = _INIT_TIMEOUT_SECONDS,
    ) -> _AcpJsonObject:
        """Send a JSON-RPC 2.0 request and await its response."""
        self._rpc_id += 1
        req_id = self._rpc_id
        loop = asyncio.get_event_loop()
        fut: asyncio.Future[_AcpJsonObject] = loop.create_future()
        self._pending[req_id] = fut

        await self._send({"jsonrpc": "2.0", "id": req_id, "method": method, "params": params})
        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        except TimeoutError as exc:
            self._pending.pop(req_id, None)
            # asyncio.TimeoutError carries no message, so a caller reporting it
            # by str() would surface a blank failure. Name the stalled call.
            raise TimeoutError(
                f"ACP agent {self._config.name!r} did not answer {method} "
                f"within {timeout:g}s (command: {self._config.command!r})"
            ) from exc

    # ------------------------------------------------------------------
    # ACP handshake
    # ------------------------------------------------------------------

    def _build_spawn_env(self) -> dict[str, str]:
        """The env handed to the generic ACP subprocess.

        Deny-by-default: base + the names declared by the agent's own config and
        by the spec's ``os_env.sandbox.env_passthrough``. No prefix family is
        added because the executor cannot know which vendor an arbitrary ACP
        agent belongs to; an agent that authenticates from a variable names it
        instead, which keeps every *other* provider's secret out.

        Kept as a named builder so the spawn-env canary can drive the real thing
        rather than a hand-copied prefix list. The canary constructs a bare
        executor carrying only what the builder reads, so the agent config is
        read defensively rather than assumed present.
        """
        config = getattr(self, "_config", None)
        return strip_runner_auth_secrets(
            clean_agent_env(
                allow_prefixes=(),
                extra_allowed=(
                    *getattr(config, "env_passthrough", ()),
                    *declared_passthrough(self._os_env),
                ),
                deny_exact=getattr(config, "env_unset", ()),
            )
        )

    def _warn_initialize_failed(self, reason: str) -> None:
        """Point a failed handshake at the env allowlist.

        A generic ACP agent gets the base environment plus whatever
        ``os_env.sandbox.env_passthrough`` declares — nothing else, since the
        executor cannot know which variable an arbitrary agent authenticates
        with. An agent that reads e.g. ``GEMINI_API_KEY`` therefore starts
        unauthenticated and usually dies during ``initialize``. That looks like
        a protocol fault, so name the likely cause once here rather than let
        every operator rediscover it.
        """
        logger.warning(
            "acp initialize failed for %r: %s. If this agent authenticates from an "
            "environment variable, declare it in os_env.sandbox.env_passthrough — "
            "the spawn environment is filtered to the base set plus that list.",
            self._config.command,
            reason,
        )

    async def _ensure_initialized(self) -> None:
        """Perform the ``initialize`` handshake if not already done."""
        if self._initialized:
            return
        if self._standard_subagents_enabled and self._session_report_sink is None:
            raise RuntimeError("standard ACP reporting requires the trusted private v3 channels")
        client_capabilities: _AcpJsonObject = {
            "fs": {
                "readTextFile": self._fs_delegation,
                "writeTextFile": self._fs_delegation,
            },
            "terminal": False,
        }
        if self._standard_subagents_enabled:
            client_capabilities["subagents"] = {}
        try:
            resp = await self._rpc(
                _AGENT_METHOD_INITIALIZE,
                {
                    "protocolVersion": _PROTOCOL_VERSION,
                    "clientInfo": {"name": "omnigent", "version": "1.0"},
                    "clientCapabilities": client_capabilities,
                },
                timeout=_INIT_TIMEOUT_SECONDS,
            )
        except Exception as exc:
            # Covers the child dying or timing out before it answers.
            self._warn_initialize_failed(str(exc))
            raise
        if "error" in resp:
            message = resp["error"].get("message", resp["error"])
            self._warn_initialize_failed(str(message))
            raise RuntimeError(f"ACP initialize failed: {message}")
        result = resp.get("result") or {}
        agent_capabilities = result.get("agentCapabilities") if isinstance(result, dict) else None
        prompt_caps = (
            agent_capabilities.get("promptCapabilities", {})
            if isinstance(agent_capabilities, dict)
            else {}
        )
        self._image_supported = bool(prompt_caps.get("image"))
        # Stash the advertised auth methods but do NOT authenticate here:
        # sending an unsolicited ``authenticate`` would change the handshake
        # for every agent that advertises methods informationally (or is
        # already authenticated via env/disk). ``_ensure_session`` reacts
        # only when ``session/new`` actually demands authentication.
        self._auth_advertisement = result if isinstance(result, dict) else {}
        self._session_reports_negotiated = self._standard_subagents_enabled
        self._initialized = True

    async def _authenticate(self, method_id: str) -> None:
        """Send ACP ``authenticate`` with *method_id* and latch success.

        Called reactively from :meth:`_ensure_session` after ``session/new``
        reports that authentication is required — never proactively, so agents
        whose ``session/new`` already succeeds keep their handshake untouched.
        """
        auth_resp = await self._rpc(
            _AGENT_METHOD_AUTHENTICATE,
            {"methodId": method_id},
            timeout=_INIT_TIMEOUT_SECONDS,
        )
        if "error" in auth_resp:
            error = auth_resp["error"]
            message = error.get("message", error) if isinstance(error, dict) else error
            raise RuntimeError(f"ACP authenticate failed: {message}")
        self._authenticated = True

    async def _authenticate_and_retry_session_new(
        self, resp: _AcpJsonObject, params: _AcpJsonObject, native_epoch: str | None
    ) -> tuple[_AcpJsonObject, str | None]:
        """Authenticate with an advertised headless method and retry once.

        Grok Build (``grok agent stdio``) rejects ``session/new`` with
        ``Authentication required`` until the client sends ``authenticate``;
        its ``_meta.defaultAuthMethodId`` (``cached_token`` when
        ``~/.grok/auth.json`` holds a token) completes headlessly. If the
        agent advertises only interactive/browser methods — or a malformed
        list with no usable id — raise a clear diagnosis instead of attempting
        a login that cannot succeed on a headless host. With no advertised
        methods at all, return the original error untouched.
        """
        method_id = _unattended_auth_method_id(self._auth_advertisement)
        if method_id is None:
            if self._auth_advertisement.get("authMethods"):
                error = resp["error"]
                message = error.get("message", error) if isinstance(error, dict) else error
                raise RuntimeError(
                    "ACP session/new requires authentication, but the agent "
                    "advertises no headless auth method (browser-only or "
                    f"malformed authMethods): {message}"
                )
            return resp, native_epoch
        await self._authenticate(method_id)
        return await self._new_session_attempt(params)

    async def _new_session_attempt(
        self, params: _AcpJsonObject
    ) -> tuple[_AcpJsonObject, str | None]:
        """Persist a fresh epoch before every native ``session/new`` attempt."""
        sink = self._session_report_sink
        native_epoch = secrets.token_urlsafe(24) if sink is not None else None
        if sink is not None and native_epoch is not None:
            await self._call_report_sink(sink.begin_native_epoch, native_epoch)
        response = await self._rpc(
            _AGENT_METHOD_SESSION_NEW, params, timeout=_INIT_TIMEOUT_SECONDS
        )
        return response, native_epoch

    async def _ensure_session(self) -> str:
        """Create (or reuse) an ACP session, returning the session id.

        In ``server`` mode we send only ``cwd`` + ``mcpServers`` and adopt the id
        the agent returns. In ``client`` mode we generate the id and send it.
        ``mcpServers`` carries Omnigent's builtin tools (via the shared serve-mcp
        relay) unless disabled — see :class:`OmnigentAcpMcp`.
        """
        if self._session_id is not None:
            return self._session_id

        params: _AcpJsonObject = {
            "cwd": self._cwd,
            # ACP requires this field even with no per-session MCP servers; the
            # helper returns [] when Omnigent MCP is disabled.
            "mcpServers": self._session_mcp_servers(),
        }
        client_id: str | None = None
        if self._config.session_id_mode == "client":
            client_id = secrets.token_urlsafe(16)
            params["sessionId"] = client_id
        if self._config.send_model_in_session_new and self._config.model:
            params["model"] = self._config.model

        resp, native_epoch = await self._new_session_attempt(params)
        if "error" in resp and not self._authenticated and _is_auth_required_error(resp["error"]):
            resp, native_epoch = await self._authenticate_and_retry_session_new(
                resp, params, native_epoch
            )
        if "error" in resp:
            raise RuntimeError(
                f"ACP session/new failed: {resp['error'].get('message', resp['error'])}"
            )
        result = resp.get("result", {})
        server_session_id = result.get("sessionId") if isinstance(result, dict) else None
        session_id = server_session_id or client_id
        if not isinstance(session_id, str) or not session_id:
            raise RuntimeError(
                "ACP session/new response missing sessionId: " + json.dumps(resp)[:200]
            )
        if self._session_report_sink is not None:
            if native_epoch is None:
                raise RuntimeError("ACP report session has no durable native epoch")
            await self._call_report_sink(
                self._session_report_sink.bind_native_session,
                native_epoch,
                session_id,
            )
        self._native_epoch = native_epoch
        self._session_id = session_id
        self._native_context_generation = (
            self._active_session_action_origin[3]
            if self._active_session_action_origin is not None
            else None
        )
        self._next_ingress_sequence = 1
        self._report_event_count = 0
        self._next_child_request_invocation_id = 0
        self._standard_associations.clear()
        self._session_translation_contexts.clear()
        self._root_tool_call_scopes.clear()
        if self._standard_subagents_enabled:
            self._standard_associations[session_id] = _StandardSessionAssociation(
                parent_session_id=None,
                association_source_id="",
                origin=self._active_session_action_origin,
            )
            self._session_translation_contexts[session_id] = _SessionTranslationContext()
        # Capture the agent's advertised model from session/new's config options, so a
        # turn's usage can name it. Agents that report the model here (e.g. jcode) would
        # otherwise leave it unknown until a later config_option_update, leaving the
        # server unable to attribute per-model token usage for the turn.
        if isinstance(result, dict):
            self._note_config_options(result.get("configOptions"))
            self._note_session_models(result.get("models"))
        self._initial_model = self._active_model
        return self._session_id

    def _note_session_models(self, models: object) -> None:
        """Record the model catalog an agent returns from ``session/new``.

        Agents advertise model selection one of two ways: a ``model`` session
        config option (see :meth:`_note_config_options`), or a ``models`` object
        here carrying ``availableModels`` and ``currentModelId``. Agents using the
        latter never emit ``config_option_update``, so without this their model
        stays on the agent's own default and a ``/model`` pick is silently
        dropped — which also leaves the agent billing whatever it defaulted to.

        :param models: The ``models`` value from ``session/new``'s result.
        """
        if not isinstance(models, dict):
            return
        available = models.get("availableModels")
        if isinstance(available, list):
            for entry in available:
                if isinstance(entry, dict):
                    model_id = entry.get("modelId")
                    if isinstance(model_id, str) and model_id:
                        self._session_model_ids.add(model_id)
        current = models.get("currentModelId")
        if isinstance(current, str) and current:
            self._active_model = current

    def _session_mcp_servers(self) -> list[_AcpJsonObject]:
        """Build ``session/new.mcpServers`` and snapshot the bridge aliases.

        Relay creation and tool-call classification happen here together so they
        cannot drift apart. No entries means no aliases, so with Omnigent MCP
        disabled every call classifies as agent-native.
        """
        mcp_servers: list[_AcpJsonObject] = []
        if self._config.omnigent_mcp:
            mcp_servers = self._mcp.session_new_servers(
                tools=self._omnigent_tools,
                tool_executor=getattr(self, "_tool_executor", None),
                loop=asyncio.get_event_loop(),
                enabled=True,
            )
        servers = {str(s.get("name", "")) for s in mcp_servers if isinstance(s, dict)}
        servers.discard("")
        tools = {
            name
            for name in (spec.get("name") for spec in (self._omnigent_tools or []))
            if isinstance(name, str) and name
        }
        # Agents name a bridged tool one of several ways; accept every shape we have
        # seen, plus the bare name for agents that report it unprefixed.
        aliases = set(tools) if servers else set()
        for server in servers:
            for tool in tools:
                aliases.update(
                    (f"mcp_{server}_{tool}", f"mcp__{server}__{tool}", f"{server}__{tool}")
                )
        self._bridge_tool_aliases = frozenset(aliases)
        return mcp_servers

    # ------------------------------------------------------------------
    # Server-initiated requests (agent → client)
    # ------------------------------------------------------------------

    async def _respond_to_agent_request(
        self,
        request: _AcpJsonObject,
        *,
        child_cwd: str | None = None,
        child_additional_directories: Sequence[str] | None = None,
        detached_child_action: bool = False,
    ) -> None:
        """Answer a server-initiated ACP request from the agent.

        - ``session/request_permission`` — decide via Omnigent's TOOL_CALL policy
          + human-consent elicitation (:meth:`_decide_permission`), then select
          the matching allow/reject option. NOT a blind approve.
        - ``fs/read_text_file`` / ``fs/write_text_file`` — when fs delegation is
          advertised, execute through the Omnigent OSEnvironment so the spec's
          sandbox read/write roots are enforced. Off → never arrive.
        - anything else — reply with JSON-RPC ``method not found`` so the agent
          fails loudly rather than acting on empty data.
        """
        req_id = request.get("id")
        method = request.get("method", "")
        params = request.get("params", {}) or {}
        logger.debug("acp[%s] agent request: method=%s id=%s", self._config.name, method, req_id)

        result: _AcpJsonObject | None = None
        error: _AcpJsonObject | None = None
        try:
            if method == _AGENT_REQUEST_REQUEST_PERMISSION:
                allow, option_id = await self._decide_permission(params)
                result = self._permission_outcome(params, allow=allow, option_id=option_id)
            elif method == "fs/read_text_file" and self._fs_delegation:
                if detached_child_action:
                    await self._validate_detached_action_origin()
                result = await self._handle_fs_read(
                    params,
                    cwd=child_cwd,
                    child_additional_directories=child_additional_directories,
                )
            elif method == "fs/write_text_file" and self._fs_delegation:
                if detached_child_action:
                    await self._validate_detached_action_origin()
                result = await self._handle_fs_write(
                    params,
                    cwd=child_cwd,
                    child_additional_directories=child_additional_directories,
                )
            else:
                error = {
                    "code": -32601,
                    "message": f"omnigent: unsupported ACP request method {method!r}",
                }
        except _AcpRequestError as exc:
            error = {"code": exc.code, "message": exc.message}
        except Exception as exc:  # noqa: BLE001
            logger.debug("acp[%s] agent request %s failed: %s", self._config.name, method, exc)
            error = {"code": -32603, "message": f"{method} failed: {exc}"}

        reply: _AcpJsonObject = {"jsonrpc": "2.0", "id": req_id}
        if error is not None:
            reply["error"] = error
        else:
            reply["result"] = result
        await self._send(reply)

    async def _validate_detached_action_origin(self) -> None:
        """Revalidate the captured root and child access before detached work."""
        evaluator = self._policy_evaluator
        if evaluator is None:
            raise _AcpRequestError(
                -32603,
                "omnigent: ACP child action validation is unavailable",
            )
        verdict = await evaluator(_ACP_ACTION_ORIGIN_CHECK_PHASE, {})
        if verdict.action != "POLICY_ACTION_ALLOW":
            raise _AcpRequestError(
                -32603,
                "omnigent: ACP child action origin is no longer valid",
            )

    # ------------------------------------------------------------------
    # Filesystem delegation (agent → client, when fs capability advertised)
    # ------------------------------------------------------------------

    async def _ensure_os_environment(self) -> OSEnvironment:
        """Lazily create the OSEnvironment backing fs delegation."""
        if self._os_environment is None:
            env = create_os_environment(self._os_env)
            if env is None:
                raise _AcpRequestError(-32603, "omnigent: no os_env for fs delegation")
            self._os_environment = env
        return self._os_environment

    async def _handle_fs_read(
        self,
        params: _AcpJsonObject,
        *,
        cwd: str | None = None,
        child_additional_directories: Sequence[str] | None = None,
    ) -> _AcpJsonObject:
        """Serve an ACP ``fs/read_text_file`` by reading through the OSEnvironment.

        ACP params ``{path, line?, limit?}`` (1-based start line, max line count;
        both optional → whole file) map onto :meth:`OSEnvironment.read`.
        """
        path = params.get("path")
        if not isinstance(path, str) or not path:
            raise _AcpRequestError(-32602, "fs/read_text_file requires a string 'path'")
        if not Path(path).is_absolute():
            path = str(Path(cwd or self._cwd) / path)
        line = params.get("line")
        limit = params.get("limit")
        offset = line if isinstance(line, int) and line >= 1 else 1
        read_limit = limit if isinstance(limit, int) and limit >= 1 else None

        env = await self._ensure_os_environment()
        if cwd is not None:
            self._assert_detached_fs_path_allowed(
                env,
                path,
                cwd=cwd,
                additional_directories=child_additional_directories,
                need_write=False,
            )
        result = await env.read(path, offset=offset, limit=read_limit)
        if "error" in result:
            message = str(result["error"])
            code = _ACP_RESOURCE_NOT_FOUND_CODE if _looks_like_missing_file(message) else -32603
            raise _AcpRequestError(code, message)
        if result.get("encoding") != "utf-8":
            raise _AcpRequestError(-32603, f"{path}: not a UTF-8 text file")
        return {"content": result.get("content", "")}

    async def _handle_fs_write(
        self,
        params: _AcpJsonObject,
        *,
        cwd: str | None = None,
        child_additional_directories: Sequence[str] | None = None,
    ) -> _AcpJsonObject:
        """Serve an ACP ``fs/write_text_file`` by writing through the OSEnvironment.

        ACP params ``{path, content}``; the write goes through the helper so the
        spec's sandbox write roots are enforced at the Python layer.
        """
        path = params.get("path")
        content = params.get("content")
        if not isinstance(path, str) or not path:
            raise _AcpRequestError(-32602, "fs/write_text_file requires a string 'path'")
        if not isinstance(content, str):
            raise _AcpRequestError(-32602, "fs/write_text_file requires string 'content'")
        if not Path(path).is_absolute():
            path = str(Path(cwd or self._cwd) / path)

        env = await self._ensure_os_environment()
        if cwd is not None:
            self._assert_detached_fs_path_allowed(
                env,
                path,
                cwd=cwd,
                additional_directories=child_additional_directories,
                need_write=True,
            )
        result = await env.write(path, content)
        if "error" in result:
            raise _AcpRequestError(-32603, str(result["error"]))
        return {}

    @staticmethod
    def _assert_detached_fs_path_allowed(
        env: OSEnvironment,
        path: str,
        *,
        cwd: str | None,
        additional_directories: Sequence[str] | None,
        need_write: bool,
    ) -> None:
        """Narrow trusted host file grants to the inherited child directories."""
        sandbox = getattr(env, "sandbox", None)
        if sandbox is None:
            raise _AcpRequestError(-32603, "omnigent: child filesystem policy is unavailable")
        candidate = Path(path).expanduser().resolve(strict=False)
        child_roots = [Path(cwd or env.cwd).expanduser().resolve(strict=False)]
        child_roots.extend(
            Path(directory).expanduser().resolve(strict=False)
            for directory in (additional_directories or ())
        )
        if not any(candidate == root or root in candidate.parents for root in child_roots):
            raise _AcpRequestError(
                -32603,
                f"{path}: blocked by the ACP child filesystem boundary",
            )
        if not any(
            (not need_write or root.access == "write") and root.contains(candidate)
            for root in reachable_roots(env.cwd, sandbox)
        ):
            raise _AcpRequestError(-32603, f"{path}: blocked by the host filesystem policy")

    # ------------------------------------------------------------------
    # Permission (session/request_permission) → policy + elicitation
    # ------------------------------------------------------------------

    def _extract_tool_call(self, params: _AcpJsonObject) -> tuple[str, _AcpJsonObject]:
        """Pull ``(tool_name, tool_input)`` from a ``session/request_permission``.

        ACP's ``toolCall`` carries a human ``title`` (e.g. ``"shell"``), a
        ``kind`` (e.g. ``"execute"``), and a ``rawInput`` dict; prefer the title,
        else the kind. An agent may instead send the request bare, carrying only
        ``toolCallId``, so fall back to what the originating ``tool_call`` update
        reported for that id — it always arrives first. Without that fallback a
        bare request degrades to ``"tool"`` with no arguments, so the approval
        card cannot say what is about to run and no TOOL_CALL policy rule can
        match it (rules gate on the tool name, then read its arguments).

        (Vendor-specific ``_meta`` tool names — e.g. Goose's
        ``_meta.goose.toolCall.toolName`` — are not read here; ``title`` is the
        portable name and ``toolCallId`` the portable correlation.)
        """
        tool_call = params.get("toolCall") or {}
        call_id = tool_call.get("toolCallId")
        call_id = call_id if isinstance(call_id, str) else None
        session_id = params.get("sessionId")
        session_context = (
            self._session_translation_contexts.get(session_id)
            if isinstance(session_id, str)
            else None
        )
        cached_name = None
        cached_args = None
        if call_id:
            if session_context is not None:
                cached_name = session_context.tool_names.get(call_id)
                cached_args = session_context.tool_inputs.get(call_id)
            else:
                cached_name = self._tool_names.get(call_id)
                cached_args = self._tool_inputs.get(call_id)
        name = tool_call.get("title") or tool_call.get("kind") or cached_name or "tool"
        args = tool_call.get("rawInput")
        if not isinstance(args, dict):
            args = cached_args if isinstance(cached_args, dict) else {}
        return str(name), args

    @property
    def _bypass_permissions(self) -> bool:
        """Whether the user opted out of approval cards for this agent.

        Mirrors :class:`~omnigent.inner.claude_sdk_executor.ClaudeSDKExecutor`'s
        ``can_use_tool`` stance: ``"bypassPermissions"`` and nothing else, so the ``"auto"``
        default keeps prompting. (Cursor also treats ``"auto"`` as no-prompt;
        ACP agents ask only about actions they consider permission-worthy, so
        silencing the default would drop meaningful prompts.)
        """
        return self._config.permission_mode == "bypassPermissions"

    @staticmethod
    def _permission_options(params: _AcpJsonObject) -> list[_AcpJsonObject]:
        """The agent's offered options, each an ``{optionId, name, kind}`` dict."""
        return [o for o in (params.get("options") or []) if isinstance(o, dict)]

    def _scoped_options(self, params: _AcpJsonObject) -> list[tuple[str, _AcpJsonObject]] | None:
        """Label the agent's options for a choice card, or ``None`` if unusable.

        Unusable means: fewer than two options, a blank or duplicated label (the
        reply names the label, so duplicates are ambiguous), or no ``reject_*``
        option — a choice card replaces the Approve/Reject buttons, so without one
        the user would have no way to say no.
        """
        labeled = [(str(o.get("name") or "").strip(), o) for o in self._permission_options(params)]
        if len(labeled) < 2 or any(not name for name, _ in labeled):
            return None
        labels = [name for name, _ in labeled]
        if len(set(labels)) != len(labels):
            return None
        if not any("reject" in str(o.get("kind", "")) for _, o in labeled):
            return None
        return labeled

    async def _ask_user(
        self, tool_name: str, tool_input: _AcpJsonObject, params: _AcpJsonObject
    ) -> tuple[bool, str | None]:
        """Route a permission request to the user; return ``(allowed, option_id)``.

        Prefers the choice bridge, which puts the agent's *own* options on the
        card: picking "allow this command for the session" is one click the agent
        then honors itself, so the same command class stops re-prompting. Falls
        back to the yes/no bridge, whose grant stays once-scoped.
        """
        choice_handler = self._elicitation_choice_handler
        labeled = self._scoped_options(params) if choice_handler is not None else None
        if choice_handler is not None and labeled is not None:
            chosen = await choice_handler(tool_name, tool_input, [name for name, _ in labeled])
            if chosen is None:
                return False, None
            picked = next((o for name, o in labeled if name == chosen), None)
            if picked is None:
                logger.warning(
                    "acp permission choice %r was not offered; denying tool=%s", chosen, tool_name
                )
                return False, None
            option_id = picked.get("optionId")
            allowed = "allow" in str(picked.get("kind", ""))
            logger.info(
                "acp permission %s by user (scope=%s): tool=%s",
                "allowed" if allowed else "denied",
                option_id,
                tool_name,
            )
            return allowed, (option_id if isinstance(option_id, str) else None)

        handler = self._elicitation_handler
        if handler is None:
            return False, None
        return bool(await handler(tool_name, tool_input)), None

    async def _decide_permission(self, params: _AcpJsonObject) -> tuple[bool, str | None]:
        """Decide a permission request — policy then elicitation.

        1. **TOOL_CALL policy** (:attr:`_policy_evaluator`): a hard
           ``POLICY_ACTION_DENY`` denies; ``POLICY_ACTION_ASK`` defers to
           elicitation (and **fails closed** when no handler is wired);
           ``ALLOW`` / unspecified falls through.
        2. **Human-consent elicitation**: the agent's own options via
           :attr:`_elicitation_choice_handler`, else a yes/no card via
           :attr:`_elicitation_handler`. Skipped under
           ``permission_mode="bypassPermissions"`` — but only for a request no
           policy had an opinion on, so a DENY still blocks and a policy that
           says ASK still prompts.

        When neither bridge is wired (standalone / unit tests), falls back to
        allow so direct use of the executor isn't blocked. In normal runner
        operation the adapter installs both, so destructive actions are gated.

        :returns: ``(allowed, option_id)`` — *option_id* is the scope the user
            picked from the agent's options, or ``None`` to let
            :meth:`_permission_outcome` choose the narrowest grant.
        """
        tool_name, tool_input = self._extract_tool_call(params)
        policy_eval = getattr(self, "_policy_evaluator", None)
        # Either bridge can carry the question to the user.
        can_ask = (
            self._elicitation_handler is not None or self._elicitation_choice_handler is not None
        )

        if policy_eval is not None:
            action: str | None
            try:
                verdict = await policy_eval(
                    "PHASE_TOOL_CALL", {"name": tool_name, "arguments": tool_input}
                )
                action = getattr(verdict, "action", None)
            except Exception as exc:  # noqa: BLE001 — fail open to elicitation
                logger.warning("acp TOOL_CALL policy eval failed for %s: %s", tool_name, exc)
                action = None
            if action == "POLICY_ACTION_DENY":
                logger.info("acp permission denied by policy: tool=%s", tool_name)
                return False, None
            if action == "POLICY_ACTION_ASK":
                if not can_ask:
                    logger.warning(
                        "acp TOOL_CALL policy ASK with no elicitation handler; denying tool=%s",
                        tool_name,
                    )
                    return False, None
                return await self._ask_user(tool_name, tool_input, params)
            # ALLOW / UNSPECIFIED / unknown → fall through to elicitation.

        if can_ask and not self._bypass_permissions:
            return await self._ask_user(tool_name, tool_input, params)
        if can_ask:
            # bypassPermissions: no policy had an opinion and the user asked not
            # to be prompted. Logged at info so the audit trail still names what
            # ran unreviewed. Answered per-request (never the agent's own bypass
            # option) so every later call stays visible to policy.
            logger.info("acp permission allowed (bypassPermissions): tool=%s", tool_name)
            return True, None

        logger.debug("acp permission allowed (no policy/elicitation wired): tool=%s", tool_name)
        return True, None

    @staticmethod
    def _permission_outcome(
        params: _AcpJsonObject, *, allow: bool, option_id: str | None = None
    ) -> _AcpJsonObject:
        """Map a decision to an ACP permission ``outcome``.

        *option_id* is a scope the user picked from the agent's own options; it is
        echoed only after confirming the agent offered it, so we never send an id
        it doesn't know. Without one: on allow prefer a once-scoped grant
        (``allow_once``) over ``allow_always``, so a blanket "always allow" is
        only ever sent because the user chose it; on deny pick a ``reject_*``
        option, or ``cancelled`` when none is offered. The agent's options carry
        both ``optionId`` and ``kind`` (e.g. ``allow_once``).
        """
        options = AcpExecutor._permission_options(params)
        if option_id is not None and any(o.get("optionId") == option_id for o in options):
            return {"outcome": {"outcome": "selected", "optionId": option_id}}

        def _pick(*kinds: str) -> _AcpJsonObject | None:
            for kind in kinds:
                for opt in options:
                    if opt.get("kind") == kind:
                        return opt
            return None

        if allow:
            chosen = _pick("allow_once", "allow_always") or next(
                (o for o in options if "allow" in str(o.get("kind", ""))), None
            )
        else:
            chosen = _pick("reject_once", "reject_always") or next(
                (o for o in options if "reject" in str(o.get("kind", ""))), None
            )
        if chosen is None:
            return {"outcome": {"outcome": "cancelled"}}
        return {"outcome": {"outcome": "selected", "optionId": chosen.get("optionId")}}

    # ------------------------------------------------------------------
    # Prompt building
    # ------------------------------------------------------------------

    @staticmethod
    def _image_blocks_from_content(content: object) -> list[_AcpJsonObject]:
        """Build ACP ``image`` prompt blocks from a message's ``input_image`` blocks."""
        out: list[_AcpJsonObject] = []
        if not isinstance(content, list):
            return out
        for block in content:
            if not isinstance(block, dict) or block.get("type") != "input_image":
                continue
            parsed = _parse_image_data_uri(block.get("image_url") or block.get("file_data"))
            if parsed:
                mime, data = parsed
                out.append({"type": "image", "mimeType": mime, "data": data})
        return out

    @staticmethod
    def _text_from_blocks(
        blocks: list[object],
        *,
        emit_image_marker: bool = False,
    ) -> str:
        """Extract prompt text from a Responses-API content-block list.

        ACP's ``session/prompt`` text part is plain text, so each block is folded:
        ``input_text``/``output_text``/``text`` verbatim; ``input_file`` inlined
        (fenced) when the runner resolved it to a text data URI, else a marker;
        ``input_image`` as a marker only when *emit_image_marker* is set.
        """
        parts: list[str] = []
        for block in blocks:
            if not isinstance(block, dict):
                continue
            btype = block.get("type")
            if btype in ("input_text", "output_text", "text"):
                text = block.get("text")
                if isinstance(text, str) and text:
                    parts.append(text)
            elif btype == "input_file":
                name = block.get("filename") or block.get("file_id") or "file"
                inlined = _inline_text_file_data(block.get("file_data"))
                if inlined:
                    parts.append(
                        f"--- attached file: {name} ---\n{inlined}\n--- end of {name} ---"
                    )
                else:
                    parts.append(f"[attached file: {name}]")
            elif btype == "input_image" and emit_image_marker:
                name = block.get("filename") or block.get("file_id")
                parts.append(f"[attached image: {name}]" if name else "[attached image]")
        return "\n".join(parts)

    @classmethod
    def _history_prefix(cls, prior: Sequence[object]) -> str:
        """Serialize prior conversation turns into a text prefix.

        On a *fresh* ACP session (the first turn of a newly spawned/respawned
        process, or after a session reset) the agent holds none of the earlier
        conversation. Since :meth:`run_turn` normally sends only the latest user
        turn, we'd lose everything before the switch. Replaying the transcript as
        a labeled ``role: content`` block restores that context.
        """
        lines = ["Conversation so far:"]
        for msg in prior:
            if not isinstance(msg, dict):
                continue
            role = str(msg.get("role", "user")).replace("_", " ")
            raw = msg.get("content")
            if raw is None:
                content = ""
            elif isinstance(raw, str):
                content = raw
            elif isinstance(raw, list):
                content = cls._text_from_blocks(raw, emit_image_marker=True)
            else:
                content = json.dumps(raw, ensure_ascii=True)
            lines.append(f"{role}: {content}")
        lines.append("")
        lines.append(
            "Respond to the latest user message, using the conversation above as context."
        )
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Executor interface
    # ------------------------------------------------------------------

    def handles_tools_internally(self) -> bool:
        """True — the ACP agent runs its own tool loop.

        The Session must NOT re-execute the ``ToolCallRequest`` /
        ``ToolCallComplete`` events we emit from ``tool_call`` updates; they are
        informational (they render tool cards showing what the agent did).
        """
        return True

    def supports_streaming(self) -> bool:
        return True

    def preserves_empty_initiating_message(self) -> bool:
        """Keep the initiating item identity even when its ACP prompt is empty."""
        return True

    def max_context_tokens(self) -> int | None:
        """Return the agent's reported context-window size, if observed yet."""
        return self._context_window

    @staticmethod
    def _usage_from_result(result: _AcpJsonObject) -> dict[str, Any] | None:
        """Map an agent's final ``result.usage`` to Omnigent's usage keys.

        ACP does not standardize usage, but agents that report it (Goose, Devin,
        jcode) use ``{totalTokens, inputTokens, outputTokens}`` plus optional
        ``cachedReadTokens`` / ``cachedWriteTokens``; Omnigent's ``TurnComplete.usage``
        uses ``{input_tokens, output_tokens, total_tokens, cache_read_input_tokens,
        cache_creation_input_tokens}``. Absent → ``None`` (usage simply isn't shown for
        agents that don't report).

        ``cachedReadTokens`` is kept as its own key rather than folded into
        ``input_tokens``: cache reads are real consumption but billed at a
        fraction of the rate, so collapsing them would overstate cost — in one
        measured turn 10,944 of 15,637 input tokens were cache reads.
        ``cache_read_input_tokens`` is the key the rest of the stack already
        speaks, so it renders without any UI change.
        """
        usage = result.get("usage")
        if not isinstance(usage, dict):
            return None
        out: dict[str, Any] = {}
        for acp_key, omni_key in (
            ("inputTokens", "input_tokens"),
            ("outputTokens", "output_tokens"),
            ("totalTokens", "total_tokens"),
            ("cachedReadTokens", "cache_read_input_tokens"),
            ("cachedWriteTokens", "cache_creation_input_tokens"),
        ):
            value = usage.get(acp_key)
            if isinstance(value, int) and not isinstance(value, bool):
                out[omni_key] = value
        return out or None

    def _usage_with_active_model(self, result: _AcpJsonObject) -> dict[str, Any] | None:
        """Usage from the result, tagged with the active model for cost attribution.

        ACP ``result.usage`` carries token counts but no model id, so the server
        cannot attribute a turn's tokens to a model — leaving its per-model usage
        view (``usage_by_model``) empty and the counts unrendered in the UI. Stamp
        the agent's active model (its ``model`` config option, captured at
        ``session/new``) so the tokens are attributed, mirroring how codex stamps
        ``usage["model"]``. The stamp is skipped when the model is unknown or the
        agent already reported one on the result.

        :param result: The ACP ``session/prompt`` result object.
        :returns: The usage dict (with ``model`` when known), or ``None``.
        """
        usage = self._usage_from_result(result)
        if usage is not None and self._active_model and "model" not in usage:
            usage["model"] = self._active_model
        return usage

    def _is_bridge_tool_call(self, name: str, update: _AcpJsonObject) -> bool:
        """True when this tool call reaches Omnigent through the MCP bridge.

        Candidates are the reported name plus the machine names some agents carry
        beside a humanized title. An unrecognized shape reads as agent-native,
        which may duplicate a card but cannot corrupt dispatch correlation.
        """
        if not self._bridge_tool_aliases:
            return False
        raw = update.get("rawInput")
        raw_tool = str(raw.get("tool", "")) if isinstance(raw, dict) else ""
        # Goose reports the machine name under ``_meta.goose.toolCall.toolName``;
        # older builds used the flatter ``_meta.goose.toolName``. Read both.
        meta = update.get("_meta")
        goose = meta.get("goose") if isinstance(meta, dict) else None
        meta_tool = ""
        if isinstance(goose, dict):
            inner = goose.get("toolCall")
            meta_tool = str(
                (inner.get("toolName", "") if isinstance(inner, dict) else "")
                or goose.get("toolName", "")
            )
        candidates = {c for c in (name, raw_tool, meta_tool) if c}
        return not self._bridge_tool_aliases.isdisjoint(candidates)

    def _reset_session_state(self) -> None:
        """Forget state that is only valid for the current ACP session."""
        self._session_id = None
        self._active_prompt_request_id = None
        self._active_prompt_response_received = False
        self._native_epoch = None
        self._native_context_generation = None
        self._next_ingress_sequence = 1
        self._report_event_count = 0
        self._next_child_request_invocation_id = 0
        self._standard_associations.clear()
        self._session_translation_contexts.clear()
        self._root_tool_call_scopes.clear()
        self._initial_model = None
        self._system_prompt_sent = False
        self._tool_names.clear()
        self._tool_inputs.clear()
        self._bridge_tool_aliases = frozenset()

    def _handle_session_update(self, update: _AcpJsonObject) -> list[ExecutorEvent]:
        """Translate one ``session/update`` payload into ExecutorEvents.

        Returns the events to yield (usually 0 or 1). Side effects: records the
        context window from ``usage_update`` and tracks tool-call names so a
        later ``tool_call_update`` can close the right card.
        """
        update_type = update.get("sessionUpdate", "")
        events: list[ExecutorEvent] = []

        # Sub-agent frames ride a per-agent dialect (Devin: cognition.ai/* on the
        # ``_meta``); a source claims them. When one does, the frame belongs to a
        # *child* session, not the parent stream — so emit the child-directed
        # events and stop. The tool-card branches below would otherwise render the
        # sub-agent's own work in the parent, and a lifecycle ``tool_call_update``
        # (whose id was never an originating ``tool_call``) would close a spurious
        # "tool" card there. Empty for a generic ACP agent (no sources), so this
        # is a no-op for every non-Devin harness.
        sub_events = read_subagent_events(update, self._extension.subagent_sources)
        if sub_events:
            for sub in sub_events:
                if isinstance(sub, SubAgentStart):
                    events.append(
                        SubAgentStarted(child_key=sub.child_key, title=sub.title, task=sub.task)
                    )
                elif isinstance(sub, SubAgentActivity):
                    events.append(
                        SubAgentToolCall(
                            child_key=sub.child_key,
                            call_id=sub.call_id,
                            name=sub.name,
                            args=dict(sub.args),
                        )
                    )
                else:  # SubAgentEnd
                    events.append(
                        SubAgentCompleted(child_key=sub.child_key, ok=sub.ok, summary=sub.summary)
                    )
            return events

        if update_type == _UPDATE_AGENT_MESSAGE_CHUNK:
            content = update.get("content", {})
            text = content.get("text", "") if isinstance(content, dict) else ""
            if text:
                events.append(TextChunk(text=text))
        elif update_type == _UPDATE_AGENT_THOUGHT_CHUNK:
            content = update.get("content", {})
            text = content.get("text", "") if isinstance(content, dict) else ""
            if text:
                events.append(ReasoningChunk(delta=text, event_type="reasoning_text"))
        elif update_type == _UPDATE_TOOL_CALL:
            call_id = update.get("toolCallId")
            name = update.get("title") or update.get("kind") or "tool"
            raw_input = update.get("rawInput")
            args = raw_input if isinstance(raw_input, dict) else {}
            if isinstance(call_id, str) and call_id:
                status = update.get("status")
                terminal = status in {_TOOL_STATUS_COMPLETED, _TOOL_STATUS_FAILED}
                self._tool_names[call_id] = str(name)
                self._tool_inputs[call_id] = args
                metadata: _AcpJsonObject = {"call_id": call_id}
                scope_metadata = self._root_tool_call_scope_metadata(call_id)
                if scope_metadata is not None:
                    metadata[ACP_TOOL_CALL_SCOPE_METADATA_KEY] = scope_metadata
                # An agent-native tool ran inside the agent's own loop and never
                # round-trips Omnigent dispatch. Leaving its id in the correlation
                # queue would mis-pair the next bridge completion.
                if not self._is_bridge_tool_call(str(name), update):
                    metadata["internally_executed"] = True
                if isinstance(status, str):
                    metadata["tool_status"] = status
                if terminal:
                    metadata["observed_call_completed"] = True
                    self._tool_names.pop(call_id, None)
                    self._tool_inputs.pop(call_id, None)
                events.append(ToolCallRequest(name=str(name), args=args, metadata=metadata))
                if terminal:
                    completion_metadata: _AcpJsonObject = {"call_id": call_id}
                    if scope_metadata is not None:
                        completion_metadata[ACP_TOOL_CALL_SCOPE_METADATA_KEY] = scope_metadata
                    completion_metadata["tool_status"] = status
                    if self._standard_subagents_enabled:
                        completion_metadata[ACP_TOOL_OUTPUT_METADATA_KEY] = (
                            self._standard_tool_output_metadata(update)
                        )
                    result = (
                        update.get("content") if "content" in update else update.get("rawOutput")
                    )
                    events.append(
                        ToolCallComplete(
                            name=str(name),
                            status=(
                                ToolCallStatus.SUCCESS
                                if status == _TOOL_STATUS_COMPLETED
                                else ToolCallStatus.ERROR
                            ),
                            result=result,
                            metadata=completion_metadata,
                        )
                    )
        elif update_type == _UPDATE_TOOL_CALL_UPDATE:
            call_id = update.get("toolCallId")
            status = update.get("status")
            if isinstance(call_id, str) and status in (
                _TOOL_STATUS_COMPLETED,
                _TOOL_STATUS_FAILED,
            ):
                name = self._tool_names.pop(call_id, "tool")
                self._tool_inputs.pop(call_id, None)
                metadata: _AcpJsonObject = {"call_id": call_id}
                scope_metadata = self._root_tool_call_scope_metadata(call_id)
                if scope_metadata is not None:
                    metadata[ACP_TOOL_CALL_SCOPE_METADATA_KEY] = scope_metadata
                metadata["tool_status"] = status
                if self._standard_subagents_enabled:
                    metadata[ACP_TOOL_OUTPUT_METADATA_KEY] = self._standard_tool_output_metadata(
                        update
                    )
                events.append(
                    ToolCallComplete(
                        name=name,
                        status=(
                            ToolCallStatus.SUCCESS
                            if status == _TOOL_STATUS_COMPLETED
                            else ToolCallStatus.ERROR
                        ),
                        result=(
                            update.get("content")
                            if "content" in update
                            else update.get("rawOutput")
                        ),
                        metadata=metadata,
                    )
                )
        elif update_type == _UPDATE_USAGE:
            size = update.get("size")
            if isinstance(size, int) and size > 0:
                self._context_window = size

        elif update_type == _UPDATE_CONFIG_OPTION:
            self._note_config_options(update.get("configOptions"))

        return events

    def _note_config_options(self, options: object) -> str | None:
        """Record which session config options the agent exposes, and their values.

        ACP agents advertise settable options (``mode``, ``model``, …) via
        ``config_option_update``. Tracking the ids tells us whether a warm model
        switch is possible at all; tracking ``model``'s ``currentValue`` is the
        only trustworthy record of which model is live — an agent's own
        self-report is unreliable.

        :returns: The echoed ``model`` ``currentValue`` when the payload carried a
            model option, else ``None`` — lets a caller distinguish "the agent
            reported its model" from "no model option present".
        """
        if not isinstance(options, list):
            return None
        model_option: _AcpJsonObject | None = None
        for opt in options:
            if not isinstance(opt, dict):
                continue
            opt_id = opt.get("id")
            if not isinstance(opt_id, str):
                continue
            self._config_option_ids.add(opt_id)
            # An option literally named ``model`` wins; otherwise the first one
            # in the model category, so several matches resolve deterministically.
            if opt_id == _CONFIG_OPTION_MODEL or (
                model_option is None and opt.get("category") == _CONFIG_OPTION_MODEL
            ):
                model_option = opt
        if model_option is None:
            return None
        self._model_config_option_id = model_option["id"]
        current = model_option.get("currentValue")
        if not isinstance(current, str) or not current:
            return None
        self._active_model = current
        return current

    async def _apply_model_override(self, session_id: str, model: str | None) -> None:
        """Warm-switch the agent's model via ACP ``session/set_config_option``.

        Standard ACP (not a vendor extension), so this works for any agent that
        exposes a ``model`` session config option. The transcript is preserved —
        the session is not recreated — so a ``/model`` pick mid-conversation keeps
        the history the agent has already built up.

        Curated sessions fail the turn if the requested model cannot be selected.
        Uncurated sessions retain the fallback to the current model and disable
        unsupported switching for this process.

        :param session_id: The live ACP session to reconfigure.
        :param model: Requested model id, or ``None`` to restore the configured default.
        """
        # A launch override must not replace the default restored by later turns.
        default_model = self._config.default_model
        if default_model is None:
            default_model = self._config.model
        elif not default_model:
            default_model = self._initial_model
        model = model or default_model
        if not model:
            return
        available = getattr(self._config, "available_models", ())
        if available and model not in available:
            raise _AcpModelSwitchError(
                f"Model {model!r} is not in this ACP agent's configured model list."
            )
        if model == self._active_model:
            return
        if not self._model_switch_supported:
            if available:
                raise _AcpModelSwitchError(f"ACP agent cannot switch to model {model!r}.")
            return
        # An agent that returned a model catalog from ``session/new`` selects
        # models with ``session/set_model`` and never advertises a ``model``
        # config option, so the config-option path below would bail out and leave
        # it on its own default. Take the catalog route first when we have one.
        if self._session_model_ids:
            await self._set_model_via_catalog(session_id, model)
            return
        # Before the first config update, the RPC response determines support.
        if self._config_option_ids and self._model_config_option_id is None:
            if available:
                raise _AcpModelSwitchError(
                    f"ACP agent exposes no model option for switching to {model!r}."
                )
            self._model_switch_supported = False
            logger.info(
                "acp[%s] agent exposes no %r config option; leaving model as-is",
                self._config.name,
                _CONFIG_OPTION_MODEL,
            )
            return

        response = await self._rpc(
            _AGENT_METHOD_SET_CONFIG_OPTION,
            {
                "sessionId": session_id,
                "configId": self._model_config_option_id or _CONFIG_OPTION_MODEL,
                "value": model,
            },
        )
        if "error" in response:
            if available:
                raise _AcpModelSwitchError(
                    f"ACP agent rejected model {model!r}: "
                    f"{response['error'].get('message', response['error'])}"
                )
            self._model_switch_supported = False
            logger.warning(
                "acp[%s] model switch to %s rejected (%s); continuing on the current model",
                self._config.name,
                model,
                response["error"].get("message", response["error"]),
            )
            return
        # The agent echoes its options back; trust the echoed ``currentValue``
        # over our request, since an agent may normalize or silently reject the
        # id. Fall back to the requested model only when the agent echoed no model
        # option at all (some accept the switch without echoing options) — never
        # overwrite an echoed value with the request, or a later turn would skip a
        # switch it should retry.
        result = response.get("result")
        echoed_model = (
            self._note_config_options(result.get("configOptions"))
            if isinstance(result, dict)
            else None
        )
        if echoed_model is None:
            self._active_model = model
        elif available and echoed_model != model:
            raise _AcpModelSwitchError(
                f"ACP agent selected {echoed_model!r} instead of requested model {model!r}."
            )
        logger.info(
            "acp[%s] model set to %s (transcript kept)", self._config.name, self._active_model
        )

    async def _set_model_via_catalog(self, session_id: str, model: str) -> None:
        """Warm-switch the model with ``session/set_model``.

        Used for agents that advertise a catalog in ``session/new`` rather than a
        ``model`` config option. Like the config-option path this preserves the
        transcript. A rejection fails curated turns; uncurated sessions retain
        their current model and disable further switch attempts.

        The requested id is *not* checked against ``availableModels``: that list
        is what the agent offers interactively, and an id outside it can still be
        valid — Cline accepts its subscription-scoped ``cline-pass/*`` ids, which
        it does not enumerate. An unusable id comes back as an RPC error, which
        is the authoritative answer.

        :param session_id: The live ACP session to reconfigure.
        :param model: Requested model id.
        """
        response = await self._rpc(
            _AGENT_METHOD_SET_MODEL, {"sessionId": session_id, "modelId": model}
        )
        if "error" in response:
            if self._config.available_models:
                raise _AcpModelSwitchError(
                    f"ACP agent rejected model {model!r}: "
                    f"{response['error'].get('message', response['error'])}"
                )
            self._model_switch_supported = False
            logger.warning(
                "acp[%s] %s to %s rejected (%s); continuing on the current model",
                self._config.name,
                _AGENT_METHOD_SET_MODEL,
                model,
                response["error"].get("message", response["error"]),
            )
            return
        self._active_model = model
        logger.info(
            "acp[%s] model set to %s via %s (transcript kept)",
            self._config.name,
            model,
            _AGENT_METHOD_SET_MODEL,
        )

    async def _drain_stale_queue(self) -> None:
        """Drain items queued before the next prompt without losing what they carry.

        Leftover agent requests are answered; a queued ``config_option_update``
        (agents often report their model right after ``session/new``) is noted
        rather than lost, so the model it names and its option id are current.
        """
        while not self._queue.empty():
            try:
                stale = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if not isinstance(stale, dict):
                continue
            if stale.get("method") == _CLIENT_NOTIFICATION_SESSION_UPDATE:
                params = stale.get("params")
                update = params.get("update") if isinstance(params, dict) else None
                if (
                    isinstance(update, dict)
                    and update.get("sessionUpdate") == _UPDATE_CONFIG_OPTION
                ):
                    self._note_config_options(update.get("configOptions"))
            elif stale.get("id") is not None and stale.get("method"):
                await self._respond_to_agent_request(stale)

    async def run_turn(
        self,
        messages: list[Message],
        tools: list[ToolSpec],
        system_prompt: str,
        config: ExecutorConfig | None = None,
    ) -> AsyncIterator[ExecutorEvent]:
        """Run one turn of the agent loop via ACP.

        Sends ``session/prompt`` and yields streaming events (text, reasoning,
        tool-call cards) as the agent works, answering any
        ``session/request_permission`` mid-turn, until the final response
        (``stopReason``) arrives — then yields ``TurnComplete`` with usage.

        Every prompt carries ``_meta["ai.omnigent/inputItemId"]``: the persisted
        user item delivered by this turn, or null for synthetic/tool recovery.

        ``tools`` (Omnigent's builtin tool schemas) are captured for the Omnigent
        MCP relay set up at ``session/new`` — the agent still runs its OWN tools.
        When ``omnigent_mcp`` is ``False`` the relay is disabled, so the schemas
        serve no purpose and are discarded immediately rather than stored.
        """
        if self._report_failure is not None:
            yield ExecutorError(
                message=(
                    "ACP session reporting stopped after a durable delivery failure: "
                    f"{describe_exception(self._report_failure)}"
                ),
                retryable=False,
            )
            return
        # Only keep builtin tools when the relay is enabled — they exist solely
        # to back the MCP relay. Storing them when the relay is disabled would
        # let stale data accidentally reach the session/prompt path (#4917).
        self._omnigent_tools = (tools or []) if self._config.omnigent_mcp else []
        try:
            if self._proc is None or self._proc.returncode is not None:
                await self._start_process()
            await self._ensure_initialized()
            session_id = await self._ensure_session()
        except Exception as exc:  # noqa: BLE001
            yield ExecutorError(message=self._startup_error_message(exc), retryable=False)
            return

        # Apply a ``/model`` pick to the live session before prompting, so the
        # switch takes effect on this turn with the transcript intact. Drain first:
        # a report queued behind ``session/new`` names the option id to target.
        requested_model = config.model if config is not None else None
        await self._drain_stale_queue()
        try:
            await self._apply_model_override(session_id, requested_model)
        except Exception as exc:  # noqa: BLE001
            if self._config.available_models:
                if not isinstance(exc, _AcpModelSwitchError):
                    # A transport failure leaves the actual selection uncertain.
                    self._active_model = None
                yield ExecutorError(
                    message=f"ACP model selection failed: {describe_exception(exc)}. "
                    "No prompt was sent; retry or select another configured model.",
                    retryable=True,
                    preserve_session=(
                        isinstance(exc, (_AcpModelSwitchError, TimeoutError))
                        and self._proc is not None
                        and self._proc.returncode is None
                        and self._reader_task is not None
                        and not self._reader_task.done()
                    ),
                )
                return
            self._model_switch_supported = False
            logger.warning("acp[%s] model switch failed: %s", self._config.name, exc)

        # A fresh ACP session holds no prior context. Captured before the latch
        # flips so we know whether to replay history into this turn.
        fresh_session = not self._system_prompt_sent

        user_text = ""
        image_blocks: list[_AcpJsonObject] = []
        latest_user_idx: int | None = None
        candidate_id = messages[-1].get("input_item_id") if messages else None
        input_item_id = candidate_id if isinstance(candidate_id, str) else None
        for idx in range(len(messages) - 1, -1, -1):
            msg = messages[idx]
            role = msg.get("role", "") if isinstance(msg, dict) else ""
            if role == "user":
                latest_user_idx = idx
                content = msg.get("content", "") if isinstance(msg, dict) else ""
                if isinstance(content, str):
                    user_text = content
                elif isinstance(content, list):
                    if self._image_supported:
                        image_blocks = self._image_blocks_from_content(content)
                    user_text = self._text_from_blocks(
                        content, emit_image_marker=not self._image_supported
                    )
                break

        # On a fresh session, replay prior conversation so a subprocess restart
        # (after the agent process exits) or a session reset doesn't drop the
        # thread. A ``/model`` switch no longer respawns — it reconfigures the
        # live session (see ``_apply_model_override``) — so it never reaches here.
        if fresh_session and latest_user_idx is not None and latest_user_idx > 0:
            history_prefix = self._history_prefix(messages[:latest_user_idx])
            user_text = f"{history_prefix}\n\nuser: {user_text}" if user_text else history_prefix

        # ACP has no system-prompt field. When inject_system_prompt is enabled
        # (the default), fold the Omnigent system prompt into the first turn so
        # the agent's model sees the spec instructions. When disabled (e.g. for
        # Pi forks like omp that fully own their own system prompt), skip the
        # injection to avoid prepending text that confuses the agent's internal
        # Claude model into emitting XML tool-call fragments (#4917).
        if fresh_session:
            if system_prompt and self._config.inject_system_prompt:
                user_text = f"{system_prompt}\n\n{user_text}" if user_text else system_prompt
            self._system_prompt_sent = True

        prompt_blocks: list[_AcpJsonObject] = []
        if user_text or not image_blocks:
            prompt_blocks.append({"type": "text", "text": user_text})
        prompt_blocks.extend(image_blocks)

        # Drain stale items from a prior turn, plus anything the switch above queued.
        await self._drain_stale_queue()

        self._rpc_id += 1
        req_id = self._rpc_id
        loop = asyncio.get_event_loop()
        fut: asyncio.Future[_AcpJsonObject] = loop.create_future()
        self._pending[req_id] = fut
        self._active_prompt_request_id = req_id
        self._active_prompt_response_received = False
        self._cancel_grace_deadline = None

        await self._send(
            {
                "jsonrpc": "2.0",
                "id": req_id,
                "method": _AGENT_METHOD_SESSION_PROMPT,
                "params": {
                    "sessionId": session_id,
                    "prompt": prompt_blocks,
                    "_meta": {_META_INPUT_ITEM_ID: input_item_id},
                },
            }
        )
        self._report_prompt_started = True

        deadline = loop.time() + _PROMPT_TIMEOUT_SECONDS
        accumulated_text: list[str] = []

        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                yield ExecutorError(message="Timeout waiting for ACP response", retryable=True)
                return

            # Complete only once the future is resolved AND the queue is drained,
            # so trailing chunks aren't truncated.
            if fut.done() and self._queue.empty():
                try:
                    response = fut.result()
                except Exception as exc:
                    logger.exception("ACP process response retrieval failed")
                    self._reset_session_state()
                    yield ExecutorError(message=f"ACP process error: {exc}", retryable=True)
                    return
                if "error" in response:
                    error_msg = response["error"].get("message", "Unknown ACP error")
                    if "Session not found" in error_msg:
                        self._reset_session_state()
                    elif self._active_prompt_request_id == req_id:
                        self._active_prompt_request_id = None
                        self._active_prompt_response_received = False
                    yield ExecutorError(message=error_msg, retryable=True)
                    return
                result = response.get("result", {}) if isinstance(response, dict) else {}
                usage = self._usage_with_active_model(result) if isinstance(result, dict) else None
                if self._active_prompt_request_id == req_id:
                    self._active_prompt_request_id = None
                    self._active_prompt_response_received = False
                yield TurnComplete(response="".join(accumulated_text), usage=usage)
                return

            try:
                notification = await asyncio.wait_for(
                    self._queue.get(), timeout=min(remaining, 2.0)
                )
            except TimeoutError:
                continue

            method = notification.get("method", "")
            params = notification.get("params", {})

            if notification.get("type") == "error":
                yield ExecutorError(
                    message=(
                        f"ACP stdout reader failed: {notification.get('message', 'unknown error')}"
                    ),
                    retryable=False,
                )
                return

            if method == _CLIENT_NOTIFICATION_SESSION_UPDATE:
                update = params.get("update", {})
                for event in self._handle_session_update(update):
                    if isinstance(event, TextChunk):
                        accumulated_text.append(event.text)
                    yield event
            elif notification.get("id") is not None and notification.get("method"):
                # Server-initiated request (session/request_permission / fs/*):
                # routes through policy + elicitation. Blocks while the human decides.
                await self._respond_to_agent_request(notification)

            # Inbound message = progress; reset the idle deadline.
            deadline = loop.time() + _PROMPT_TIMEOUT_SECONDS

    async def interrupt_session(self, session_key: str) -> bool:  # noqa: ARG002 — one ACP session per process
        """Abort the running turn via the ACP ``session/cancel`` notification.

        The agent responds by ending the in-flight ``session/prompt`` with a
        ``cancelled`` stop reason, which the ``run_turn`` loop then surfaces.
        Best-effort: a no-op (returns ``False``) if there's no live session.
        """
        if self._proc is None or self._proc.returncode is not None or self._session_id is None:
            return False
        try:
            await self._send(
                {
                    "jsonrpc": "2.0",
                    "method": _CLIENT_NOTIFICATION_SESSION_CANCEL,
                    "params": {"sessionId": self._session_id},
                }
            )
            # Duplicate interrupts share the original deadline, even during reap.
            if self._cancel_grace_deadline is None:
                self._cancel_grace_deadline = (
                    asyncio.get_running_loop().time() + ACP_CANCEL_GRACE_SECONDS
                )
            return True
        except Exception as exc:  # noqa: BLE001 — interrupt is best-effort
            logger.debug("acp[%s] session/cancel failed: %s", self._config.name, exc)
            return False

    async def close_session(self, session_key: str) -> None:
        """Close a named session (no-op; the ACP session is per-process)."""

    def close_timeout(self) -> float:
        return ACP_REAP_TIMEOUT_SECONDS

    async def close(self) -> None:
        """Allow a cancelled prompt to answer, then terminate the agent subprocess."""
        if self._cancel_grace_deadline is not None and self._active_prompt_request_id is not None:
            response = self._pending.get(self._active_prompt_request_id)
            remaining = self._cancel_grace_deadline - asyncio.get_running_loop().time()
            if response is not None and not response.done():
                # Keep the reader and stdin alive while the cancelled prompt answers.
                if remaining > 0:
                    await asyncio.wait({response}, timeout=remaining)
                if not response.done():
                    logger.debug(
                        "acp[%s] session/prompt %s cancel grace expired without a response",
                        self._config.name,
                        self._active_prompt_request_id,
                    )
        self._cancel_grace_deadline = None
        self._active_prompt_request_id = None
        self._active_prompt_response_received = False
        self._cancel_detached_request_tasks()
        detached = tuple(self._detached_request_tasks)
        if detached:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*detached, return_exceptions=True), timeout=1.0
                )
            except TimeoutError:
                for task in detached:
                    task.cancel()
            self._detached_request_tasks.clear()
        self._reset_session_state()
        # Tear down the Omnigent MCP relay HTTP server + its bridge dir first.
        with contextlib.suppress(Exception):
            self._mcp.close()
        if self._reader_task:
            self._reader_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._reader_task
            self._reader_task = None
        if self._stderr_task:
            self._stderr_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._stderr_task
            self._stderr_task = None
        if self._os_environment is not None:
            with contextlib.suppress(Exception):
                self._os_environment.close()
            self._os_environment = None
        if self._proc:
            with contextlib.suppress(Exception):
                self._proc.stdin.close()  # type: ignore[union-attr]
            try:
                # Tree-aware: the handle is the sandbox launcher, not the
                # agent it forked. A bare terminate() orphans the agent.
                _proc.terminate_tree(self._proc)
                await asyncio.wait_for(self._proc.wait(), timeout=_ACP_TERMINATION_TIMEOUT_SECONDS)
            except Exception:  # noqa: BLE001
                with contextlib.suppress(Exception):
                    _proc.kill_tree(self._proc)
            finally:
                self._proc = None
