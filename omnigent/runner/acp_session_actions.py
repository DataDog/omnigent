"""Bridge generation-bound ACP child actions into the existing runner paths."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import math
from collections.abc import Awaitable, Callable, Mapping
from typing import Any
from urllib.parse import quote

import httpx

from omnigent.inner.acp_standard_subagents import (
    ACTION_FEATURES,
    ACTION_PROTOCOL,
    ACTION_PROTOCOL_VERSION,
)
from omnigent.spec.types import DEFAULT_ASK_TIMEOUT

_logger = logging.getLogger("omnigent.runner.app")
_PROTOCOL = ACTION_PROTOCOL
_VERSION = ACTION_PROTOCOL_VERSION
_WAIT_MS = 25_000
_RETRY_INITIAL_S = 0.5
_RETRY_MAX_S = 15.0
_MAX_ACTIVE_ACTIONS = 64
_MAX_TOOL_ACTIONS = 8
_MAX_POLICY_ACTIONS = 16
_MAX_ACTION_ORIGINS = 2_048
# The harness retains 2,048 action result/cancellation tombstones per generation.
_MAX_ACTION_ID_TOMBSTONES = 2_048

ResultSink = Callable[[dict[str, Any]], Awaitable[None]]
ToolActionHandler = Callable[[dict[str, Any], ResultSink], Awaitable[None]]
PolicyActionHandler = Callable[[dict[str, Any], ResultSink], Awaitable[None]]
EventPublisher = Callable[[str, Mapping[str, object]], None]
ApprovalRememberer = Callable[[str], None]
FatalGenerationCallback = Callable[[str, str, str], Awaitable[None]]
ApprovalTargetResolver = Callable[[dict[str, Any]], Awaitable[str]]


class AcpSessionActionSubscriber:
    """Poll one harness generation and execute each action at most once."""

    def __init__(
        self,
        *,
        parent_id: str,
        harness_client: httpx.AsyncClient,
        generation: str,
        publish_event: EventPublisher,
        remember_approval: ApprovalRememberer,
        dispatch_tool: ToolActionHandler,
        evaluate_policy: PolicyActionHandler,
        resolve_approval_target: ApprovalTargetResolver | None = None,
        on_fatal_generation: FatalGenerationCallback | None = None,
    ) -> None:
        self.parent_id = parent_id
        self.generation = generation
        self._harness_client = harness_client
        self._publish_event = publish_event
        self._remember_approval = remember_approval
        self._dispatch_tool = dispatch_tool
        self._evaluate_policy = evaluate_policy
        self._resolve_approval_target = resolve_approval_target
        self._on_fatal_generation = on_fatal_generation
        self._origin_handlers: dict[str, tuple[ToolActionHandler, PolicyActionHandler, float]] = {}
        self._stopped = asyncio.Event()
        self._changed = asyncio.Event()
        self._poll_task: asyncio.Task[None] | None = None
        self._action_tasks: dict[str, asyncio.Task[None]] = {}
        self._action_payloads: dict[str, dict[str, Any]] = {}
        self._action_identities: dict[str, tuple[str, str]] = {}
        # Never evict an identity during this generation: a stale poll response
        # after result acceptance must not execute a side-effecting tool again.
        self._completed_actions: dict[str, tuple[str, str]] = {}
        self._result_cache: dict[str, dict[str, Any]] = {}
        self._approval_waiters: dict[str, tuple[str, asyncio.Future[dict[str, Any]]]] = {}
        self._tool_slots = asyncio.Semaphore(_MAX_TOOL_ACTIONS)
        self._policy_slots = asyncio.Semaphore(_MAX_POLICY_ACTIONS)
        self._fatal_generation_reason: str | None = None
        self._fatal_generation_notification: asyncio.Task[None] | None = None
        self._features: frozenset[str] = frozenset()

    async def start(self) -> None:
        """Validate the negotiated action channel and start its independent poller."""
        response = await self._harness_client.get(
            f"/v1/sessions/{quote(self.parent_id, safe='')}/actions",
            params={"generation": self.generation, "wait_ms": 0},
            timeout=10.0,
        )
        if response.status_code in {404, 501}:
            raise AcpSessionActionChannelUnsupported(
                "harness does not provide the standard ACP session-action channel"
            )
        response.raise_for_status()
        self._validate_page(response.json())
        self._poll_task = asyncio.create_task(
            self._poll(), name=f"acp-session-actions:{self.parent_id}"
        )

    @property
    def is_busy(self) -> bool:
        """Whether an accepted child action is still being handled."""
        return bool(self._action_tasks)

    @property
    def features(self) -> tuple[str, ...]:
        """Features proven by the live action-capability endpoint."""
        return tuple(sorted(self._features))

    async def wait_until_idle(self, *, timeout_s: float = 10.0) -> bool:
        """Wait for accepted actions to finish without stopping action admission."""
        deadline = asyncio.get_running_loop().time() + timeout_s
        while self._action_tasks:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return False
            self._changed.clear()
            if not self._action_tasks:
                return True
            try:
                await asyncio.wait_for(self._changed.wait(), timeout=remaining)
            except TimeoutError:
                return False
        return True

    async def drain_available(self, *, timeout_s: float = 10.0) -> bool:
        """Admit queued harness actions and wait until they are acknowledged."""
        deadline = asyncio.get_running_loop().time() + timeout_s
        for _ in range(16):
            if self._stopped.is_set():
                return False
            response = await self._harness_client.get(
                f"/v1/sessions/{quote(self.parent_id, safe='')}/actions",
                params={"generation": self.generation, "wait_ms": 0},
                timeout=10.0,
            )
            response.raise_for_status()
            actions = self._validate_page(response.json())
            for action in actions:
                self._start_action(action)
                if self._fatal_generation_reason is not None:
                    return False
            if not actions:
                return await self.wait_until_idle(
                    timeout_s=max(0.0, deadline - asyncio.get_running_loop().time())
                )
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0 or not await self.wait_until_idle(timeout_s=remaining):
                return False
        return False

    async def drain_origins(self, origin_tokens: set[str], *, timeout_s: float = 10.0) -> bool:
        """Admit queued actions for selected origins and wait only for those actions."""
        if not origin_tokens:
            return True
        deadline = asyncio.get_running_loop().time() + timeout_s
        for _ in range(16):
            if self._stopped.is_set():
                return False
            response = await self._harness_client.get(
                f"/v1/sessions/{quote(self.parent_id, safe='')}/actions",
                params={"generation": self.generation, "wait_ms": 0},
                timeout=10.0,
            )
            response.raise_for_status()
            actions = self._validate_page(response.json())
            for action in actions:
                child = action.get("child")
                if isinstance(child, dict) and child.get("origin_token") in origin_tokens:
                    self._start_action(action)
                    if self._fatal_generation_reason is not None:
                        return False
            pending = [
                task
                for action_id, task in self._action_tasks.items()
                if (
                    isinstance(self._action_payloads.get(action_id, {}).get("child"), dict)
                    and self._action_payloads[action_id]["child"].get("origin_token")
                    in origin_tokens
                )
            ]
            if not pending:
                return True
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return False
            _done, unfinished = await asyncio.wait(pending, timeout=remaining)
            if unfinished:
                return False
        return False

    async def stop(self) -> None:
        """Stop polling and cancel every action owned by this harness generation."""
        self._stopped.set()
        current = asyncio.current_task()
        tasks = [task for task in self._action_tasks.values() if task is not current]
        for task in tasks:
            task.cancel()
        poll_task = self._poll_task
        if poll_task is not None and poll_task is not current:
            poll_task.cancel()
            tasks.append(poll_task)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for _action_id, future in self._approval_waiters.values():
            if not future.done():
                future.cancel()
        self._approval_waiters.clear()
        self._action_tasks.clear()
        self._action_payloads.clear()
        self._action_identities.clear()
        self._completed_actions.clear()
        self._result_cache.clear()
        self._origin_handlers.clear()

    async def drain_and_stop(self, *, timeout_s: float = 10.0) -> None:
        """Stop admission, let accepted actions settle, then cancel stragglers.

        The poller is stopped first so no new actions enter while existing work
        delivers its result. A bounded timeout prevents a human approval or an
        unavailable harness from pinning generation teardown forever.
        """
        if self._stopped.is_set():
            await self.stop()
            return
        current = asyncio.current_task()
        poll_task = self._poll_task
        self._poll_task = None
        if poll_task is not None and poll_task is not current:
            poll_task.cancel()
            await asyncio.gather(poll_task, return_exceptions=True)
        tasks = [task for task in self._action_tasks.values() if task is not current]
        if tasks:
            try:
                await asyncio.wait_for(asyncio.gather(*tasks), timeout=timeout_s)
            except TimeoutError:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
        self._stopped.set()
        await self.stop()

    def register_origin(
        self,
        token: str,
        dispatch_tool: ToolActionHandler,
        evaluate_policy: PolicyActionHandler,
        *,
        ask_timeout_s: float = DEFAULT_ASK_TIMEOUT,
    ) -> None:
        """Bind a runner-issued parent-turn token to its immutable handlers."""
        if not token or len(token) > 256:
            raise ValueError("ACP action origin token is malformed")
        if (
            isinstance(ask_timeout_s, bool)
            or not isinstance(ask_timeout_s, (int, float))
            or not math.isfinite(ask_timeout_s)
            or ask_timeout_s <= 0
        ):
            raise ValueError("ACP action origin approval timeout must be finite and positive")
        existing = self._origin_handlers.get(token)
        handlers = (dispatch_tool, evaluate_policy, float(ask_timeout_s))
        if existing is not None:
            if existing != handlers:
                raise RuntimeError("ACP action origin token was rebound")
            return
        if len(self._origin_handlers) >= _MAX_ACTION_ORIGINS:
            raise RuntimeError("ACP action origin capacity reached")
        self._origin_handlers[token] = handlers

    def forget_origin(self, token: str) -> bool:
        """Release settled handlers without touching action idempotency tombstones.

        Call only after the report/action barriers have drained this origin. The
        active-action guard protects against accidental early cleanup, while the
        completed-action identity cache remains independent of handler lifetime.
        """
        for action in self._action_payloads.values():
            child = action.get("child")
            if isinstance(child, dict) and child.get("origin_token") == token:
                return False
        return self._origin_handlers.pop(token, None) is not None

    async def cancel_child(self, native_epoch: str, native_session_id: str, run_id: str) -> int:
        """Cancel only actions belonging to the terminal child run."""
        from omnigent.inner.acp_subagents import acp_child_key

        target_key = acp_child_key(self.parent_id, native_epoch, native_session_id, run_id)
        return await self.cancel_child_key(target_key)

    async def cancel_child_key(self, child_key: str) -> int:
        """Cancel actions belonging to a report subscriber's stable child key."""
        tasks: list[asyncio.Task[None]] = []
        for action_id, task in tuple(self._action_tasks.items()):
            child = self._action_payloads.get(action_id, {}).get("child")
            if isinstance(child, dict) and _child_key(self.parent_id, child) == child_key:
                task.cancel()
                tasks.append(task)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        return len(tasks)

    def resolve_approval(self, data: Mapping[str, Any]) -> bool:
        """Route a UI approval reply to the child action that owns its elicitation."""
        elicitation_id = data.get("elicitation_id")
        if not isinstance(elicitation_id, str) or not elicitation_id:
            return False
        entry = self._approval_waiters.get(elicitation_id)
        if entry is None:
            return False
        _action_id, future = entry
        if future.done():
            return True
        action = data.get("action")
        if action not in ("accept", "decline", "cancel"):
            return False
        result: dict[str, Any] = {
            "type": "approval",
            "elicitation_id": elicitation_id,
            "action": action,
        }
        content = data.get("content")
        if isinstance(content, dict):
            result["content"] = content
        future.set_result(result)
        return True

    async def _poll(self) -> None:
        delay = _RETRY_INITIAL_S
        while not self._stopped.is_set():
            try:
                self._changed.clear()
                response = await self._harness_client.get(
                    f"/v1/sessions/{quote(self.parent_id, safe='')}/actions",
                    params={"generation": self.generation, "wait_ms": _WAIT_MS},
                    timeout=_WAIT_MS / 1000 + 10.0,
                )
                if response.status_code == 409:
                    _logger.info(
                        "ACP action generation ended", extra={"session_id": self.parent_id}
                    )
                    await self.stop()
                    return
                response.raise_for_status()
                actions = self._validate_page(response.json())
                started = False
                for action in actions:
                    started = self._start_action(action) or started
                    if self._fatal_generation_reason is not None:
                        return
                if actions and not started:
                    # The harness retains pending actions until the result is
                    # acknowledged. Back off when a page contains only work
                    # already being handled instead of spinning on that page.
                    with contextlib.suppress(TimeoutError):
                        await asyncio.wait_for(self._changed.wait(), timeout=delay)
                    delay = min(delay * 2, _RETRY_MAX_S)
                else:
                    delay = _RETRY_INITIAL_S
            except asyncio.CancelledError:
                raise
            except AcpSessionActionChannelUnsupported as exc:
                self._fail_generation(str(exc))
                return
            except Exception:
                _logger.exception(
                    "ACP session-action polling failed", extra={"session_id": self.parent_id}
                )
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._stopped.wait(), timeout=delay)
                delay = min(delay * 2, _RETRY_MAX_S)

    def _validate_page(self, page: Any) -> list[dict[str, Any]]:
        if not isinstance(page, dict):
            raise AcpSessionActionChannelUnsupported(
                "harness returned an incompatible ACP session-action page"
            )
        if page.get("generation") != self.generation:
            raise RuntimeError("harness returned a stale ACP session-action generation")
        if (
            page.get("protocol") != _PROTOCOL
            or page.get("version") != _VERSION
            or not isinstance(page.get("features"), list)
            or not ACTION_FEATURES.issubset(page["features"])
        ):
            raise AcpSessionActionChannelUnsupported(
                "harness returned an incompatible ACP session-action page"
            )
        features = page.get("features")
        self._features = frozenset(feature for feature in features if isinstance(feature, str))
        actions = page.get("actions")
        if not isinstance(actions, list):
            raise RuntimeError("harness returned malformed ACP session actions")
        validated: list[dict[str, Any]] = []
        for action in actions:
            if not isinstance(action, dict):
                raise RuntimeError("harness returned a malformed ACP session action")
            action_id = action.get("action_id")
            event_id = action.get("event_id")
            child = action.get("child")
            event = action.get("event")
            if (
                not isinstance(action_id, str)
                or not action_id
                or not isinstance(event_id, str)
                or not event_id
                or not isinstance(child, dict)
                or not isinstance(event, dict)
            ):
                raise RuntimeError("harness returned an incomplete ACP session action")
            if not all(
                isinstance(child.get(field), str) and child[field]
                for field in ("native_epoch", "native_session_id", "run_id")
            ):
                raise RuntimeError("harness returned an action without trusted child identity")
            if not isinstance(child.get("origin_token"), str) or not child["origin_token"]:
                raise RuntimeError("harness returned an action without a trusted turn origin")
            if (
                child.get("run_id") == "session"
                and child.get("origin_generation") != self.generation
            ):
                raise RuntimeError("harness returned an ACP child action for a stale origin")
            cwd = child.get("cwd")
            directories = child.get("additional_directories", [])
            if cwd is not None and (not isinstance(cwd, str) or not cwd):
                raise RuntimeError("harness returned an ACP child action with an invalid cwd")
            if not isinstance(directories, list) or any(
                not isinstance(path, str) or not path for path in directories
            ):
                raise RuntimeError("harness returned an ACP child action with invalid directories")
            validated.append(action)
        return validated

    def _start_action(self, action: dict[str, Any]) -> bool:
        action_id = action["action_id"]
        if self._stopped.is_set():
            return False
        identity = _action_identity(action)
        completed = self._completed_actions.get(action_id)
        if completed is not None:
            if completed != identity:
                _logger.error(
                    "ACP action id was reused with different content after completion",
                    extra={"session_id": self.parent_id, "action_id": action_id},
                )
            return False
        active = self._action_identities.get(action_id)
        if active is not None:
            if active != identity:
                _logger.error(
                    "ACP action id was reused with different content while active",
                    extra={"session_id": self.parent_id, "action_id": action_id},
                )
            return False
        if len(self._action_tasks) >= _MAX_ACTIVE_ACTIONS:
            return False
        # Reserve a tombstone slot before execution begins. Refuse new work at
        # capacity rather than evicting an at-most-once identity.
        if (
            len(self._completed_actions) + len(self._action_identities)
            >= _MAX_ACTION_ID_TOMBSTONES
        ):
            self._fail_generation(
                "ACP action identity capacity reached; refusing further actions."
            )
            return False
        task = asyncio.create_task(
            self._run_action(action), name=f"acp-session-action:{self.parent_id}:{action_id}"
        )
        self._action_payloads[action_id] = action
        self._action_identities[action_id] = identity
        self._action_tasks[action_id] = task
        task.add_done_callback(lambda done, key=action_id: self._action_done(key, done))
        return True

    def _fail_generation(self, reason: str) -> None:
        """Stop this generation without manufacturing a result for overflow work."""
        if self._fatal_generation_reason is not None:
            return
        self._fatal_generation_reason = reason
        self._stopped.set()
        _logger.error(
            "ACP action identity capacity reached; failing the harness generation",
            extra={"session_id": self.parent_id},
        )
        if self._on_fatal_generation is not None:
            self._fatal_generation_notification = asyncio.create_task(
                self._notify_fatal_generation(reason),
                name=f"acp-session-action-failure:{self.parent_id}:{self.generation}",
            )

    async def _notify_fatal_generation(self, reason: str) -> None:
        callback = self._on_fatal_generation
        if callback is None:
            return
        try:
            await callback(self.parent_id, self.generation, reason)
        except asyncio.CancelledError:
            raise
        except Exception:
            _logger.exception(
                "ACP fatal-generation recovery callback failed",
                extra={"session_id": self.parent_id},
            )

    def _action_done(self, action_id: str, task: asyncio.Task[None]) -> None:
        if self._action_tasks.get(action_id) is not task:
            return
        self._changed.set()
        exception = None if task.cancelled() else task.exception()
        cached = self._result_cache.get(action_id)
        action = self._action_payloads.get(action_id)
        if (
            cached is not None
            and action is not None
            and not task.cancelled()
            and not self._stopped.is_set()
        ):
            if exception is not None:
                _logger.error(
                    "ACP session action handler ended; retrying cached result only",
                    exc_info=(type(exception), exception, exception.__traceback__),
                    extra={"session_id": self.parent_id, "action_id": action_id},
                )
            retry = asyncio.create_task(
                self._retry_cached_result(action_id),
                name=f"acp-session-action-result:{self.parent_id}:{action_id}",
            )
            self._action_tasks[action_id] = retry
            retry.add_done_callback(lambda done, key=action_id: self._action_done(key, done))
            return
        self._action_tasks.pop(action_id, None)
        self._action_payloads.pop(action_id, None)
        self._result_cache.pop(action_id, None)
        identity = self._action_identities.pop(action_id, None)
        if identity is not None and not self._stopped.is_set():
            self._completed_actions[action_id] = identity
        for correlation, (owner_action_id, _future) in tuple(self._approval_waiters.items()):
            if owner_action_id == action_id:
                self._approval_waiters.pop(correlation, None)
        if exception is not None:
            _logger.error(
                "ACP session action ended before delivery",
                exc_info=(type(exception), exception, exception.__traceback__),
                extra={"session_id": self.parent_id, "action_id": action_id},
            )

    async def _retry_cached_result(self, action_id: str) -> None:
        action = self._action_payloads.get(action_id)
        result = self._result_cache.get(action_id)
        if action is not None and result is not None:
            await self._post_result(action, result)

    async def _run_action(self, action: dict[str, Any]) -> None:
        action_id = action["action_id"]
        event = action["event"]
        event_type = event.get("type")
        delivered = False

        async def deliver(result: dict[str, Any]) -> None:
            nonlocal delivered
            if delivered:
                cached = self._result_cache.get(action_id)
                if cached is not None and cached != result:
                    raise RuntimeError("ACP action attempted to deliver conflicting results")
                return
            delivered = True
            await self._post_result(action, result)

        try:
            child = action.get("child")
            origin_token = child.get("origin_token") if isinstance(child, dict) else None
            origin_handlers = (
                self._origin_handlers.get(origin_token) if isinstance(origin_token, str) else None
            )
            if origin_handlers is None:
                raise RuntimeError("ACP child action has no registered parent-turn origin")
            if event_type == "response.output_item.done":
                item = event.get("item")
                if (
                    not isinstance(item, dict)
                    or item.get("type") != "function_call"
                    or item.get("status") != "action_required"
                ):
                    raise RuntimeError("ACP tool action has an unsupported output item")
                async with self._tool_slots:
                    await origin_handlers[0](action, deliver)
            elif event_type == "policy_evaluation.requested":
                async with self._policy_slots:
                    await origin_handlers[1](action, deliver)
            elif event_type == "response.elicitation_request":
                await self._await_approval(action, deliver, ask_timeout_s=origin_handlers[2])
            else:
                raise RuntimeError(f"unsupported ACP action event {event_type!r}")
            if not delivered and not self._stopped.is_set():
                raise RuntimeError("ACP action handler returned without a result")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _logger.exception(
                "ACP session action handler failed",
                extra={"session_id": self.parent_id, "action_id": action_id},
            )
            fallback = self._fallback_result(event, exc)
            if fallback is not None and not delivered and not self._stopped.is_set():
                await deliver(fallback)

    async def _await_approval(
        self,
        action: dict[str, Any],
        deliver: ResultSink,
        *,
        ask_timeout_s: float,
    ) -> None:
        event = action["event"]
        elicitation_id = event.get("elicitation_id")
        if not isinstance(elicitation_id, str) or not elicitation_id:
            raise RuntimeError("ACP approval action has no elicitation id")
        published_event = event
        if self._resolve_approval_target is not None:
            target_session_id = await self._resolve_approval_target(action)
            if not isinstance(target_session_id, str) or not target_session_id:
                raise RuntimeError("ACP child approval has no persisted target session")
            params = event.get("params")
            if not isinstance(params, dict):
                raise RuntimeError("ACP child approval has no parameter object")
            existing_target = params.get("target_session_id")
            if existing_target not in (None, target_session_id):
                raise RuntimeError("ACP child approval has a conflicting target session")
            published_event = {
                **event,
                "params": {**params, "target_session_id": target_session_id},
            }
        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        waiter = (action["action_id"], future)
        self._approval_waiters[elicitation_id] = waiter
        self._remember_approval(elicitation_id)
        result: dict[str, Any] | None = None
        unanswered = False
        try:
            self._publish_event(self.parent_id, published_event)
            try:
                result = await asyncio.wait_for(asyncio.shield(future), timeout=ask_timeout_s)
            except TimeoutError:
                # Fence late answers before publishing or result delivery can await.
                if self._approval_waiters.get(elicitation_id) == waiter:
                    self._approval_waiters.pop(elicitation_id, None)
                future.cancel()
                unanswered = True
                result = {
                    "type": "approval",
                    "elicitation_id": elicitation_id,
                    "action": "cancel",
                }
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 — cancellation result must still reach the child
            if self._approval_waiters.get(elicitation_id) == waiter:
                self._approval_waiters.pop(elicitation_id, None)
            unanswered = True
            result = {
                "type": "approval",
                "elicitation_id": elicitation_id,
                "action": "cancel",
            }
        finally:
            if self._approval_waiters.get(elicitation_id) == waiter:
                self._approval_waiters.pop(elicitation_id, None)
            resolved: dict[str, object] = {
                "type": "response.elicitation_resolved",
                "elicitation_id": elicitation_id,
            }
            if unanswered:
                resolved["reason"] = "unanswered"
            elif result is not None:
                resolved["action"] = result.get("action")
            with contextlib.suppress(Exception):
                self._publish_event(self.parent_id, resolved)
        if result is not None:
            await deliver(result)

    async def _post_result(self, action: dict[str, Any], result: dict[str, Any]) -> None:
        action_id = action["action_id"]
        event_id = action["event_id"]
        cached = self._result_cache.get(action_id)
        if cached is not None and cached != result:
            raise RuntimeError("ACP action result changed after delivery began")
        self._result_cache[action_id] = dict(result)
        body = {
            "generation": self.generation,
            "event_id": event_id,
            "result": self._result_cache[action_id],
        }
        delay = _RETRY_INITIAL_S
        while not self._stopped.is_set():
            try:
                response = await self._harness_client.post(
                    f"/v1/sessions/{quote(self.parent_id, safe='')}/actions/"
                    f"{quote(action_id, safe='')}/result",
                    json=body,
                    timeout=30.0,
                )
                if 200 <= response.status_code < 300:
                    payload = response.json()
                    if not isinstance(payload, dict) or payload.get("accepted") is not True:
                        raise RuntimeError("harness did not acknowledge the ACP action result")
                    self._result_cache.pop(action_id, None)
                    return
                if response.status_code == 409:
                    detail = ""
                    try:
                        payload = response.json()
                        detail = str(payload.get("error", "")) if isinstance(payload, dict) else ""
                    except (TypeError, ValueError):
                        pass
                    if detail in {"stale_generation", "action_cancelled", "action_not_found"}:
                        self._result_cache.pop(action_id, None)
                        return
                _logger.warning(
                    "ACP action result delivery returned HTTP %d; retrying",
                    response.status_code,
                    extra={"session_id": self.parent_id, "action_id": action_id},
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                _logger.warning(
                    "ACP action result delivery failed; retrying",
                    exc_info=True,
                    extra={"session_id": self.parent_id, "action_id": action_id},
                )
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stopped.wait(), timeout=delay)
            delay = min(delay * 2, _RETRY_MAX_S)

    def _fallback_result(self, event: dict[str, Any], exc: Exception) -> dict[str, Any] | None:
        event_type = event.get("type")
        if event_type == "response.output_item.done":
            item = event.get("item")
            call_id = item.get("call_id") if isinstance(item, dict) else None
            if isinstance(call_id, str) and call_id:
                return {
                    "type": "tool_result",
                    "call_id": call_id,
                    "output": json.dumps({"error": str(exc)}),
                }
        if event_type == "policy_evaluation.requested":
            evaluation_id = event.get("evaluation_id")
            phase = event.get("phase")
            if isinstance(evaluation_id, str) and evaluation_id:
                # Unexpected runner errors fail closed. The normal policy helper
                # retains its established phase-aware fallback behavior.
                return {
                    "type": "policy_verdict",
                    "evaluation_id": evaluation_id,
                    "action": "POLICY_ACTION_DENY",
                    "reason": f"Policy evaluation failed in the runner for {phase}.",
                }
        if event_type == "response.elicitation_request":
            elicitation_id = event.get("elicitation_id")
            if isinstance(elicitation_id, str) and elicitation_id:
                return {
                    "type": "approval",
                    "elicitation_id": elicitation_id,
                    "action": "cancel",
                }
        return None


class AcpSessionActionChannelUnsupported(RuntimeError):
    """The live harness does not implement the required private action channel."""


async def ensure_session_action_capability(
    *,
    parent_id: str,
    harness_client: httpx.AsyncClient,
    generation: str,
    require_full_reporting: bool = False,
) -> bool:
    """Probe the real empty-or-populated action page before native ACP opt-in."""
    response = await harness_client.get(
        f"/v1/sessions/{quote(parent_id, safe='')}/actions",
        params={"generation": generation, "wait_ms": 0},
        timeout=10.0,
    )
    if response.status_code in {404, 501}:
        if require_full_reporting:
            raise RuntimeError("configured ACP reporting requires private action protocol v2")
        return False
    response.raise_for_status()
    try:
        page = response.json()
    except ValueError as exc:
        raise RuntimeError("harness returned malformed ACP session-action capabilities") from exc
    if isinstance(page, dict) and page.get("generation") != generation:
        raise RuntimeError("harness returned a stale ACP session-action generation")
    supported = (
        isinstance(page, dict)
        and page.get("protocol") == _PROTOCOL
        and page.get("version") == _VERSION
        and page.get("generation") == generation
        and isinstance(page.get("features"), list)
        and ACTION_FEATURES.issubset(page["features"])
        and isinstance(page.get("actions"), list)
    )
    if not supported and require_full_reporting:
        raise RuntimeError("configured ACP reporting requires private action protocol v2")
    return bool(supported)


def _action_identity(action: Mapping[str, Any]) -> tuple[str, str]:
    event_id = action.get("event_id")
    if not isinstance(event_id, str) or not event_id:
        raise RuntimeError("ACP action has no event id")
    canonical = json.dumps(
        action, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")
    )
    return event_id, hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _child_key(parent_id: str, child: Mapping[str, Any]) -> str | None:
    from omnigent.inner.acp_subagents import acp_child_key

    native_epoch = child.get("native_epoch")
    native_session_id = child.get("native_session_id")
    run_id = child.get("run_id")
    if not isinstance(native_epoch, str) or not native_epoch:
        return None
    if not isinstance(native_session_id, str) or not native_session_id:
        return None
    if not isinstance(run_id, str) or not run_id:
        return None
    return acp_child_key(parent_id, native_epoch, native_session_id, run_id)
