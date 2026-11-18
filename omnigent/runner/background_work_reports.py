"""Report each session's background-work flag to the server, newest value last.

The flag reaches the server as an ``external_background_work`` session event.
One request runs per session at a time and always sends the newest value. Each
report carries when it was decided, strictly increasing, so the server keeps the
newest even if an abandoned request lands late. Transient failures, including a
request with no answer by its deadline, retry with backoff. A server that predates
the event rejects it as an unknown type, and the reporter stops sending until the
runner reconnects, possibly to an upgraded server.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from urllib.parse import quote

import httpx

_logger = logging.getLogger(__name__)

EVENT_TYPE = "external_background_work"
_RETRY_INITIAL_S = 1.0
_RETRY_MAX_S = 30.0
# The runner's client has no read timeout, so an unanswered request would hold
# back every later value for its session.
_REQUEST_TIMEOUT_S = 10.0

Post = Callable[..., Awaitable[httpx.Response]]


class BackgroundWorkReporter:
    """Send each session's newest background-work flag, one request per session at a time."""

    def __init__(self, post: Post) -> None:
        """
        :param post: The runner's authenticated server client's ``post``.
        """
        self._post = post
        # Newest value per session, with when it was decided in epoch nanoseconds.
        self._wanted: dict[str, tuple[bool, int]] = {}
        # Last desired state survives successful sends and reconnects. Keep it
        # separate from _wanted, which is only the current retry queue.
        self._known: dict[str, bool] = {}
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._disabled = False
        # Bumped on reconnect: an answer to a request sent before it is stale.
        self._generation = 0
        self._last_reported_at = 0
        # Strict report waiters keep the existing single-writer path and its
        # monotonic revisions while allowing ACP delivery to wait for a server
        # acknowledgment before acknowledging the durable child report.
        self._required: dict[str, list[tuple[bool, int, asyncio.Future[None]]]] = {}

    def resume(self) -> None:
        """Reassert every known session state after reconnect."""
        self._generation += 1
        self._disabled = False
        self.reassert()

    def reassert(self) -> None:
        """Queue the newest known state for every session on the current connection."""
        for session_id, running in tuple(self._known.items()):
            self.report(session_id, running)

    def report(self, session_id: str, running: bool) -> None:
        """
        Send *running* for *session_id*; call on the runner's event loop.

        :param session_id: Session/conversation identifier, e.g. ``"conv_abc123"``.
        :param running: Whether the session's work outlives its ended turn.
        """
        self._last_reported_at = max(time.time_ns(), self._last_reported_at + 1)
        self._known[session_id] = running
        if self._disabled:
            return
        self._wanted[session_id] = (running, self._last_reported_at)
        if session_id not in self._tasks:
            self._tasks[session_id] = asyncio.get_running_loop().create_task(
                self._send_newest(session_id)
            )

    async def report_and_wait(
        self, session_id: str, running: bool, *, timeout_s: float = 30.0
    ) -> None:
        """Send a required state transition and wait for its server acknowledgment.

        Legacy shell reporters continue to call :meth:`report` without waiting.
        ACP starts and terminal clears use this method so a durable report is not
        acknowledged before the server has received the combined session state.
        """
        loop = asyncio.get_running_loop()
        self._last_reported_at = max(time.time_ns(), self._last_reported_at + 1)
        reported_at = self._last_reported_at
        self._known[session_id] = running
        if self._disabled:
            raise RuntimeError("the server does not accept background-work reports")
        future: asyncio.Future[None] = loop.create_future()
        self._required.setdefault(session_id, []).append((running, reported_at, future))
        self._wanted[session_id] = (running, reported_at)
        if session_id not in self._tasks:
            self._tasks[session_id] = loop.create_task(self._send_newest(session_id))
        try:
            await asyncio.wait_for(future, timeout=timeout_s)
        finally:
            required = self._required.get(session_id)
            if required is not None:
                required[:] = [entry for entry in required if entry[2] is not future]
                if not required:
                    self._required.pop(session_id, None)

    async def _send_newest(self, session_id: str) -> None:
        # The acknowledged value, with the connection generation it was sent on.
        sent: tuple[bool, int] | None = None
        delay = _RETRY_INITIAL_S
        try:
            while not self._disabled:
                required = self._required.get(session_id)
                required_item = required[0] if required else None
                wanted = self._wanted.get(session_id)
                if required_item is None and wanted is None:
                    return
                if required_item is not None:
                    running, reported_at = required_item[0], required_item[1]
                elif wanted is not None:
                    running, reported_at = wanted
                else:
                    return
                # A reconnect's resync must reach the new connection's server.
                if sent == (running, self._generation):
                    if required_item is not None:
                        if not required_item[2].done():
                            required_item[2].set_result(None)
                        current_required = self._required.get(session_id)
                        if current_required and current_required[0] is required_item:
                            current_required.pop(0)
                        if current_required is not None and not current_required:
                            self._required.pop(session_id, None)
                        continue
                    return
                generation = self._generation
                outcome = await self._send(session_id, running, reported_at)
                # A reconnect can replace the server while this request is in
                # flight. Its response belongs to the old connection and must
                # not release a strict ACP waiter or suppress resync on the new
                # connection, even when it was a successful HTTP response.
                if generation != self._generation:
                    sent = None
                    continue
                if outcome == "rejected":
                    self._fail_required(
                        session_id,
                        RuntimeError("the server rejected a background-work report"),
                    )
                    return
                if outcome == "acknowledged":
                    sent = (running, generation)
                    delay = _RETRY_INITIAL_S
                    if required_item is not None:
                        required = self._required.get(session_id)
                        if required and required[0] is required_item:
                            required.pop(0)
                            if not required:
                                self._required.pop(session_id, None)
                        if not required_item[2].done():
                            required_item[2].set_result(None)
                    continue
                # A failed request may still have reached the server, so its value is unknown.
                sent = None
                await asyncio.sleep(delay)
                delay = min(delay * 2, _RETRY_MAX_S)
        except Exception as exc:  # strict waiters must not be stranded.
            self._fail_required(session_id, exc)
            _logger.exception("background-work reporter stopped unexpectedly")
        finally:
            self._tasks.pop(session_id, None)
            if not self._required.get(session_id):
                self._wanted.pop(session_id, None)

    def _fail_required(self, session_id: str, exc: BaseException) -> None:
        for _running, _reported_at, future in self._required.pop(session_id, []):
            if not future.done():
                future.set_exception(exc)

    async def _send(self, session_id: str, running: bool, reported_at: int) -> str:
        """Send one event; ``"acknowledged"``, ``"retry"`` or ``"rejected"``."""
        generation = self._generation
        try:
            response = await asyncio.wait_for(
                self._post(
                    f"/v1/sessions/{quote(session_id, safe='')}/events",
                    json={
                        "type": EVENT_TYPE,
                        "data": {"running": running, "reported_at": reported_at},
                    },
                ),
                timeout=_REQUEST_TIMEOUT_S,
            )
        except (httpx.HTTPError, OSError):  # OSError includes the deadline's TimeoutError.
            return "retry"
        if response.is_success:
            return "acknowledged"
        if response.status_code in (408, 429) or response.status_code >= 500:
            return "retry"
        if "Unknown event type" in response.text:
            if generation != self._generation:
                # The server that refused it was replaced by a reconnect; try the new one.
                return "retry"
            # The server predates the flag, so nothing sent to it can succeed.
            _logger.info("The server does not accept background-work reports")
            self._disabled = True
        else:
            _logger.warning(
                "The server rejected a background-work report (HTTP %s)",
                response.status_code,
                extra={"session_id": session_id},
            )
        return "rejected"
