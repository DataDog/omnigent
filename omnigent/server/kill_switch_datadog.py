"""Datadog Feature Flags source for the kill switch (``datadog-flags`` extra).

Uses ddtrace's OpenFeature provider, which caches the flag configuration
(CDN or Agent Remote Config) so evaluation is an in-memory read.
"""

from __future__ import annotations

import os
import socket
from collections.abc import Mapping
from typing import Any, cast

from omnigent.server.kill_switch import FlagReading

_INSTALL_HINT = (
    "OMNIGENT_KILL_SWITCH_FLAG_KEY is set but the 'datadog-flags' extra is not "
    "installed; install omnigent[datadog-flags] (or OMNIGENT_EXTRAS=datadog-flags)."
)


class DatadogFlagSource:
    """Evaluates one boolean flag through OpenFeature."""

    def __init__(
        self,
        flag_key: str,
        *,
        environ: Mapping[str, str] | None = None,
        provider: Any | None = None,
    ) -> None:
        try:
            from openfeature import api
            from openfeature.evaluation_context import EvaluationContext
        except ImportError as exc:
            raise RuntimeError(_INSTALL_HINT) from exc
        if provider is None:
            try:
                from ddtrace.openfeature import DataDogProvider
            except ImportError as exc:
                raise RuntimeError(_INSTALL_HINT) from exc
            provider = cast(Any, DataDogProvider())
        env = os.environ if environ is None else environ
        self._api = api
        self._flag_key = flag_key
        self._provider = provider
        self._client: Any = None
        self._context = EvaluationContext(
            targeting_key=env.get("DD_SERVICE") or "omnigent-server",
            attributes={
                "env": env.get("DD_ENV", ""),
                "version": env.get("DD_VERSION", ""),
                "replica": socket.gethostname(),
            },
        )

    def evaluate(self, last_known: bool) -> FlagReading:
        if self._client is None:
            # Blocks up to the provider's own init timeout; runs off the loop.
            self._api.set_provider(self._provider)
            self._client = self._api.get_client("omnigent-kill-switch")
        details = self._client.get_boolean_details(self._flag_key, last_known, self._context)
        if details.error_code is not None:
            message = details.error_message or str(details.error_code)
            return FlagReading(engaged=None, error=f"{details.error_code}: {message}")
        return FlagReading(engaged=bool(details.value))

    def close(self) -> None:
        if self._client is not None:
            self._api.shutdown()
