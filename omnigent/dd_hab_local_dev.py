"""Typed opt-in for temporary Datadog Habitat development substitutes.

This gate is owned by the Habitat integration and must be removed before
release 0.1.0.  It deliberately does not control migrations, authentication,
or credential handling: it only permits the narrowly listed local adapters.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass
from functools import lru_cache

logger = logging.getLogger(__name__)

HAB_LOCAL_DEV_ENV = "OMNIGENT_DD_HAB_LOCAL_DEV"

# These are adapter-selection inputs, not normal Habitat deployment settings.
# Keep paths and values out of the resolved value and diagnostics because a
# workload-token path can itself be sensitive operational information.
_LOCAL_SUBSTITUTE_ENVS = frozenset(
    {
        "HAB_WORKLOAD_TOKEN_FILE",
        "OMNIGENT_HAB_REGISTRY_PATH",
        "OMNIGENT_HAB_LOCAL_READINESS_GRACE_SECONDS",
    }
)
_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})
_FALSE_VALUES = frozenset({"0", "false", "no", "off", ""})


@dataclass(frozen=True)
class HabLocalDevOverrides:
    """The names of selected development-only adapters, never their values."""

    enabled: bool
    substitute_names: frozenset[str]


def resolve_hab_local_dev_overrides(
    environ: Mapping[str, str] | None = None,
) -> HabLocalDevOverrides:
    """Strictly resolve local Habitat substitutes from an environment mapping."""
    source = os.environ if environ is None else environ
    raw_flag = source.get(HAB_LOCAL_DEV_ENV, "").strip().lower()
    if raw_flag in _TRUE_VALUES:
        enabled = True
    elif raw_flag in _FALSE_VALUES:
        enabled = False
    else:
        raise ValueError(
            f"{HAB_LOCAL_DEV_ENV} must be one of 1, true, yes, on, 0, false, no, or off"
        )

    selected = {name for name in _LOCAL_SUBSTITUTE_ENVS if source.get(name, "").strip()}
    if source.get("OMNIGENT_HAB_EXCHANGE_MODE", "").strip().lower() in {"file", "static"}:
        selected.add("OMNIGENT_HAB_EXCHANGE_MODE")
    selected = frozenset(selected)
    if selected and not enabled:
        names = ", ".join(sorted(selected))
        raise ValueError(
            f"{names} are Datadog Habitat development substitutes; set "
            f"{HAB_LOCAL_DEV_ENV}=1 to enable them explicitly"
        )
    return HabLocalDevOverrides(enabled=enabled, substitute_names=selected)


@lru_cache(maxsize=1)
def hab_local_dev_overrides() -> HabLocalDevOverrides:
    """Resolve once per server process and warn when substitutes are selected."""
    overrides = resolve_hab_local_dev_overrides()
    if overrides.enabled and overrides.substitute_names:
        logger.warning(
            "%s enables Datadog Habitat development substitutes: %s",
            HAB_LOCAL_DEV_ENV,
            ", ".join(sorted(overrides.substitute_names)),
        )
    return overrides
