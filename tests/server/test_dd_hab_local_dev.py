"""Tests for the central Datadog Habitat local-development opt-in."""

from __future__ import annotations

import pytest

from omnigent.dd_hab_local_dev import HAB_LOCAL_DEV_ENV, resolve_hab_local_dev_overrides


def test_absent_flag_without_substitutes_preserves_production_defaults() -> None:
    assert resolve_hab_local_dev_overrides({}).enabled is False
    assert resolve_hab_local_dev_overrides({}).substitute_names == frozenset()


@pytest.mark.parametrize("value", ["0", "false", "no", "off", "", " FALSE "])
def test_false_values_are_accepted(value: str) -> None:
    assert resolve_hab_local_dev_overrides({HAB_LOCAL_DEV_ENV: value}).enabled is False


@pytest.mark.parametrize("value", ["1", "true", "yes", "on", " TRUE "])
def test_true_values_are_accepted(value: str) -> None:
    assert resolve_hab_local_dev_overrides({HAB_LOCAL_DEV_ENV: value}).enabled is True


def test_malformed_flag_fails_configuration() -> None:
    with pytest.raises(ValueError, match=HAB_LOCAL_DEV_ENV):
        resolve_hab_local_dev_overrides({HAB_LOCAL_DEV_ENV: "maybe"})


@pytest.mark.parametrize(
    "substitute",
    [
        "OMNIGENT_HAB_EXCHANGE_MODE",
        "HAB_WORKLOAD_TOKEN_FILE",
        "OMNIGENT_HAB_TICINO_ADDRESS",
        "OMNIGENT_HAB_REGISTRY_PATH",
        "OMNIGENT_HAB_LOCAL_READINESS_GRACE_SECONDS",
    ],
)
def test_substitute_requires_explicit_local_gate(substitute: str) -> None:
    with pytest.raises(ValueError, match=substitute):
        resolve_hab_local_dev_overrides({substitute: "secret-or-path"})


def test_local_gate_reports_names_but_not_values() -> None:
    result = resolve_hab_local_dev_overrides(
        {HAB_LOCAL_DEV_ENV: "1", "HAB_WORKLOAD_TOKEN_FILE": "/private/token"}
    )
    assert result.enabled is True
    assert result.substitute_names == frozenset({"HAB_WORKLOAD_TOKEN_FILE"})
    assert "/private/token" not in repr(result)


def test_local_gate_without_substitute_has_no_override() -> None:
    assert (
        resolve_hab_local_dev_overrides({HAB_LOCAL_DEV_ENV: "1"}).substitute_names == frozenset()
    )
