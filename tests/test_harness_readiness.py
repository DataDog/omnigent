"""Tests for omnigent.onboarding.harness_readiness gating."""

from __future__ import annotations

import io
import json
import subprocess

import pytest

from omnigent.onboarding import harness_readiness as hr


class _FakeStdin:
    def __init__(self) -> None:
        self.text = ""

    def write(self, value: str) -> int:
        self.text += value
        return len(value)

    def flush(self) -> None:
        return None

    def close(self) -> None:
        return None


class _FakePiProcess:
    def __init__(self, stdout: str) -> None:
        self.stdin = _FakeStdin()
        self.stdout = io.StringIO(stdout)
        self.returncode: int | None = None

    def poll(self) -> int | None:
        return self.returncode

    def terminate(self) -> None:
        self.returncode = -15

    def kill(self) -> None:
        self.returncode = -9

    def wait(self, timeout: float | None = None) -> int:
        del timeout
        if self.returncode is None:
            self.returncode = 0
        return self.returncode


@pytest.mark.parametrize("harness", ["pi", "pi-native", "native-pi"])
def test_pi_harnesses_gate_on_pi_cli(harness: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """``pi`` and ``pi-native`` are both gated on the ``pi`` CLI being installed.

    Regression guard: ``pi-native`` has no ``_HARNESS_FAMILY`` entry (pi uses
    the ``PI_SURFACE`` sentinel), so it used to hit the unknown-harness
    fail-open branch and report configured even when ``pi`` was missing — the
    host pre-spawn check then let a doomed launch through. Both spellings must
    track ``harness_cli_installed``.
    """
    monkeypatch.setattr(hr, "harness_cli_installed", lambda _key, **_kw: False)
    assert hr.harness_is_configured(harness) is False

    monkeypatch.setattr(hr, "harness_cli_installed", lambda _key, **_kw: True)
    assert hr.harness_is_configured(harness) is True


@pytest.mark.parametrize("harness", ["kiro-native", "native-kiro"])
def test_kiro_native_harnesses_gate_on_kiro_cli(
    harness: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Native Kiro is gated on the ``kiro-cli`` binary being installed."""
    calls: list[str] = []

    def _installed(key: str, **_kw: object) -> bool:
        calls.append(key)
        return False

    monkeypatch.setattr(hr, "harness_cli_installed", _installed)
    assert hr.harness_is_configured(harness) is False
    assert calls[-1] == hr.KIRO_KEY

    monkeypatch.setattr(hr, "harness_cli_installed", lambda _key, **_kw: True)
    assert hr.harness_is_configured(harness) is True


def test_sdk_and_unknown_harnesses_still_fail_open(monkeypatch: pytest.MonkeyPatch) -> None:
    """SDK and unknown harnesses are never gated, even with no CLI installed.

    Pins that the pi-native fix narrowed only the pi surface — SDK harnesses
    (runtime/ambient credentials) and unknown harnesses must keep failing open
    so a working launch is never blocked.
    """
    monkeypatch.setattr(hr, "harness_cli_installed", lambda _key, **_kw: False)
    assert hr.harness_is_configured("claude-sdk") is True
    assert hr.harness_is_configured("openai-agents") is True
    assert hr.harness_is_configured("totally-unknown-harness") is True


def test_configured_harness_map_exposes_pi_native(monkeypatch: pytest.MonkeyPatch) -> None:
    """The readiness map carries a ``pi-native`` key for the web picker lookup.

    The agent picker warns "needs setup" by looking up the agent's harness
    (``pi-native``) in this map; without the key the Pi row could never warn.
    A missing binary now reports the richer ``"binary-missing"`` reason (Pi
    gained the credential axis) rather than a bare ``False``.
    """
    monkeypatch.setattr(hr, "harness_cli_installed", lambda _key, **_kw: False)
    monkeypatch.setattr(hr, "resolve_cli_binary", lambda *_args, **_kwargs: None)
    cmap = hr.configured_harness_map()
    assert cmap.get("pi-native") == "binary-missing"
    assert cmap.get("pi") == "binary-missing"


def test_configured_harness_map_pi_installed_without_any_models_needs_auth(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pi installed but with no usable model source reports ``"needs-auth"``.

    Neither an Omnigent-managed provider nor a model backed by Pi's own
    credentials is available, so the picker should retain its yellow warning.
    """
    monkeypatch.setattr(hr, "harness_cli_installed", lambda _key, **_kw: True)
    monkeypatch.setattr(hr, "_family_provider_configured", lambda _h: False)
    monkeypatch.setattr(hr, "_pi_cli_has_available_models", lambda: False)
    cmap = hr.configured_harness_map()
    assert cmap.get("pi") == "needs-auth"
    assert cmap.get("pi-native") == "needs-auth"


def test_configured_harness_map_pi_installed_with_native_models_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pi's own available models make it ready without an Omnigent provider."""
    monkeypatch.setattr(hr, "harness_cli_installed", lambda _key, **_kw: True)
    monkeypatch.setattr(hr, "_family_provider_configured", lambda _h: False)
    monkeypatch.setattr(hr, "_pi_cli_has_available_models", lambda: True)
    cmap = hr.configured_harness_map()
    assert cmap.get("pi") is True
    assert cmap.get("pi-native") is True


def test_configured_harness_map_pi_installed_with_provider_skips_rpc_probe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An Omnigent provider remains the fast path and avoids spawning Pi."""
    monkeypatch.setattr(hr, "harness_cli_installed", lambda _key, **_kw: True)
    monkeypatch.setattr(hr, "_family_provider_configured", lambda _h: True)

    def _must_not_probe() -> bool:
        raise AssertionError("Pi RPC probed despite a configured provider")

    monkeypatch.setattr(hr, "_pi_cli_has_available_models", _must_not_probe)
    cmap = hr.configured_harness_map()
    assert cmap.get("pi") is True
    assert cmap.get("pi-native") is True


def test_configured_harness_map_probes_pi_readiness_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """All Pi aliases share one potentially expensive RPC probe per refresh."""
    calls = 0
    monkeypatch.setattr(hr, "harness_cli_installed", lambda _key, **_kw: True)
    monkeypatch.setattr(hr, "_family_provider_configured", lambda _h: False)

    def _models_available() -> bool:
        nonlocal calls
        calls += 1
        return True

    monkeypatch.setattr(hr, "_pi_cli_has_available_models", _models_available)
    cmap = hr.configured_harness_map()
    assert calls == 1
    assert cmap["pi"] is True
    assert cmap["pi-native"] is True
    assert cmap["native-pi"] is True


@pytest.mark.parametrize(("models", "expected"), [([], False), ([{"id": "model-1"}], True)])
def test_pi_rpc_probe_uses_available_models_json(
    monkeypatch: pytest.MonkeyPatch,
    models: list[dict[str, str]],
    expected: bool,
) -> None:
    """Pi's structured RPC response is parsed without scraping table output."""
    response = json.dumps(
        {
            "id": "omnigent-readiness",
            "type": "response",
            "command": "get_available_models",
            "success": True,
            "data": {"models": models},
        }
    )
    process = _FakePiProcess("not-json\n" + response + "\n")
    spawned: list[tuple[list[str], dict[str, object]]] = []
    monkeypatch.setattr(hr, "resolve_cli_binary", lambda *_args, **_kwargs: "/bin/pi")

    def _popen(argv: list[str], **kwargs: object) -> _FakePiProcess:
        spawned.append((argv, kwargs))
        return process

    monkeypatch.setattr(hr.subprocess, "Popen", _popen)

    assert hr._pi_cli_has_available_models(timeout=1.0) is expected
    assert spawned[0][0] == [
        "/bin/pi",
        "--mode",
        "rpc",
        "--no-extensions",
        "--offline",
        "--no-session",
    ]
    assert spawned[0][1] == {
        "stdin": subprocess.PIPE,
        "stdout": subprocess.PIPE,
        "stderr": subprocess.DEVNULL,
        "text": True,
        "encoding": "utf-8",
        "errors": "replace",
    }
    assert json.loads(process.stdin.text) == {
        "id": "omnigent-readiness",
        "type": "get_available_models",
    }


def test_configured_harness_map_exposes_kiro_native(monkeypatch: pytest.MonkeyPatch) -> None:
    """The readiness map carries Kiro native keys for the web picker lookup."""
    monkeypatch.setattr(hr, "harness_cli_installed", lambda _key, **_kw: False)
    cmap = hr.configured_harness_map()
    assert cmap.get("kiro-native") is False
    assert cmap.get("native-kiro") is False
