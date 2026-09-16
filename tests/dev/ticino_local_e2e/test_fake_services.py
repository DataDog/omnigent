"""Unit tests for the secret-safe evidence helpers in the live OIDC harness."""

from __future__ import annotations

import base64
import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

_MODULE_PATH = Path(__file__).parents[3] / "dev/ticino_local_e2e/fake_services.py"
_SPEC = importlib.util.spec_from_file_location("ticino_fake_services", _MODULE_PATH)
assert _SPEC is not None and _SPEC.loader is not None
fake_services = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = fake_services
_SPEC.loader.exec_module(fake_services)


def _jwt_with_audience(audience: object) -> str:
    payload = json.dumps({"aud": audience}).encode()
    encoded = base64.urlsafe_b64encode(payload).rstrip(b"=").decode()
    return f"header.{encoded}.signature"


def test_inspect_untrusted_jwt_requires_registered_client_audience() -> None:
    assert fake_services.inspect_untrusted_jwt(
        _jwt_with_audience(["something-else", "omnigent-local"]),
        expected_client_id="omnigent-local",
    ) == (True, True)
    assert fake_services.inspect_untrusted_jwt(
        _jwt_with_audience("different-client"),
        expected_client_id="omnigent-local",
    ) == (True, False)


def test_inspect_untrusted_jwt_rejects_non_jwt_without_echoing_it() -> None:
    result = fake_services.inspect_untrusted_jwt(
        "not-a-token", expected_client_id="omnigent-local"
    )
    assert result == (
        False,
        False,
    )


def test_receipt_stores_only_boolean_handoff_evidence(tmp_path: Path) -> None:
    receipt_path = tmp_path / "receipt.json"
    store = fake_services.ReceiptStore(receipt_path)
    token = _jwt_with_audience("omnigent-local")
    store.observe_exchange(route_matched=True, audience="hab", bearer=token)
    store.observe_habitat_create("Bearer fake-habitat-obo-bearer")

    receipt = json.loads(receipt_path.read_text())
    assert receipt == {
        "exchange_requested_hab_audience": True,
        "exchange_requests": 1,
        "exchange_route_matched": True,
        "habitat_authorization_present": True,
        "habitat_create_requests": 1,
        "subject_token_audience_matches_client": True,
        "subject_token_is_jwt": True,
        "subject_token_present": True,
    }
    assert token not in receipt_path.read_text()


def test_down_rejects_an_unsafe_state_directory() -> None:
    script = _MODULE_PATH.parents[0] / "run.sh"
    env = os.environ | {"TICINO_E2E_STATE_DIR": "/"}
    result = subprocess.run([str(script), "down"], env=env, capture_output=True, text=True)
    assert result.returncode != 0
    assert "must use /tmp/omnigent-ticino-token-handoff-e2e" in result.stderr
