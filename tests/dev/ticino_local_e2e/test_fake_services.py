"""Unit tests for the secret-safe evidence helpers in the live OIDC harness."""

from __future__ import annotations

import base64
import http.client
import importlib.util
import json
import os
import subprocess
import sys
import threading
from pathlib import Path
from urllib.parse import urlencode

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
    store.observe_exchange(
        route_matched=True,
        used_post=True,
        form_urlencoded=True,
        emissary_request="true",
        workload_authorization="Bearer fake-workload-bearer",
        grant_type=fake_services._TOKEN_EXCHANGE_GRANT_TYPE,
        audience="hab",
        subject_token_type=fake_services._ID_TOKEN_TYPE,
        requested_token_type=fake_services._ID_TOKEN_TYPE,
        subject_token=token,
    )
    store.observe_habitat_create("Bearer fake-habitat-obo-bearer")

    receipt = json.loads(receipt_path.read_text())
    assert receipt == {
        "exchange_form_urlencoded": True,
        "exchange_grant_type_matches": True,
        "exchange_requested_hab_audience": True,
        "exchange_requests": 1,
        "exchange_route_matched": True,
        "exchange_requested_token_type_matches": True,
        "exchange_subject_token_type_matches": True,
        "exchange_used_post": True,
        "emissary_request_header_matches": True,
        "habitat_authorization_present": True,
        "habitat_create_requests": 1,
        "subject_token_audience_matches_client": True,
        "subject_token_is_jwt": True,
        "subject_token_present": True,
        "workload_authorization_distinct_from_subject_token": True,
        "workload_authorization_present": True,
    }
    assert token not in receipt_path.read_text()


def test_exchange_handler_requires_rfc8693_form_and_returns_json_access_token(
    tmp_path: Path,
) -> None:
    receipt_path = tmp_path / "receipt.json"
    store = fake_services.ReceiptStore(receipt_path)
    server = fake_services.ThreadingHTTPServer(
        ("127.0.0.1", 0), fake_services.make_exchange_handler(store)
    )
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        token = _jwt_with_audience("omnigent-local")
        form = urlencode(
            {
                "grant_type": fake_services._TOKEN_EXCHANGE_GRANT_TYPE,
                "audience": "hab",
                "subject_token_type": fake_services._ID_TOKEN_TYPE,
                "requested_token_type": fake_services._ID_TOKEN_TYPE,
                "subject_token": token,
            }
        )
        connection = http.client.HTTPConnection("127.0.0.1", server.server_port)
        connection.request(
            "POST",
            fake_services._EXCHANGE_PATH,
            body=form,
            headers={
                "Authorization": "Bearer fake-workload-bearer",
                "Content-Type": "application/x-www-form-urlencoded",
                "X-Emissary-Request": "true",
            },
        )
        response = connection.getresponse()
        assert response.status == 200
        assert response.getheader("Content-Type") == "application/json"
        assert json.loads(response.read()) == {
            "access_token": "fake-habitat-obo-bearer",
            "expires_in": 60,
            "issued_token_type": fake_services._ID_TOKEN_TYPE,
            "token_type": "Bearer",
        }
        connection.close()
    finally:
        server.shutdown()
        server.server_close()
        thread.join()

    receipt = json.loads(receipt_path.read_text())
    assert all(
        receipt[key]
        for key in (
            "exchange_route_matched",
            "exchange_used_post",
            "exchange_form_urlencoded",
            "emissary_request_header_matches",
            "workload_authorization_present",
            "workload_authorization_distinct_from_subject_token",
            "exchange_grant_type_matches",
            "exchange_requested_hab_audience",
            "exchange_subject_token_type_matches",
            "exchange_requested_token_type_matches",
            "subject_token_present",
            "subject_token_is_jwt",
            "subject_token_audience_matches_client",
        )
    )
    assert receipt["habitat_create_requests"] == 0
    assert receipt["habitat_authorization_present"] is False
    assert receipt["exchange_requests"] == 1
    assert token not in receipt_path.read_text()


def test_exchange_handler_rejects_missing_emissary_routing_header(tmp_path: Path) -> None:
    receipt_path = tmp_path / "receipt.json"
    store = fake_services.ReceiptStore(receipt_path)
    server = fake_services.ThreadingHTTPServer(
        ("127.0.0.1", 0), fake_services.make_exchange_handler(store)
    )
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        form = urlencode(
            {
                "grant_type": fake_services._TOKEN_EXCHANGE_GRANT_TYPE,
                "audience": "hab",
                "subject_token_type": fake_services._ID_TOKEN_TYPE,
                "requested_token_type": fake_services._ID_TOKEN_TYPE,
                "subject_token": _jwt_with_audience("omnigent-local"),
            }
        )
        connection = http.client.HTTPConnection("127.0.0.1", server.server_port)
        connection.request(
            "POST",
            fake_services._EXCHANGE_PATH,
            body=form,
            headers={
                "Authorization": "Bearer fake-workload-bearer",
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )
        assert connection.getresponse().status == 400
        connection.close()
    finally:
        server.shutdown()
        server.server_close()
        thread.join()

    receipt = json.loads(receipt_path.read_text())
    assert receipt["emissary_request_header_matches"] is False


def test_exchange_handler_rejects_a_legacy_get_shaped_request() -> None:
    assert not fake_services._is_valid_exchange_request(
        route_matched=True,
        form_urlencoded=False,
        emissary_request=None,
        workload_authorization="Bearer fake-workload-bearer",
        grant_type=None,
        audience="hab",
        subject_token_type=None,
        requested_token_type=None,
        subject_token=None,
    )


def test_down_rejects_an_unsafe_state_directory() -> None:
    script = _MODULE_PATH.parents[0] / "run.sh"
    env = os.environ | {"TICINO_E2E_STATE_DIR": "/"}
    result = subprocess.run([str(script), "down"], env=env, capture_output=True, text=True)
    assert result.returncode != 0
    assert "must use /tmp/omnigent-ticino-token-handoff-e2e" in result.stderr


def test_run_script_keeps_database_password_out_of_process_arguments() -> None:
    script = (_MODULE_PATH.parents[0] / "run.sh").read_text()
    assert "--database-uri" not in script
    assert '--config "$server_config"' in script
    assert 'PGPASSFILE="$pgpass_file"' in script
    assert "postgresql+psycopg://%s@127.0.0.1" in script
    assert "psycopg[binary]" in script


def test_run_script_detaches_long_running_processes() -> None:
    script = (_MODULE_PATH.parents[0] / "run.sh").read_text()
    assert "nohup env PYTHONPATH=" in script
    assert "exec nohup env" in script
