#!/usr/bin/env python3
"""Loopback-only fake Emissary and Habitat endpoints for the live OIDC harness.

The process records only boolean assertions.  It never writes a request
authorization header, ID token, workload bearer, or exchanged bearer to disk
or stdout.  The fake Habitat deliberately rejects its first create request
after observing it, so this harness cannot create a real sandbox by accident.
"""

from __future__ import annotations

import argparse
import base64
import importlib
import json
import threading
from concurrent import futures
from dataclasses import asdict, dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

_EXCHANGE_PATH = "/ticino/agent/v1/issuer/sycamore/oauth/token"
_TOKEN_EXCHANGE_GRANT_TYPE = "urn:ietf:params:oauth:grant-type:token-exchange"
_ID_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:id_token"


@dataclass
class Receipt:
    """Non-sensitive, durable evidence emitted by the local fake services."""

    exchange_requests: int = 0
    exchange_route_matched: bool = False
    exchange_used_post: bool = False
    exchange_form_urlencoded: bool = False
    emissary_request_header_matches: bool = False
    workload_authorization_present: bool = False
    workload_authorization_distinct_from_subject_token: bool = False
    exchange_grant_type_matches: bool = False
    exchange_requested_hab_audience: bool = False
    exchange_subject_token_type_matches: bool = False
    exchange_requested_token_type_matches: bool = False
    subject_token_present: bool = False
    subject_token_is_jwt: bool = False
    subject_token_audience_matches_client: bool = False
    habitat_create_requests: int = 0
    habitat_authorization_present: bool = False


class ReceiptStore:
    """Write a receipt atomically; values deliberately exclude credentials."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._receipt = Receipt()
        self._lock = threading.Lock()
        self._write_locked()

    def observe_exchange(
        self,
        *,
        route_matched: bool,
        used_post: bool,
        form_urlencoded: bool,
        emissary_request: str | None,
        workload_authorization: str | None,
        grant_type: str | None,
        audience: str | None,
        subject_token_type: str | None,
        requested_token_type: str | None,
        subject_token: str,
    ) -> None:
        with self._lock:
            workload_bearer = _bearer_from_header(workload_authorization)
            receipt = self._receipt
            receipt.exchange_requests += 1
            receipt.exchange_route_matched = receipt.exchange_route_matched or route_matched
            receipt.exchange_used_post = receipt.exchange_used_post or used_post
            receipt.exchange_form_urlencoded = receipt.exchange_form_urlencoded or form_urlencoded
            receipt.emissary_request_header_matches = (
                receipt.emissary_request_header_matches or emissary_request == "true"
            )
            receipt.workload_authorization_present = (
                receipt.workload_authorization_present or bool(workload_bearer)
            )
            receipt.workload_authorization_distinct_from_subject_token = (
                receipt.workload_authorization_distinct_from_subject_token
                or (bool(workload_bearer) and workload_bearer != subject_token)
            )
            receipt.exchange_grant_type_matches = (
                receipt.exchange_grant_type_matches or grant_type == _TOKEN_EXCHANGE_GRANT_TYPE
            )
            receipt.exchange_requested_hab_audience = (
                receipt.exchange_requested_hab_audience or audience == "hab"
            )
            receipt.exchange_subject_token_type_matches = (
                receipt.exchange_subject_token_type_matches or subject_token_type == _ID_TOKEN_TYPE
            )
            receipt.exchange_requested_token_type_matches = (
                receipt.exchange_requested_token_type_matches
                or requested_token_type == _ID_TOKEN_TYPE
            )
            receipt.subject_token_present = receipt.subject_token_present or bool(subject_token)
            is_jwt, expected_audience = inspect_untrusted_jwt(
                subject_token, expected_client_id="omnigent-local"
            )
            receipt.subject_token_is_jwt = receipt.subject_token_is_jwt or is_jwt
            receipt.subject_token_audience_matches_client = (
                receipt.subject_token_audience_matches_client or expected_audience
            )
            self._write_locked()

    def observe_habitat_create(self, authorization: str | None) -> None:
        with self._lock:
            self._receipt.habitat_create_requests += 1
            self._receipt.habitat_authorization_present = (
                self._receipt.habitat_authorization_present or bool(authorization)
            )
            self._write_locked()

    def _write_locked(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self._path.with_suffix(".tmp")
        temporary.write_text(json.dumps(asdict(self._receipt), sort_keys=True) + "\n")
        temporary.replace(self._path)


def inspect_untrusted_jwt(token: str, *, expected_client_id: str) -> tuple[bool, bool]:
    """Return structural/audience assertions without retaining token contents.

    This does not validate a signature; Omnigent has already done that during
    its OIDC callback.  The fake endpoint only establishes that the launcher
    sent a JWT-shaped subject token for the registered public client.
    """

    pieces = token.split(".")
    if len(pieces) != 3 or not all(pieces):
        return False, False
    try:
        payload = pieces[1] + "=" * (-len(pieces[1]) % 4)
        claims = json.loads(base64.urlsafe_b64decode(payload.encode("ascii")))
    except (UnicodeEncodeError, ValueError, json.JSONDecodeError):
        return False, False
    audience = claims.get("aud") if isinstance(claims, dict) else None
    if isinstance(audience, str):
        return True, audience == expected_client_id
    if isinstance(audience, list):
        return True, expected_client_id in audience
    return True, False


def _bearer_from_header(header: str | None) -> str:
    if not header or not header.startswith("Bearer "):
        return ""
    return header.removeprefix("Bearer ")


def make_exchange_handler(receipts: ReceiptStore) -> type[BaseHTTPRequestHandler]:
    """Construct the fake Emissary HTTP handler bound to a receipt store."""

    class ExchangeHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            parsed = urlparse(self.path)
            route_matched = parsed.path == _EXCHANGE_PATH
            content_length = int(self.headers.get("Content-Length", "0"))
            form_urlencoded = self.headers.get("Content-Type", "").split(";", 1)[0] == (
                "application/x-www-form-urlencoded"
            )
            form = (
                parse_qs(self.rfile.read(content_length).decode("utf-8"))
                if form_urlencoded
                else {}
            )
            receipts.observe_exchange(
                route_matched=route_matched,
                used_post=True,
                form_urlencoded=form_urlencoded,
                emissary_request=self.headers.get("X-Emissary-Request"),
                workload_authorization=self.headers.get("Authorization"),
                grant_type=_form_value(form, "grant_type"),
                audience=_form_value(form, "audience"),
                subject_token_type=_form_value(form, "subject_token_type"),
                requested_token_type=_form_value(form, "requested_token_type"),
                subject_token=_form_value(form, "subject_token") or "",
            )
            if not _is_valid_exchange_request(
                route_matched=route_matched,
                form_urlencoded=form_urlencoded,
                emissary_request=self.headers.get("X-Emissary-Request"),
                workload_authorization=self.headers.get("Authorization"),
                grant_type=_form_value(form, "grant_type"),
                audience=_form_value(form, "audience"),
                subject_token_type=_form_value(form, "subject_token_type"),
                requested_token_type=_form_value(form, "requested_token_type"),
                subject_token=_form_value(form, "subject_token"),
            ):
                self.send_error(HTTPStatus.BAD_REQUEST, "invalid RFC 8693 token exchange request")
                return
            # Deliberately static test data, never a real credential.  Its only
            # purpose is to prove the launcher uses the exchange output when it
            # reaches the loopback fake Habitat service.
            body = json.dumps(
                {
                    "access_token": "fake-habitat-obo-bearer",
                    "issued_token_type": _ID_TOKEN_TYPE,
                    "token_type": "Bearer",
                }
            ).encode()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, _format: str, *_args: object) -> None:
            # BaseHTTPRequestHandler's default log includes request paths and
            # is unnecessary noise for a harness dealing with credentials.
            return

    return ExchangeHandler


def _form_value(form: dict[str, list[str]], key: str) -> str | None:
    values = form.get(key)
    return values[0] if values and len(values) == 1 else None


def _is_valid_exchange_request(
    *,
    route_matched: bool,
    form_urlencoded: bool,
    emissary_request: str | None,
    workload_authorization: str | None,
    grant_type: str | None,
    audience: str | None,
    subject_token_type: str | None,
    requested_token_type: str | None,
    subject_token: str | None,
) -> bool:
    return (
        route_matched
        and form_urlencoded
        and emissary_request == "true"
        and bool(_bearer_from_header(workload_authorization))
        and grant_type == _TOKEN_EXCHANGE_GRANT_TYPE
        and audience == "hab"
        and subject_token_type == _ID_TOKEN_TYPE
        and requested_token_type == _ID_TOKEN_TYPE
        and bool(subject_token)
    )


def start_fake_habitat(*, port: int, receipts: ReceiptStore, launcher_module: str) -> Any:
    """Start an intentionally non-provisioning gRPC Habitat service."""

    try:
        import grpc

        hab_pb2 = importlib.import_module(f"{launcher_module}.generated.hab.v1.hab_pb2")
        hab_pb2_grpc = importlib.import_module(f"{launcher_module}.generated.hab.v1.hab_pb2_grpc")
    except ImportError as exc:
        raise RuntimeError(
            f"Could not import fake-Habitat dependencies from {launcher_module!r}; "
            "install the PR #91876 launcher wheel in the harness venv or set "
            "HAB_LAUNCHER_MODULE to its installed module name."
        ) from exc

    class HabitatFake(hab_pb2_grpc.HabServiceServicer):
        def CreateHab(self, _request: Any, context: Any) -> Any:
            receipts.observe_habitat_create(
                dict(context.invocation_metadata()).get("authorization")
            )
            yield hab_pb2.CreateHabResponse(
                error="local fake Habitat stops after validating the OBO handoff",
                terminal=True,
            )

        def ListHabs(self, _request: Any, _context: Any) -> Any:
            return hab_pb2.ListHabsResponse()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    hab_pb2_grpc.add_HabServiceServicer_to_server(HabitatFake(), server)
    if server.add_insecure_port(f"127.0.0.1:{port}") != port:
        raise RuntimeError(f"could not bind fake Habitat on 127.0.0.1:{port}")
    server.start()
    return server


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--exchange-port", type=int, default=6768)
    parser.add_argument("--habitat-port", type=int, default=6769)
    parser.add_argument("--launcher-module", default="hab_launcher")
    args = parser.parse_args()

    receipts = ReceiptStore(args.receipt)
    exchange = ThreadingHTTPServer(
        ("127.0.0.1", args.exchange_port), make_exchange_handler(receipts)
    )
    habitat = start_fake_habitat(
        port=args.habitat_port,
        receipts=receipts,
        launcher_module=args.launcher_module,
    )
    try:
        exchange.serve_forever()
    finally:
        exchange.server_close()
        habitat.stop(grace=0)


if __name__ == "__main__":
    main()
