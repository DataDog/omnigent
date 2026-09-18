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


@dataclass
class Receipt:
    """Non-sensitive, durable evidence emitted by the local fake services."""

    exchange_requests: int = 0
    exchange_route_matched: bool = False
    exchange_requested_hab_audience: bool = False
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

    def observe_exchange(self, *, route_matched: bool, audience: str | None, bearer: str) -> None:
        with self._lock:
            receipt = self._receipt
            receipt.exchange_requests += 1
            receipt.exchange_route_matched = receipt.exchange_route_matched or route_matched
            receipt.exchange_requested_hab_audience = (
                receipt.exchange_requested_hab_audience or audience == "hab"
            )
            receipt.subject_token_present = receipt.subject_token_present or bool(bearer)
            is_jwt, expected_audience = inspect_untrusted_jwt(
                bearer, expected_client_id="omnigent-local"
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
        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            route_matched = parsed.path == "/ticino/agent/v1/issuer/sycamore/token"
            desired_audience = parse_qs(parsed.query).get("desired_audience", [None])[0]
            receipts.observe_exchange(
                route_matched=route_matched,
                audience=desired_audience,
                bearer=_bearer_from_header(self.headers.get("Authorization")),
            )
            if not route_matched:
                self.send_error(HTTPStatus.NOT_FOUND)
                return
            # Deliberately static test data, never a real credential.  Its only
            # purpose is to prove the launcher uses the exchange output when it
            # reaches the loopback fake Habitat service.
            body = b"fake-habitat-obo-bearer"
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, _format: str, *_args: object) -> None:
            # BaseHTTPRequestHandler's default log includes request paths and
            # is unnecessary noise for a harness dealing with credentials.
            return

    return ExchangeHandler


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
    parser.add_argument("--launcher-module", default="omnigent_hab_launcher")
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
