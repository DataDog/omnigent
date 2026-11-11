"""Habitat AuthService adapter for the Habitat sandbox backend."""

from __future__ import annotations

import importlib
import os
from urllib.parse import urlsplit


def habitat_broker_enabled() -> bool:
    return os.environ.get(
        "OMNIGENT_SANDBOX_PROVIDER_MODULE"
    ) == "omnigent_hab_launcher" and os.environ.get("OMNIGENT_HAB_ENABLED", "").lower() in {
        "true",
        "1",
        "yes",
        "on",
    }


class HabitatBroker:
    """Use the application identity for every bounded broker request."""

    def __init__(self, apiserver: str) -> None:
        endpoint = urlsplit(apiserver)
        if (
            endpoint.scheme != "https"
            or not endpoint.hostname
            or endpoint.username
            or endpoint.password
            or endpoint.path not in {"", "/"}
            or endpoint.query
            or endpoint.fragment
        ):
            raise ValueError("Habitat broker requires an HTTPS API origin")
        self._target = endpoint.netloc

    @classmethod
    def from_env(cls) -> HabitatBroker:
        return cls(os.environ.get("HAB_APISERVER", ""))

    def _call(self, method: str, request, response_type):
        # The deployment plugin supplies its first-hop workload identity.
        import grpc

        try:
            plugin = importlib.import_module("omnigent_hab_launcher.production")
            bearer = plugin.get_broker_application_bearer()
            if not isinstance(bearer, str) or not bearer.strip():
                raise ValueError("Application identity unavailable")
            with grpc.secure_channel(self._target, grpc.ssl_channel_credentials()) as channel:
                rpc = channel.unary_unary(
                    f"/hab.v1.AuthService/{method}",
                    request_serializer=lambda value: value.SerializeToString(),
                    response_deserializer=response_type.FromString,
                )
                return rpc(request, metadata=(("authorization", f"Bearer {bearer}"),), timeout=10)
        except Exception:  # noqa: BLE001 - credential boundary must suppress provider details
            raise RuntimeError("Habitat authentication is unavailable; retry sign-in") from None

    def start(self, callback_uri: str, force_reauthentication: bool = False):
        from omnigent.server._habitat_proto import auth_pb2 as proto

        return self._call(
            "StartLogin",
            proto.StartLoginRequest(
                profile="omnigent",
                callback_uri=callback_uri,
                force_reauthentication=force_reauthentication,
            ),
            proto.StartLoginResponse,
        )

    def complete(self, transaction: str, callback_uri: str, state: str, code: str):
        from omnigent.server._habitat_proto import auth_pb2 as proto

        return self._call(
            "CompleteLogin",
            proto.CompleteLoginRequest(
                profile="omnigent",
                transaction_credential=transaction,
                callback_uri=callback_uri,
                state=state,
                authorization_code=code,
            ),
            proto.CompleteLoginResponse,
        )

    def get_token(self, session: str):
        from omnigent.server._habitat_proto import auth_pb2 as proto

        return self._call(
            "GetClientToken",
            proto.GetClientTokenRequest(profile="omnigent", session_credential=session),
            proto.GetClientTokenResponse,
        )

    def logout(self, session: str) -> None:
        from omnigent.server._habitat_proto import auth_pb2 as proto

        self._call(
            "Logout",
            proto.LogoutRequest(profile="omnigent", session_credential=session),
            proto.LogoutResponse,
        )
