"""Habitat authentication uses the application bearer and bounded RPCs."""

import sys
from types import SimpleNamespace

import grpc
import pytest

from omnigent.server.habitat_broker import HabitatBroker, habitat_broker_enabled


def test_backend_selection_controls_broker(monkeypatch):
    monkeypatch.setenv("OMNIGENT_SANDBOX_PROVIDER_MODULE", "omnigent_hab_launcher")
    monkeypatch.setenv("OMNIGENT_HAB_ENABLED", "true")
    assert habitat_broker_enabled()
    monkeypatch.setenv("OMNIGENT_SANDBOX_PROVIDER_MODULE", "another_provider")
    assert not habitat_broker_enabled()


def test_application_identity_and_fresh_login(monkeypatch):
    calls = []

    class Channel:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            return None

        def unary_unary(self, method, **_):
            def invoke(request, **kwargs):
                calls.append((method, request, kwargs))
                return object()

            return invoke

    monkeypatch.setattr(grpc, "secure_channel", lambda *_: Channel())
    monkeypatch.setitem(sys.modules, "omnigent_hab_launcher", SimpleNamespace())
    monkeypatch.setitem(
        sys.modules,
        "omnigent_hab_launcher.production",
        SimpleNamespace(get_broker_application_bearer=lambda: "application-secret"),
    )
    broker = HabitatBroker("https://api.hab.dog:443")
    broker.start("https://omnigent.us1.ddbuild.io/auth/callback", True)
    broker.get_token("session-secret")
    broker.logout("session-secret")
    assert calls[0][1].force_reauthentication is True
    assert calls[0][1].profile == "omnigent"
    assert calls[1][1].session_credential == "session-secret"
    assert calls[2][0] == "/hab.v1.AuthService/Logout"
    for _, _, kwargs in calls:
        assert kwargs == {
            "metadata": (("authorization", "Bearer application-secret"),),
            "timeout": 10,
        }


@pytest.mark.parametrize(
    "endpoint",
    ["http://api.hab.dog", "https://user:secret@api.hab.dog", "https://api.hab.dog/path"],
)
def test_broker_rejects_unsafe_origins(endpoint):
    with pytest.raises(ValueError):
        HabitatBroker(endpoint)
