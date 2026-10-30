"""Tests for ``OMNIGENT_MIDDLEWARE_MODULES`` external ASGI middlewares."""

from __future__ import annotations

import sys
import types
from collections.abc import Iterator
from pathlib import Path

import pytest
from starlette.testclient import TestClient
from starlette.types import ASGIApp, Receive, Scope, Send
from starlette.websockets import WebSocketDisconnect

from omnigent.runtime.agent_cache import AgentCache
from omnigent.server.app import create_app
from omnigent.server.external_middleware import (
    MIDDLEWARE_MODULES_ENV,
    load_external_middlewares,
)
from omnigent.stores.agent_store.sqlalchemy_store import SqlAlchemyAgentStore
from omnigent.stores.artifact_store.local import LocalArtifactStore
from omnigent.stores.conversation_store.sqlalchemy_store import SqlAlchemyConversationStore
from omnigent.stores.file_store.sqlalchemy_store import SqlAlchemyFileStore


def _tagger(tag: str, log: list[str]):
    def factory(app: ASGIApp) -> ASGIApp:
        async def middleware(scope: Scope, receive: Receive, send: Send) -> None:
            log.append(tag)
            await app(scope, receive, send)

        return middleware

    return factory


def _deny_paths(prefix: str, seen: list[tuple[str, str]]):
    """Deny HTTP (403) and close WebSockets whose path starts with ``prefix``."""

    def factory(app: ASGIApp) -> ASGIApp:
        async def middleware(scope: Scope, receive: Receive, send: Send) -> None:
            seen.append((scope["type"], scope.get("path", "")))
            if scope["type"] == "http" and scope["path"].startswith(prefix):
                await send({"type": "http.response.start", "status": 403, "headers": []})
                await send({"type": "http.response.body", "body": b"killed"})
                return
            if scope["type"] == "websocket" and scope["path"].startswith(prefix):
                await send({"type": "websocket.close", "code": 1008})
                return
            await app(scope, receive, send)

        return middleware

    return factory


@pytest.fixture()
def fake_modules(monkeypatch: pytest.MonkeyPatch) -> Iterator[dict[str, types.ModuleType]]:
    created: dict[str, types.ModuleType] = {}

    def add(name: str, **attrs: object) -> types.ModuleType:
        module = types.ModuleType(name)
        for key, value in attrs.items():
            setattr(module, key, value)
        monkeypatch.setitem(sys.modules, name, module)
        created[name] = module
        return module

    created["__add__"] = add  # type: ignore[assignment]
    yield created


@pytest.mark.parametrize("value", [None, "", " , "])
def test_unset_or_empty_loads_nothing(monkeypatch: pytest.MonkeyPatch, value: str | None) -> None:
    if value is None:
        monkeypatch.delenv(MIDDLEWARE_MODULES_ENV, raising=False)
    else:
        monkeypatch.setenv(MIDDLEWARE_MODULES_ENV, value)
    assert load_external_middlewares({}) == []


def test_order_across_modules_and_cfg_passed(
    monkeypatch: pytest.MonkeyPatch, fake_modules
) -> None:
    add = fake_modules["__add__"]
    log: list[str] = []
    received: list[object] = []

    def make(tags: list[str]):
        def create_middlewares(cfg):
            received.append(cfg)
            return [_tagger(tag, log) for tag in tags]

        return create_middlewares

    add("mw_one", create_middlewares=make(["a", "b"]))
    add("mw_two", create_middlewares=make(["c"]))
    monkeypatch.setenv(MIDDLEWARE_MODULES_ENV, "mw_one, mw_two")
    cfg = {"port": 1}
    middlewares = load_external_middlewares(cfg)
    assert received == [cfg, cfg]

    async def inner(scope, receive, send):
        log.append("inner")

    app: ASGIApp = inner
    for factory in reversed(middlewares):
        app = factory(app)
    import asyncio

    asyncio.run(app({"type": "http"}, None, None))  # type: ignore[arg-type]
    assert log == ["a", "b", "c", "inner"]


def _raise(cfg):
    raise ValueError("boom")


@pytest.mark.parametrize(
    ("attrs", "message"),
    [
        (None, "Could not import"),
        ({}, "does not expose a callable"),
        ({"create_middlewares": 5}, "does not expose a callable"),
        ({"create_middlewares": _raise}, "raised while building"),
        ({"create_middlewares": lambda cfg: None}, "expected a sequence"),
        ({"create_middlewares": lambda cfg: "abc"}, "expected a sequence"),
        ({"create_middlewares": lambda cfg: [1]}, "expected a sequence"),
    ],
)
def test_errors_raise_runtime_error(
    monkeypatch: pytest.MonkeyPatch, fake_modules, attrs, message: str
) -> None:
    name = "mw_missing_xyz" if attrs is None else "mw_bad"
    if attrs is not None:
        fake_modules["__add__"](name, **attrs)
    monkeypatch.setenv(MIDDLEWARE_MODULES_ENV, name)
    with pytest.raises(RuntimeError, match=message):
        load_external_middlewares({})


def test_server_app_outermost_with_auth_and_base_path(
    runtime_init: None, db_uri: str, tmp_path: Path
) -> None:
    """External middleware runs before auth and BasePathMiddleware (sees the prefix)."""
    from omnigent.server.auth import UnifiedAuthProvider

    seen: list[tuple[str, str]] = []
    artifact_store = LocalArtifactStore(str(tmp_path / "artifacts"))
    app = create_app(
        agent_store=SqlAlchemyAgentStore(db_uri),
        file_store=SqlAlchemyFileStore(db_uri),
        conversation_store=SqlAlchemyConversationStore(db_uri),
        artifact_store=artifact_store,
        agent_cache=AgentCache(artifact_store=artifact_store, cache_dir=tmp_path / "cache"),
        auth_provider=UnifiedAuthProvider(source="header"),
        base_path="/omni",
        external_middlewares=[_deny_paths("/omni/v1", seen)],
    )
    with TestClient(app) as client:  # lifespan passes through the middleware
        # Unauthenticated: the kill switch answers before auth would reject it.
        response = client.get("/omni/v1/sessions")
        assert response.status_code == 403
        assert response.text == "killed"
        with pytest.raises(WebSocketDisconnect) as excinfo:
            with client.websocket_connect("/omni/v1/anything") as ws:
                ws.receive_text()
        assert excinfo.value.code == 1008
        # Non-denied path still reaches omnigent.
        assert client.get("/omni/health").status_code != 403
    assert ("lifespan", "") in seen
    assert ("http", "/omni/v1/sessions") in seen
