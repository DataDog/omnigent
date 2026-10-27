"""Tests that ``omnigent debug db-upgrade`` never prints database credentials.

The URL may be passed positionally or via ``OMNIGENT_DB_URL`` (which keeps it
out of ``ps``). Either way, the password and credential-like query params must
not reach stdout/stderr, and an unparseable URL must never be echoed raw.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from click.testing import CliRunner, Result

from omnigent.cli import cli

_SECRET = "s3cretPW"
_PG_URL = f"postgresql+psycopg://omnigent:{_SECRET}@db.example.com:5432/omnigent"


class _FakeEngine:
    def dispose(self) -> None:
        pass


@pytest.fixture
def migrated(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Stub engine creation and migrations; return the URLs that were migrated."""
    import sqlalchemy

    import omnigent.db.utils

    urls: list[str] = []
    monkeypatch.setattr(sqlalchemy, "create_engine", lambda url, **_: _FakeEngine())
    monkeypatch.setattr(
        omnigent.db.utils, "_run_migrations", lambda _engine, url: urls.append(url)
    )
    monkeypatch.delenv("OMNIGENT_DB_URL", raising=False)
    return urls


def _upgrade(*args: str, env: dict[str, str] | None = None) -> Result:
    return CliRunner().invoke(cli, ["debug", "db-upgrade", *args], env=env)


def _all_output(result: Result) -> str:
    return result.stdout + result.stderr


def test_positional_password_is_redacted_and_warns(migrated: list[str]) -> None:
    result = _upgrade(_PG_URL)

    assert result.exit_code == 0, result.output
    assert _SECRET not in _all_output(result)
    assert "Upgrading postgresql+psycopg://omnigent:***@db.example.com:5432/omnigent" in (
        result.stdout
    )
    assert "OMNIGENT_DB_URL" in result.stderr
    assert migrated == [_PG_URL]


def test_env_var_url_is_redacted_and_attributed(migrated: list[str]) -> None:
    result = _upgrade(env={"OMNIGENT_DB_URL": _PG_URL})

    assert result.exit_code == 0, result.output
    assert _SECRET not in _all_output(result)
    assert "(from OMNIGENT_DB_URL)" in result.stdout
    assert result.stderr == ""
    assert migrated == [_PG_URL]


def test_positional_url_wins_over_env_var(migrated: list[str], tmp_path: Path) -> None:
    db_path = tmp_path / "chat.db"
    sqlite3.connect(db_path).close()

    result = _upgrade(f"sqlite:///{db_path}", env={"OMNIGENT_DB_URL": _PG_URL})

    assert result.exit_code == 0, result.output
    assert migrated == [f"sqlite:///{db_path}"]
    assert "from OMNIGENT_DB_URL" not in result.stdout


@pytest.mark.parametrize("password", ["s3/cretPW", "s3?cretPW", "s3#cretPW"])
def test_password_with_url_delimiters_is_redacted(migrated: list[str], password: str) -> None:
    """SQLAlchemy accepts unencoded ``/ ? #`` in passwords; none may leak."""
    url = f"postgresql+psycopg://omnigent:{password}@db.example.com:5432/omnigent"

    result = _upgrade(url)

    assert result.exit_code == 0, result.output
    assert "cretPW" not in _all_output(result)
    assert "omnigent:***@db.example.com:5432/omnigent" in result.stdout
    assert migrated == [url]


def test_azure_style_username_password_is_redacted(migrated: list[str]) -> None:
    """An unencoded ``@`` in the username (``user@server``) must not expose the password."""
    url = f"postgresql+psycopg://adm@srv:{_SECRET}@db.example.com:5432/omnigent"

    result = _upgrade(url)

    assert result.exit_code == 0, result.output
    assert _SECRET not in _all_output(result)
    assert "adm%40srv:***@db.example.com:5432/omnigent" in result.stdout
    assert migrated == [url]


@pytest.mark.parametrize("env", [None, {"OMNIGENT_DB_URL": ""}])
def test_missing_url_is_a_usage_error(migrated: list[str], env: dict[str, str] | None) -> None:
    result = _upgrade(env=env)

    assert result.exit_code == 2
    assert "OMNIGENT_DB_URL" in result.stderr
    assert migrated == []


@pytest.mark.parametrize("key", ["password", "sslpassword"])
def test_query_string_credentials_are_redacted(migrated: list[str], key: str) -> None:
    result = _upgrade(f"postgresql://db.example.com/omnigent?sslmode=require&{key}={_SECRET}")

    assert result.exit_code == 0, result.output
    assert _SECRET not in _all_output(result)
    assert f"{key}=REDACTED" in result.stdout
    assert "OMNIGENT_DB_URL" in result.stderr


@pytest.mark.parametrize("source", ["positional", "env"])
@pytest.mark.parametrize(
    "raw",
    [
        f"not a url {_SECRET}",
        # A non-numeric port makes make_url raise ValueError, not ArgumentError.
        f"postgresql://omnigent:{_SECRET}@db.example.com:notaport/omnigent",
    ],
)
def test_unparseable_url_is_never_echoed(migrated: list[str], source: str, raw: str) -> None:
    if source == "positional":
        result = _upgrade(raw)
    else:
        result = _upgrade(env={"OMNIGENT_DB_URL": raw})

    assert result.exit_code != 0
    assert "Invalid database URL" in result.stderr
    assert _SECRET not in _all_output(result)
    assert migrated == []


def test_unparseable_url_display_never_falls_back_to_raw(
    migrated: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Even if validation let a malformed URL through, the echo stays redacted."""
    import omnigent.cli

    monkeypatch.setattr(omnigent.cli, "_require_existing_sqlite_db", lambda _url: None)
    raw = f"not-a-url-{_SECRET}"

    result = _upgrade(raw)

    assert _SECRET not in _all_output(result)
    assert "Upgrading <unparseable database URL> ..." in result.stdout


def test_real_sqlite_upgrade(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OMNIGENT_DB_URL", raising=False)
    db_path = tmp_path / "chat.db"
    sqlite3.connect(db_path).close()

    result = _upgrade(f"sqlite:///{db_path}")

    assert result.exit_code == 0, result.output
    assert f"Upgrading sqlite:///{db_path} ..." in result.stdout
    assert "Upgrade complete." in result.stdout
    assert result.stderr == ""
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT version_num FROM alembic_version").fetchone()
