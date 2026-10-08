"""Tests for the read-only ``omnigent debug db-check`` compatibility check.

Regression guard for the incident where a server image rolled forward (or
back) onto a database at a merge revision (``m017dd1006a``) that the image's
own migration graph didn't know about (truncated at ``m016dd0930a``) — the
server auto-migration path misreported this as a routine out-of-date schema
and attempted (and could partially apply) a migration against an
incompatible database. ``db-check`` lets a release process catch this
*before* the server starts, purely by inspecting revisions — it must never
write to the database or run a migration itself.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from click.testing import CliRunner

from omnigent.cli import cli
from omnigent.db import utils as db_utils
from omnigent.db.utils import _build_alembic_config, _check_db_revision_compatibility

# The real incident revisions. m017dd1006a merges two branches, one of which
# is mm1a2b3c4d5e; dropping only m017 would leave two heads (m016dd0930a and
# mm1a2b3c4d5e) in the truncated graph, so both are excluded to leave
# m016dd0930a as the sole, legitimate head of the "older candidate" graph.
_M016 = "m016dd0930a"
_M017 = "m017dd1006a"
_EXCLUDE_FOR_PRE_M017_CANDIDATE = (_M017, "mm1a2b3c4d5e")


def _truncated_migrations_config(
    tmp_path: Path, db_uri: str, *, exclude: tuple[str, ...]
) -> Config:
    """Build an Alembic ``Config`` whose migration graph omits *exclude*.

    Copies ``versions/*.py`` from the real migrations directory into a fresh
    tmp directory, skipping any file whose name starts with one of the
    *exclude* revision ids, and points a new ``Config`` at the copy. Only the
    version scripts are needed — ``ScriptDirectory`` parses ``revision`` /
    ``down_revision`` without executing ``env.py`` or ``script.py.mako``.
    """
    src_versions = Path(db_utils.__file__).parent / "migrations" / "versions"
    dst_migrations = tmp_path / "migrations"
    dst_versions = dst_migrations / "versions"
    dst_versions.mkdir(parents=True)
    for script_file in src_versions.glob("*.py"):
        if any(script_file.name.startswith(revision) for revision in exclude):
            continue
        dst_versions.joinpath(script_file.name).write_bytes(script_file.read_bytes())

    alembic_ini = Path(db_utils.__file__).parent / "alembic.ini"
    config = Config(str(alembic_ini))
    db_utils._set_alembic_database_url(config, db_uri)
    config.set_main_option("script_location", str(dst_migrations))
    return config


def _upgrade_to(uri: str, engine: sa.Engine, revision: str) -> None:
    config = _build_alembic_config(uri)
    with engine.begin() as conn:
        config.attributes["connection"] = conn
        command.upgrade(config, revision)


def _table_names(db_path: Path) -> set[str]:
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    finally:
        conn.close()
    return {row[0] for row in rows}


def _alembic_version_rows(db_path: Path) -> list[str]:
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute("SELECT version_num FROM alembic_version").fetchall()
    finally:
        conn.close()
    return sorted(r[0] for r in rows)


def test_incompatible_schema_matches_the_m017_incident(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """DB at m017dd1006a vs. a candidate image that predates it -> exit 1."""
    db_path = tmp_path / "incident.db"
    uri = f"sqlite:///{db_path}"
    engine = sa.create_engine(uri)
    _upgrade_to(uri, engine, _M017)
    engine.dispose()

    truncated_config = _truncated_migrations_config(
        tmp_path, uri, exclude=_EXCLUDE_FOR_PRE_M017_CANDIDATE
    )
    monkeypatch.setattr(db_utils, "_build_alembic_config", lambda _db_uri: truncated_config)

    result = _check_db_revision_compatibility(uri)

    assert result.exit_code == 1
    assert result.current == _M017
    assert result.head == _M016


def test_needs_migration_when_behind_the_real_head(tmp_path: Path) -> None:
    """DB at an earlier known revision vs. the full real head -> exit 2."""
    db_path = tmp_path / "behind-head.db"
    uri = f"sqlite:///{db_path}"
    engine = sa.create_engine(uri)
    _upgrade_to(uri, engine, _M016)
    engine.dispose()

    result = _check_db_revision_compatibility(uri)

    assert result.exit_code == 2
    assert result.current == _M016
    assert result.head == _M017


def test_connection_failure_for_unreachable_sqlite(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A sqlite URL whose parent directory is missing -> exit 1."""
    missing = tmp_path / "no-such-dir" / "chat.db"
    monkeypatch.setenv("OMNIGENT_DB_URL", f"sqlite:///{missing}")

    result = CliRunner().invoke(cli, ["debug", "db-check"])

    assert result.exit_code == 1
    assert "does not exist" in result.output


def test_incompatible_for_garbage_alembic_version(tmp_path: Path) -> None:
    """An alembic_version row holding an unknown revision -> exit 1."""
    db_path = tmp_path / "garbage.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL)")
        conn.execute("INSERT INTO alembic_version (version_num) VALUES ('totally-unknown-rev')")
        conn.commit()
    finally:
        conn.close()

    result = _check_db_revision_compatibility(f"sqlite:///{db_path}")

    assert result.exit_code == 1
    assert "totally-unknown-rev" in result.message


def test_no_migration_side_effects_when_behind_head(tmp_path: Path) -> None:
    """Running the check against a behind-head DB must not mutate it."""
    db_path = tmp_path / "no-side-effects.db"
    uri = f"sqlite:///{db_path}"
    engine = sa.create_engine(uri)
    _upgrade_to(uri, engine, _M016)
    engine.dispose()

    before_bytes = db_path.read_bytes()
    before_tables = _table_names(db_path)
    before_version = _alembic_version_rows(db_path)

    result = _check_db_revision_compatibility(uri)
    assert result.exit_code == 2  # sanity: this is the "behind head" case

    assert db_path.read_bytes() == before_bytes, "db-check must not write to the database file"
    assert _table_names(db_path) == before_tables, "db-check must not create or drop tables"
    assert _alembic_version_rows(db_path) == before_version, (
        "db-check must not advance alembic_version"
    )


def test_cli_requires_omnigent_db_url_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OMNIGENT_DB_URL", raising=False)

    result = CliRunner().invoke(cli, ["debug", "db-check"])

    assert result.exit_code != 0
    assert "OMNIGENT_DB_URL" in result.output


def test_cli_reports_up_to_date_with_exit_zero(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = tmp_path / "up-to-date.db"
    uri = f"sqlite:///{db_path}"
    engine = sa.create_engine(uri)
    _upgrade_to(uri, engine, "head")
    engine.dispose()

    monkeypatch.setenv("OMNIGENT_DB_URL", uri)
    result = CliRunner().invoke(cli, ["debug", "db-check"])

    assert result.exit_code == 0
    assert "up to date" in result.output
