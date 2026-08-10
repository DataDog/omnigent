"""Alembic environment configuration."""

from __future__ import annotations

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import Connection, engine_from_config, pool

from omnigent.db import ConversationBase, OmnigentBase

config = context.config

if config.config_file_name is not None:
    # disable_existing_loggers=False prevents fileConfig from setting
    # disabled=True on loggers created before migration runs, which
    # would silently suppress all their output for the rest of the
    # process lifetime.
    fileConfig(config.config_file_name, disable_existing_loggers=False)
    # alembic.ini sets ``[logger_alembic] level = INFO`` for verbose
    # debugging during migration authoring. End-user runs of
    # ``omnigent run -p`` would otherwise dump 3 INFO lines
    # per fresh DB to stderr. Honor the CLI's ``--verbose`` toggle
    # by checking the root logger: when the root is not at DEBUG
    # (i.e. ``--verbose`` was NOT passed), pull alembic back to
    # WARNING so migrations are silent on the success path. Errors
    # still surface.
    import logging as _logging  # local — env.py runs in alembic context

    if not _logging.getLogger().isEnabledFor(_logging.DEBUG):
        _logging.getLogger("alembic").setLevel(_logging.WARNING)

# Both bases share one physical DB and one migration lineage; autogenerate
# diffs the union of their metadata so neither side's tables look "extra".
target_metadata = [OmnigentBase.metadata, ConversationBase.metadata]

# Allow overriding the DB URL via environment variable.
db_url = os.environ.get("OMNIGENT_DB_URL")
if db_url:
    config.set_main_option("sqlalchemy.url", db_url)

# Resolve the Alembic version-tracking table name. ``_build_alembic_config``
# (omnigent/db/utils.py) sets the ``version_table`` main option from the
# ``OMNIGENT_ALEMBIC_VERSION_TABLE`` env var; when env.py is driven directly by
# the standalone ``alembic`` CLI that option is unset, so fall back to the env
# var itself. ``None`` lets Alembic use its built-in default (``alembic_version``).
# Deployments sharing a schema with another Alembic-managed app override this to
# avoid colliding on the default version table.
version_table = config.get_main_option("version_table") or os.environ.get(
    "OMNIGENT_ALEMBIC_VERSION_TABLE"
)


def run_migrations_offline() -> None:
    """
    Run migrations in 'offline' mode — emit SQL to stdout
    without connecting to the database.
    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        version_table=version_table,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """
    Run migrations in 'online' mode — connect to the database
    and apply migrations directly.

    If a shared connection was passed via config.attributes (e.g.
    from _run_migrations in db/utils.py), reuse it instead of
    creating a new connection pool. This is required for SQLite
    in-memory databases and avoids redundant connections.
    """
    connection = config.attributes.get("connection")
    if connection is not None:
        _run_with_connection(connection)
    else:
        connectable = engine_from_config(
            config.get_section(config.config_ini_section, {}),
            prefix="sqlalchemy.",
            poolclass=pool.NullPool,
        )
        with connectable.connect() as conn:
            _run_with_connection(conn)


def _run_with_connection(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        render_as_batch=True,  # required for SQLite ALTER TABLE support
        version_table=version_table,
    )
    with context.begin_transaction():
        context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
