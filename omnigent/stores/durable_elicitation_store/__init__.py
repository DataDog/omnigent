"""Durable-elicitation store — persists pending ``sys_ask_user`` questions.

The store owns the ``durable_elicitations`` table: one row per durable
human question a hosted agent parks on. Unlike the in-memory pending
index (:mod:`omnigent.runtime.pending_elicitations`), state here survives
Omnigent server restarts, so a question asked before a crash is still
answerable — and the answer still resumes the right session exactly once —
after a cold start.
"""

from __future__ import annotations

from omnigent.stores.durable_elicitation_store._model import (
    AnswerConflictError,
    DurableElicitation,
    DurableElicitationError,
    DuplicateRegistrationError,
)
from omnigent.stores.durable_elicitation_store.sqlalchemy_store import (
    SqlAlchemyDurableElicitationStore,
)

__all__ = [
    "DurableElicitation",
    "DurableElicitationError",
    "DuplicateRegistrationError",
    "AnswerConflictError",
    "SqlAlchemyDurableElicitationStore",
]
