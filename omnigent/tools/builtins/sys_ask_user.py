"""
LLM-callable ``sys_ask_user`` tool — ask the person a durable question.

One tool, three question shapes:

- **Free-form** — ``question`` only. The person answers with arbitrary text.
- **Single choice** — ``question`` + ``options``. Exactly one option is the
  answer.
- **Multiple choice** — ``question`` + ``options`` + ``multiple: true``. At
  least one option; duplicates are rejected.

The class owns the LLM-facing schema and argument validation. Execution is
**runner-dispatched** (like ``sys_timer_set``): the runner intercepts
``sys_ask_user`` in ``omnigent/runner/ask_user.py``, registers the question as
a *durable* elicitation with the Omnigent server, publishes the form card, and
parks until the person answers — possibly after Vitrine reconnects, and with
no execution-timeout ceiling while the question is pending. The shared
helpers here (validation, schema building, answer checking, fingerprinting)
keep the tool surface, the runner executor, and the server's resolve path
rejecting the same inputs.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from omnigent.tools.base import Tool

#: The single ``answer`` property every generated ``requestedSchema`` uses.
ANSWER_FIELD = "answer"

#: Upper bound on question length. Long enough for any realistic prompt;
#: short enough that a runaway model can't park a multi-KB card in the UI.
_MAX_QUESTION_LEN = 4_000

#: Upper bound on the number of options.
_MAX_OPTIONS = 20


@dataclass(frozen=True)
class AskUserQuestion:
    """
    A validated ``sys_ask_user`` question.

    :param question: The prompt shown to the person, e.g.
        ``"Which environment should I deploy to?"``.
    :param options: Ordered choices, or ``()`` for free-form. Order is
        preserved end-to-end — schema, UI card, and persisted payload.
    :param multiple: Whether several options may be selected. Only
        meaningful when ``options`` is non-empty.
    """

    question: str
    options: tuple[str, ...] = ()
    multiple: bool = False

    @property
    def is_free_form(self) -> bool:
        """Whether the question accepts arbitrary text instead of choices."""
        return not self.options


def validate_ask_user_args(args: dict[str, Any]) -> AskUserQuestion | str:
    """
    Validate parsed ``sys_ask_user`` arguments.

    Shared by the builtin's schema contract, the runner's
    ``_execute_ask_user``, and the server's resolve path so every surface
    rejects the same inputs with the same messages. Invalid input fails
    *before* any pending state is created.

    :param args: JSON-decoded argument mapping, e.g.
        ``{"question": "Ship?", "options": ["yes", "no"]}``.
    :returns: An :class:`AskUserQuestion` when valid, otherwise an error
        message naming the first invalid field, e.g.
        ``"question must be a non-empty string"``.
    """
    question = args.get("question")
    if not isinstance(question, str) or not question.strip():
        return "question must be a non-empty string"
    if len(question) > _MAX_QUESTION_LEN:
        return f"question must be at most {_MAX_QUESTION_LEN} characters"

    options_raw = args.get("options")
    if options_raw is None:
        options: tuple[str, ...] = ()
    else:
        if not isinstance(options_raw, list):
            return "options must be a list of strings"
        if not options_raw:
            return "options must be a non-empty list when provided"
        if len(options_raw) > _MAX_OPTIONS:
            return f"options must have at most {_MAX_OPTIONS} entries"
        for option in options_raw:
            if not isinstance(option, str) or not option.strip():
                return "options entries must be non-empty strings"
        if len(set(options_raw)) != len(options_raw):
            return "options entries must be distinct"
        options = tuple(options_raw)

    multiple = args.get("multiple", False)
    if not isinstance(multiple, bool):
        return "multiple must be a boolean"
    if multiple and not options:
        return "multiple requires options"

    return AskUserQuestion(question=question, options=options, multiple=multiple)


def build_requested_schema(question: AskUserQuestion) -> dict[str, Any]:
    """
    Build the MCP-style ``requestedSchema`` for a validated question.

    One required ``answer`` property:

    - free-form → ``{"type": "string", "minLength": 1}``
    - single choice → ``{"type": "string", "enum": [...]}``
    - multiple choice → ``{"type": "array", "items": {"type": "string",
      "enum": [...]}, "minItems": 1, "uniqueItems": true}``

    Option order is preserved so the UI card renders the choices in the
    order the model supplied.

    :param question: A validated question (see
        :func:`validate_ask_user_args`).
    :returns: The ``requestedSchema`` dict embedded in the elicitation
        params.
    """
    if question.is_free_form:
        answer: dict[str, Any] = {"type": "string", "minLength": 1}
    elif question.multiple:
        answer = {
            "type": "array",
            "items": {"type": "string", "enum": list(question.options)},
            "minItems": 1,
            "uniqueItems": True,
        }
    else:
        answer = {"type": "string", "enum": list(question.options)}
    return {
        "type": "object",
        "properties": {ANSWER_FIELD: answer},
        "required": [ANSWER_FIELD],
        "additionalProperties": False,
    }


def validate_answer_content(
    content: dict[str, Any] | None,
    question: AskUserQuestion,
) -> dict[str, Any] | str:
    """
    Check a resolved answer's ``content`` against the question's shape.

    Unlike the generic MCP schema validation, this is a *structured* check
    the resolve endpoint can return a per-field error for: the content must
    carry exactly the ``answer`` field, of the right type, and — for choice
    questions — with every selection drawn from the persisted options.
    Duplicate selections in a multiple-choice answer are rejected.

    :param content: The resolver's content map, e.g.
        ``{"answer": "staging"}``. ``None`` means "declined / no answer".
    :param question: The persisted question the answer responds to.
    :returns: The content unchanged when it fits, otherwise an error
        message, e.g. ``"answer must be one of: yes, no"``.
    """
    if content is None:
        return "answer content is required"
    if not isinstance(content, dict) or set(content.keys()) != {ANSWER_FIELD}:
        return f"content must carry exactly one field: {ANSWER_FIELD}"
    value = content[ANSWER_FIELD]
    if question.is_free_form:
        if not isinstance(value, str) or not value.strip():
            return "answer must be a non-empty string"
        return content
    if question.multiple:
        if not isinstance(value, list) or not value:
            return "answer must be a non-empty list of options"
        for item in value:
            if not isinstance(item, str) or item not in question.options:
                allowed = ", ".join(question.options)
                return f"answer selections must be among: {allowed}"
        if len(set(value)) != len(value):
            return "answer selections must be distinct"
        return content
    if not isinstance(value, str) or value not in question.options:
        allowed = ", ".join(question.options)
        return f"answer must be one of: {allowed}"
    return content


def question_fingerprint(question: AskUserQuestion) -> str:
    """
    Canonical fingerprint of a question's payload.

    Create-or-get keyed on workspace + session + invocation identity
    compares this fingerprint to decide whether a replayed registration
    carries the *same* question (idempotent) or a changed one (rejected).
    Options order is significant: ``["a", "b"]`` and ``["b", "a"]`` are
    different questions for fingerprinting purposes, matching how the UI
    renders them.

    :param question: The validated question.
    :returns: A canonical JSON string, e.g.
        ``'{"multiple": false, "options": ["yes", "no"], "question": "Ship?"}'``.
    """
    payload = {
        "question": question.question,
        "options": list(question.options),
        "multiple": question.multiple,
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def format_tool_result(
    *,
    action: str,
    answer: str | list[str] | None = None,
) -> str:
    """
    Format the tool output the parked execution resumes with.

    The LLM sees a compact JSON object carrying the elicitation action and,
    when accepted, the actual answer — free-form text or the selected
    option(s). No default answer is ever invented: a decline or cancel
    carries only the action.

    :param action: ``"accept"``, ``"decline"``, or ``"cancel"``.
    :param answer: The person's answer on accept; ``None`` otherwise.
    :returns: JSON string, e.g. ``'{"action": "accept", "answer": "staging"}'``.
    """
    result: dict[str, Any] = {"action": action}
    if action == "accept" and answer is not None:
        result["answer"] = answer
    return json.dumps(result)


class SysAskUserTool(Tool):
    """
    Ask the person a question and wait for the answer, durably.

    Schema-only builtin: ``invoke`` is runner-dispatched (see the module
    docstring), so the default :meth:`Tool.invoke` fail-loud applies. The
    runner's ``sys_ask_user`` intercept owns execution.
    """

    @classmethod
    def name(cls) -> str:
        """:returns: ``"sys_ask_user"``."""
        return "sys_ask_user"

    @classmethod
    def description(cls) -> str:
        """:returns: Description visible to the LLM in tool listings."""
        return (
            "Ask the user a question and wait for their answer. Pass "
            "question (required) plus an optional options list of distinct "
            "strings for a choice question; add multiple=true to allow "
            "several selections. Without options the user answers in free "
            "text. Blocks until the user responds — they may take a long "
            "time or respond after reconnecting. Returns "
            '{"action": "accept", "answer": ...} on an answer, or '
            '{"action": "decline"} / {"action": "cancel"} when the '
            "user declines or the question is cancelled."
        )

    def get_schema(self) -> dict[str, Any]:
        """
        :returns: OpenAI tool schema with ``question`` (string, required),
            ``options`` (array of distinct non-empty strings, optional), and
            ``multiple`` (boolean, optional, only with ``options``).
        """
        return {
            "type": "function",
            "function": {
                "name": self.name(),
                "description": self.description(),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "question": {
                            "type": "string",
                            "description": (
                                "The question to ask. Must be a non-empty "
                                "string; keep it specific enough that a "
                                "choice or short text answers it."
                            ),
                        },
                        "options": {
                            "type": "array",
                            "items": {"type": "string", "minLength": 1},
                            "minItems": 1,
                            "uniqueItems": True,
                            "description": (
                                "Optional ordered list of distinct choices. "
                                "Omit for a free-form text answer."
                            ),
                        },
                        "multiple": {
                            "type": "boolean",
                            "description": (
                                "When true (and options is given), the user "
                                "may select several options. Defaults to "
                                "false."
                            ),
                            "default": False,
                        },
                    },
                    "required": ["question"],
                    "additionalProperties": False,
                },
            },
        }
