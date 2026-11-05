"""Contract tests for the ``sys_ask_user`` tool and its shared helpers.

Covers the three question shapes (free-form, single choice, multiple
choice), invalid-argument rejection before any pending state exists, the
generated ``requestedSchema`` shapes, answer validation, and the schema-only
builtin surface.
"""

from __future__ import annotations

import json

import pytest

from omnigent.tools.builtins.sys_ask_user import (
    ANSWER_FIELD,
    AskUserQuestion,
    SysAskUserTool,
    build_requested_schema,
    format_tool_result,
    question_fingerprint,
    validate_answer_content,
    validate_ask_user_args,
)


class TestValidateArgs:
    def test_free_form(self) -> None:
        result = validate_ask_user_args({"question": "What next?"})
        assert isinstance(result, AskUserQuestion)
        assert result.question == "What next?"
        assert result.options == ()
        assert result.multiple is False
        assert result.is_free_form

    def test_single_choice(self) -> None:
        result = validate_ask_user_args(
            {"question": "Ship?", "options": ["yes", "no"]}
        )
        assert isinstance(result, AskUserQuestion)
        assert result.options == ("yes", "no")
        assert result.multiple is False

    def test_multiple_choice(self) -> None:
        result = validate_ask_user_args(
            {"question": "Include?", "options": ["a", "b"], "multiple": True}
        )
        assert isinstance(result, AskUserQuestion)
        assert result.multiple is True

    @pytest.mark.parametrize(
        "args",
        [
            {},
            {"question": ""},
            {"question": "   "},
            {"question": 42},
            {"question": "q", "options": "not-a-list"},
            {"question": "q", "options": []},
            {"question": "q", "options": ["a", "a"]},
            {"question": "q", "options": ["a", ""]},
            {"question": "q", "options": [1, 2]},
            {"question": "q", "multiple": True},
            {"question": "q", "multiple": "yes"},
        ],
    )
    def test_invalid_inputs_are_rejected(self, args: dict) -> None:
        result = validate_ask_user_args(args)
        assert isinstance(result, str), f"expected error for {args}, got {result!r}"

    def test_option_order_is_preserved(self) -> None:
        result = validate_ask_user_args(
            {"question": "q", "options": ["z", "a", "m"]}
        )
        assert isinstance(result, AskUserQuestion)
        assert result.options == ("z", "a", "m")


class TestRequestedSchema:
    def test_free_form_schema(self) -> None:
        question = validate_ask_user_args({"question": "Name the branch"})
        assert isinstance(question, AskUserQuestion)
        schema = build_requested_schema(question)
        answer = schema["properties"][ANSWER_FIELD]
        assert schema["required"] == [ANSWER_FIELD]
        assert answer == {"type": "string", "minLength": 1}

    def test_single_choice_schema_is_string_enum(self) -> None:
        question = validate_ask_user_args(
            {"question": "Env?", "options": ["staging", "prod"]}
        )
        assert isinstance(question, AskUserQuestion)
        schema = build_requested_schema(question)
        answer = schema["properties"][ANSWER_FIELD]
        assert answer["type"] == "string"
        assert answer["enum"] == ["staging", "prod"]

    def test_multiple_choice_schema_is_enum_array(self) -> None:
        question = validate_ask_user_args(
            {"question": "Pick", "options": ["a", "b", "c"], "multiple": True}
        )
        assert isinstance(question, AskUserQuestion)
        schema = build_requested_schema(question)
        answer = schema["properties"][ANSWER_FIELD]
        assert answer["type"] == "array"
        assert answer["items"] == {"type": "string", "enum": ["a", "b", "c"]}
        assert answer["minItems"] == 1
        assert answer["uniqueItems"] is True


class TestValidateAnswerContent:
    def _question(self, multiple: bool = False) -> AskUserQuestion:
        result = validate_ask_user_args(
            {"question": "q", "options": ["a", "b"], "multiple": multiple}
        )
        assert isinstance(result, AskUserQuestion)
        return result

    def test_free_form_accepts_text(self) -> None:
        question = validate_ask_user_args({"question": "q"})
        assert isinstance(question, AskUserQuestion)
        assert validate_answer_content({"answer": "hello"}, question) == {
            "answer": "hello"
        }

    def test_free_form_rejects_blank(self) -> None:
        question = validate_ask_user_args({"question": "q"})
        assert isinstance(question, AskUserQuestion)
        assert isinstance(validate_answer_content({"answer": "  "}, question), str)

    def test_single_choice_rejects_non_option(self) -> None:
        assert isinstance(validate_answer_content({"answer": "z"}, self._question()), str)

    def test_multiple_requires_at_least_one_selection(self) -> None:
        assert isinstance(
            validate_answer_content({"answer": []}, self._question(multiple=True)), str
        )

    def test_multiple_rejects_duplicates(self) -> None:
        assert isinstance(
            validate_answer_content(
                {"answer": ["a", "a"]}, self._question(multiple=True)
            ),
            str,
        )

    def test_multiple_accepts_distinct_options(self) -> None:
        content = validate_answer_content(
            {"answer": ["b", "a"]}, self._question(multiple=True)
        )
        assert content == {"answer": ["b", "a"]}

    def test_none_content_is_invalid(self) -> None:
        assert isinstance(validate_answer_content(None, self._question()), str)

    def test_extra_fields_are_rejected(self) -> None:
        assert isinstance(
            validate_answer_content({"answer": "a", "extra": 1}, self._question()), str
        )


class TestFingerprintAndResult:
    def test_fingerprint_covers_payload(self) -> None:
        q1 = validate_ask_user_args({"question": "q", "options": ["a", "b"]})
        q2 = validate_ask_user_args({"question": "q", "options": ["b", "a"]})
        q3 = validate_ask_user_args({"question": "q", "options": ["a", "b"]})
        assert isinstance(q1, AskUserQuestion)
        assert isinstance(q2, AskUserQuestion)
        assert isinstance(q3, AskUserQuestion)
        assert question_fingerprint(q1) != question_fingerprint(q2)
        assert question_fingerprint(q1) == question_fingerprint(q3)
        json.loads(question_fingerprint(q1))  # canonical JSON

    def test_result_carries_answer_only_on_accept(self) -> None:
        assert json.loads(format_tool_result(action="accept", answer="staging")) == {
            "action": "accept",
            "answer": "staging",
        }
        assert json.loads(format_tool_result(action="decline")) == {"action": "decline"}
        assert json.loads(format_tool_result(action="accept")) == {"action": "accept"}
        assert json.loads(
            format_tool_result(action="accept", answer=["a", "b"])
        ) == {"action": "accept", "answer": ["a", "b"]}


class TestBuiltinSurface:
    def test_schema_only_invoke_fails_loud(self) -> None:
        tool = SysAskUserTool()
        with pytest.raises(NotImplementedError):
            tool.invoke("{}", None)  # type: ignore[arg-type]

    def test_schema_lists_all_three_shapes(self) -> None:
        schema = SysAskUserTool().get_schema()
        assert schema["type"] == "function"
        fn = schema["function"]
        assert fn["name"] == "sys_ask_user"
        properties = fn["parameters"]["properties"]
        assert set(properties) == {"question", "options", "multiple"}
        assert fn["parameters"]["required"] == ["question"]
