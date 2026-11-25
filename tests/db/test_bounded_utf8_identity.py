"""Codec contract for exact bounded UTF-8 ACP identities."""

from __future__ import annotations

import pytest
from sqlalchemy.dialects import mysql, postgresql, sqlite

from omnigent.db.db_models import BoundedUtf8Identity


def test_mysql_identity_codec_round_trips_exact_multibyte_limits() -> None:
    codec = BoundedUtf8Identity(max_characters=512, max_utf8_bytes=512)
    dialect = mysql.dialect()
    adapted = codec.dialect_impl(dialect)
    identity = "📨" * 128
    assert len(identity) == 128
    assert len(identity.encode("utf-8")) == 512
    assert adapted.max_characters == 512
    assert adapted.max_utf8_bytes == 512
    assert adapted.compile(dialect=dialect) == "VARBINARY(512)"

    bind = adapted.bind_processor(dialect)
    result = adapted.result_processor(dialect, None)
    assert bind is not None and result is not None
    encoded = bind(identity)
    assert encoded == identity.encode("utf-8")
    assert result(encoded) == identity
    assert result(bytearray(encoded)) == identity
    assert result(memoryview(encoded)) == identity
    assert result(identity) == identity
    assert bind(None) is None
    assert result(None) is None


@pytest.mark.parametrize(
    ("max_characters", "max_utf8_bytes", "value", "expected"),
    [
        (128, 512, "🧭" * 128, "VARBINARY(512)"),
        (512, 512, "🧭" * 128, "VARBINARY(512)"),
    ],
)
def test_mysql_epoch_and_session_declarations_keep_independent_character_bounds(
    max_characters: int,
    max_utf8_bytes: int,
    value: str,
    expected: str,
) -> None:
    codec = BoundedUtf8Identity(
        max_characters=max_characters,
        max_utf8_bytes=max_utf8_bytes,
    )
    dialect = mysql.dialect()
    adapted = codec.dialect_impl(dialect)
    bind = adapted.bind_processor(dialect)
    assert bind is not None
    assert adapted.compile(dialect=dialect) == expected
    assert bind(value) == value.encode("utf-8")


def test_native_identity_distinctions_are_never_normalized() -> None:
    codec = BoundedUtf8Identity(max_characters=512, max_utf8_bytes=512)
    dialect = mysql.dialect()
    adapted = codec.dialect_impl(dialect)
    bind = adapted.bind_processor(dialect)
    result = adapted.result_processor(dialect, None)
    assert bind is not None and result is not None
    values = ("Agent", "agent", "cafe", "café", "é", "e\u0301", "native", "native ")
    encoded = [bind(value) for value in values]
    assert len(set(encoded)) == len(values)
    assert [result(value) for value in encoded] == list(values)


@pytest.mark.parametrize(
    ("value", "exception", "message"),
    [
        ("", TypeError, "non-empty string"),
        (b"native-id", TypeError, "non-empty string"),
        ("a" * 513, ValueError, "character limit"),
        ("é" * 257, ValueError, "byte limit"),
        ("\ud800", ValueError, "valid Unicode"),
    ],
)
def test_mysql_identity_codec_rejects_invalid_or_oversized_binds(
    value: object,
    exception: type[Exception],
    message: str,
) -> None:
    codec = BoundedUtf8Identity(max_characters=512, max_utf8_bytes=512)
    with pytest.raises(exception, match=message):
        codec.dialect_impl(mysql.dialect()).process_bind_param(value, mysql.dialect())


def test_epoch_identity_codec_enforces_128_characters_but_allows_512_bytes() -> None:
    codec = BoundedUtf8Identity(max_characters=128, max_utf8_bytes=512)
    dialect = mysql.dialect()
    adapted = codec.dialect_impl(dialect)
    bind = adapted.bind_processor(dialect)
    assert bind is not None
    maximum = "🧭" * 128
    assert len(maximum.encode("utf-8")) == 512
    assert bind(maximum) == maximum.encode("utf-8")
    with pytest.raises(ValueError, match="character limit"):
        bind("a" * 129)


@pytest.mark.parametrize("value", [b"\xff", bytearray(b"\xff"), memoryview(b"\xff")])
def test_mysql_identity_codec_rejects_malformed_persisted_utf8(value: object) -> None:
    with pytest.raises(ValueError, match="stored bounded UTF-8 identity is malformed"):
        BoundedUtf8Identity(max_characters=512, max_utf8_bytes=512).process_result_value(
            value, mysql.dialect()
        )


def test_non_mysql_codecs_keep_bounded_string_representation_and_none() -> None:
    value = "e\u0301 "
    for dialect in (sqlite.dialect(), postgresql.dialect()):
        codec = BoundedUtf8Identity(max_characters=512, max_utf8_bytes=512)
        adapted = codec.dialect_impl(dialect)
        bind = adapted.bind_processor(dialect)
        result = adapted.result_processor(dialect, None)
        assert bind is not None and result is not None
        assert adapted.compile(dialect=dialect) == "VARCHAR(512)"
        assert bind(value) == value
        assert result(value) == value
        assert bind(None) is None
        assert result(None) is None
        with pytest.raises(ValueError, match="byte limit"):
            bind("é" * 257)


def test_identity_limits_survive_copy_dialect_adaptation_and_cache_keying() -> None:
    original = BoundedUtf8Identity(max_characters=128, max_utf8_bytes=512)
    same_limits = BoundedUtf8Identity(max_characters=128, max_utf8_bytes=512)
    different_character_limit = BoundedUtf8Identity(
        max_characters=129,
        max_utf8_bytes=512,
    )
    different_byte_limit = BoundedUtf8Identity(max_characters=128, max_utf8_bytes=513)

    assert original._static_cache_key == same_limits._static_cache_key
    assert original._static_cache_key != different_character_limit._static_cache_key
    assert original._static_cache_key != different_byte_limit._static_cache_key

    copied = original.copy()
    for adapted in (copied, original.dialect_impl(mysql.dialect())):
        assert adapted.max_characters == 128
        assert adapted.max_utf8_bytes == 512
        assert adapted._static_cache_key == original._static_cache_key

    value = "🧭" * 128
    mysql_type = original.dialect_impl(mysql.dialect())
    bind = mysql_type.bind_processor(mysql.dialect())
    assert bind is not None
    assert bind(value) == value.encode("utf-8")
