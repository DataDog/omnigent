"""Unit tests for the flat session ``token_usage`` projection (VIT-2027)."""

from __future__ import annotations

from typing import Any

import pytest

from omnigent.server.routes._sessions.helpers import _token_usage_for_display
from omnigent.server.schemas import ModelUsage


@pytest.mark.parametrize(
    ("usage", "expected"),
    [
        pytest.param(
            {"input_tokens": 1200, "output_tokens": 340, "total_tokens": 1540},
            ModelUsage(input_tokens=1200, output_tokens=340, total_tokens=1540),
            id="unpriced-model-less",
        ),
        pytest.param(
            {
                "input_tokens": "1000",
                "output_tokens": 500,
                "cache_read_input_tokens": 10,
                "total_cost_usd": 0.42,
                "by_model": {"m": {"input_tokens": 1000}},
            },
            ModelUsage(
                input_tokens=1000,
                output_tokens=500,
                cache_read_input_tokens=10,
                total_cost_usd=0.42,
            ),
            id="priced-carries-cost-and-coerces",
        ),
        pytest.param({}, None, id="empty"),
        pytest.param(
            {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}, None, id="all-zero"
        ),
        pytest.param(
            {"input_tokens": "garbage", "output_tokens": None, "total_tokens": [1]},
            None,
            id="garbage",
        ),
        pytest.param({"total_cost_usd": 1.0}, None, id="cost-without-tokens"),
    ],
)
def test_token_usage_for_display(usage: dict[str, Any], expected: ModelUsage | None) -> None:
    """Flat totals project regardless of model; no real tokens yields ``None``."""
    assert _token_usage_for_display(usage) == expected
