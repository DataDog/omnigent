"""Tests for ``_spec_with_workdir_paths`` local-tool path resolution.

The runner resolves workdir-relative tool paths to absolute paths so
bundle-deployed file-based tools (``tools/python/foo.py``) load from the
extracted image. Callable-backed tools (``language ==
"omnigent-python-callable"``) store a DOTTED IMPORT PATH in the same
field — that must be left untouched. Joining a dotted path onto the
workdir corrupts ``pkg.mod.func`` into ``<workdir>/pkg.mod.func``, the
import fails, the tool never registers, and any ``tool_call`` policy
narrowed to it can never fire. These tests fail loudly if that
distinction regresses.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from omnigent.runner.app import ResolvedSpec, _rewrap_like, _spec_with_workdir_paths
from omnigent.spec.types import AgentSpec, LocalToolInfo


def _spec_with_tools(tools: list[LocalToolInfo]) -> AgentSpec:
    """Minimal AgentSpec carrying only the local tools under test."""
    return AgentSpec(spec_version=1, name="probe", local_tools=tools)


def test_callable_dotted_path_left_untouched() -> None:
    """A dotted callable path survives workdir resolution unchanged.

    The bug: the workdir got prepended onto the dotted import path, so
    the tool failed to import and never registered — defeating any
    tool_call policy on it.
    """
    spec = _spec_with_tools(
        [
            LocalToolInfo(
                name="calculate",
                path="tests.resources.examples._shared.tool_functions.calculate",
                language="omnigent-python-callable",
            )
        ]
    )

    resolved = _spec_with_workdir_paths(spec, Path("/tmp/agent-image"))

    assert resolved.local_tools[0].path == (
        "tests.resources.examples._shared.tool_functions.calculate"
    )


def test_file_based_relative_path_resolved_to_workdir(tmp_path: Path) -> None:
    """A file-based tool's relative path IS joined onto the workdir."""
    spec = _spec_with_tools(
        [
            LocalToolInfo(
                name="arxiv_search",
                path="tools/python/arxiv_search.py",
                language="python",
            )
        ]
    )

    resolved = _spec_with_workdir_paths(spec, tmp_path)

    assert resolved.local_tools[0].path == str(
        (tmp_path / "tools/python/arxiv_search.py").resolve()
    )


def test_same_spec_workdir_rewrite_preserves_origin_metadata(tmp_path: Path) -> None:
    """Local path normalization must retain the server-pinned origin snapshot."""
    spec = _spec_with_tools(
        [LocalToolInfo(name="example", path="tools/python/example.py", language="python")]
    )
    mcp_selection = {"omnigent.mcp.original": "1"}
    original = ResolvedSpec(
        spec=spec,
        workdir=tmp_path,
        bundle_location="agent/original.tar.gz",
        operator_authored=False,
        effective_spec_digest="effective-spec-digest",
        operator_environment_digest="operator-environment-digest",
        mcp_selection_snapshot=mcp_selection,
    )

    rewritten_spec = _spec_with_workdir_paths(original.spec, original.workdir)
    rewritten = _rewrap_like(original, rewritten_spec, original.workdir)

    assert isinstance(rewritten, ResolvedSpec)
    assert rewritten.spec.local_tools[0].path == str(
        (tmp_path / "tools/python/example.py").resolve()
    )
    assert rewritten.bundle_location == original.bundle_location
    assert rewritten.operator_authored is original.operator_authored
    assert rewritten.effective_spec_digest == original.effective_spec_digest
    assert rewritten.operator_environment_digest == original.operator_environment_digest
    assert rewritten.mcp_selection_snapshot == mcp_selection
    assert rewritten.mcp_selection_snapshot is not mcp_selection


def test_absolute_path_left_untouched(tmp_path: Path) -> None:
    """An already-absolute file path is not re-joined."""
    spec = _spec_with_tools(
        [
            LocalToolInfo(
                name="arxiv_search",
                path="/abs/tools/python/arxiv_search.py",
                language="python",
            )
        ]
    )

    resolved = _spec_with_workdir_paths(spec, tmp_path)

    assert resolved.local_tools[0].path == "/abs/tools/python/arxiv_search.py"


@pytest.mark.parametrize("language", ["omnigent-python-callable", "python", None])
def test_dotted_path_untouched_regardless_of_language(
    tmp_path: Path, language: str | None
) -> None:
    """A dotted import path survives even if its language field is wrong.

    The structural file-vs-dotted discriminator must win over the language
    string so a translator rename of the callable-tool language can't
    silently reintroduce the workdir-mangling bug.
    """
    spec = _spec_with_tools(
        [
            LocalToolInfo(
                name="calculate",
                path="tests.resources.examples._shared.tool_functions.calculate",
                language=language,  # type: ignore[arg-type]
            )
        ]
    )

    resolved = _spec_with_workdir_paths(spec, tmp_path)

    assert resolved.local_tools[0].path == (
        "tests.resources.examples._shared.tool_functions.calculate"
    )
