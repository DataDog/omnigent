"""Compatibility seam for non-standard ACP vendor behavior.

The standard ACP v1 subagents preview is implemented by the shared executor,
not an extension. :class:`AcpExtension` remains for vendor-specific frames
that have a separate contract.

An extension is composed at the harness boundary, not discovered. Legacy
lifecycle sources remain separate from the standard session registry and must
not reinterpret standard ``subagent_update`` frames.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from omnigent.inner.acp_subagents import AcpSubAgentSource


@dataclass(frozen=True)
class AcpExtension:
    """Vendor behavior layered onto the generic ACP executor.

    :param name: The vendor this extension speaks for, e.g. ``"devin"``. Used in
        logs and to identify the extension in tests; never matched against a
        harness id to select behavior (the harness wrap does the selecting).
    :param subagent_sources: Dialects that recognize this vendor's sub-agent
        lifecycle reporting (see :mod:`omnigent.inner.acp_subagents`). Empty —
        the default — means the executor does no sub-agent scanning at all.
    """

    name: str
    subagent_sources: tuple[AcpSubAgentSource, ...] = field(default=())

    @property
    def surfaces_subagents(self) -> bool:
        """Whether this vendor's sub-agents become Omnigent child sessions.

        The source of truth for the harness's declared ``subagents`` capability,
        so the capability matrix cannot drift from what the code actually does.
        """
        return bool(self.subagent_sources)


# The generic ACP harness and every builtin ACP CLI row that declares no vendor
# behavior. Reads as "protocol only".
NO_ACP_EXTENSION = AcpExtension(name="acp")
