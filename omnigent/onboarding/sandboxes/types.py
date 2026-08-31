"""Public types for the sandbox launcher surface.

These dataclasses and exceptions are the vocabulary used by both the
existing :class:`~omnigent.onboarding.sandboxes.base.SandboxLauncher`
interface and the newer pluggable surface in
:mod:`omnigent.onboarding.sandboxes.registry`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable


class SandboxError(Exception):
    """Base for all sandbox-provider errors."""


class SandboxConfigError(SandboxError):
    """Sandbox provider configuration is malformed or unavailable."""


class SandboxAuthError(SandboxError):
    """Provider credentials or local tooling are missing/invalid."""


class SandboxCommandError(SandboxError):
    """A command executed inside a sandbox failed.

    :param message: Human-readable reason.
    :param command: The remote command that failed.
    :param returncode: Remote exit code.
    :param stdout: Captured standard output.
    :param stderr: Captured standard error.
    """

    def __init__(
        self,
        message: str,
        *,
        command: str | None = None,
        returncode: int | None = None,
        stdout: str | None = None,
        stderr: str | None = None,
    ) -> None:
        super().__init__(message)
        self.command = command
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


@dataclass(frozen=True)
class SandboxCapabilities:
    """Feature flags declared by a sandbox provider.

    Providers advertise which primitives they support so callers can fail
    fast and surface actionable messages.

    :param cli_bootstrap: Provider supports ``omnigent sandbox create`` /
        ``connect`` (``put`` / ``stream_exec`` / ``exec_foreground`` /
        ``wheel_install_command``).
    :param managed_launch: Provider supports server-managed
        ``host_type="managed"`` sessions (``prepare`` / ``provision`` /
        ``start_host``).
    :param local_port_forward: Provider can bridge a local port into the
        sandbox for the App OAuth callback flow.
    :param resume_stopped: Provider can resume a stopped sandbox in place
        with its persistent volume.
    :param programmatic_terminate: Provider can terminate a sandbox
        programmatically.
    :param file_copy: Provider supports copying files into the sandbox.
    :param streaming_exec: Provider supports streaming process execution
        inside the sandbox.
    :param foreground_exec: Provider supports a foreground exec that
        inherits local stdio.
    :param authenticated_provisioning: Provider accepts an
        :class:`AuthenticatedProvisioningContext` for on-behalf-of
        identity propagation (e.g. Habitat via Ticino OBO).
    """

    cli_bootstrap: bool = False
    managed_launch: bool = False
    local_port_forward: bool = False
    resume_stopped: bool = False
    programmatic_terminate: bool = False
    file_copy: bool = False
    streaming_exec: bool = False
    foreground_exec: bool = False
    authenticated_provisioning: bool = False


@dataclass(frozen=True)
class SandboxSpec:
    """Provider-agnostic description of a sandbox to provision."""

    name: str
    image: str | None = None
    cpu: float | None = None
    memory_mib: int | None = None
    disk_gb: int | None = None
    lifetime_s: int | None = None
    tags: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class SandboxInfo:
    """Result of a successful provision or attach."""

    sandbox_id: str
    workspace_path: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class HostContext:
    """Context handed to ``start_host`` when launching a managed host."""

    token: str
    host_id: str
    host_name: str
    server_url: str
    repo_url: str | None = None
    repo_branch: str | None = None
    repo_name: str | None = None
    host_config: dict[str, object] = field(default_factory=dict)
    on_stage: Callable[[str], None] | None = None


@dataclass(frozen=True)
class AuthenticatedProvisioningContext:
    """Server-side verified identity for on-behalf-of sandbox provisioning.

    Carries the authenticated user's verified identity and a server-side
    callable that returns the current signed ID token. The callable may
    refresh the token from the IdP but never exposes the refresh token.
    Intentionally not JSON-serializable — it lives only in the server
    process and is never sent to the browser, sandbox, or API response.

    :param user_id: Verified user email (lowercased).
    :param oidc_session_id: Internal OIDC session ID for credential lookup.
    :param get_id_token: Callable returning the current signed ID token
        string. May raise ``ReauthenticationError`` when the user must
        sign in again.
    """

    user_id: str
    oidc_session_id: str
    get_id_token: Callable[[], str]
