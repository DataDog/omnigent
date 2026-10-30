"""Operator-supplied outer ASGI middlewares (``OMNIGENT_MIDDLEWARE_MODULES``).

Each listed module must expose
``create_middlewares(server_cfg: Mapping[str, object]) -> Sequence[Callable[[ASGIApp], ASGIApp]]``.
The middlewares wrap outside every built-in omnigent middleware (including
``BasePathMiddleware``, so they see the original base-path-prefixed path). List
order is preserved across modules: the first module's first middleware is the
outermost layer.
"""

from __future__ import annotations

import importlib
import os
from collections.abc import Callable, Mapping, Sequence

from starlette.types import ASGIApp

MIDDLEWARE_MODULES_ENV = "OMNIGENT_MIDDLEWARE_MODULES"

ExternalMiddleware = Callable[[ASGIApp], ASGIApp]


def load_external_middlewares(server_cfg: Mapping[str, object]) -> list[ExternalMiddleware]:
    """Import ``OMNIGENT_MIDDLEWARE_MODULES`` and collect their middlewares, outermost first.

    :raises RuntimeError: When a module cannot be imported, lacks a callable
        ``create_middlewares``, raises, or returns something other than a
        sequence of callables.
    """
    raw = os.environ.get(MIDDLEWARE_MODULES_ENV, "")
    module_names = [name.strip() for name in raw.split(",") if name.strip()]
    middlewares: list[ExternalMiddleware] = []
    for module_name in module_names:
        try:
            module = importlib.import_module(module_name)
        except ImportError as exc:
            raise RuntimeError(
                f"Could not import middleware module {module_name!r} "
                f"(set via {MIDDLEWARE_MODULES_ENV}): {exc}"
            ) from exc
        factory = getattr(module, "create_middlewares", None)
        if not callable(factory):
            raise RuntimeError(
                f"Middleware module {module_name!r} does not expose a callable "
                "'create_middlewares'"
            )
        try:
            result = factory(server_cfg)
        except Exception as exc:
            raise RuntimeError(
                f"Middleware module {module_name!r} raised while building middlewares: {exc}"
            ) from exc
        if (
            not isinstance(result, Sequence)
            or isinstance(result, (str, bytes))
            or not all(callable(item) for item in result)
        ):
            raise RuntimeError(
                f"Middleware module {module_name!r} returned {type(result).__name__}, "
                "expected a sequence of callables"
            )
        middlewares.extend(result)
    return middlewares
