"""THROWAWAY VIT-924 rollback-validation fault. Never merge.

Dev only (OTEL_RESOURCE_ATTRIBUTES contains deployment.environment.name=development).
/health returns 503 starting DELAY_S seconds after this image first started in the pod.
The first start time is stored in a marker file on the data volume, so liveness
restarts don't reset the clock. Markers older than 2 hours are ignored.
No DB access, no migrations.
"""

import logging
import os
import time
from pathlib import Path

DELAY_S = 90
MAX_AGE_S = 2 * 3600
_log = logging.getLogger("omnigent.vit924_fault")
_onset_logged = False


def _enabled() -> bool:
    return "deployment.environment.name=development" in os.environ.get("OTEL_RESOURCE_ATTRIBUTES", "")


def _first_start() -> float:
    marker = Path(os.environ.get("OMNIGENT_DATA_DIR", "/data")) / ".vit924-fault-first-start"
    now = time.time()
    try:
        started = float(marker.read_text().strip())
        if 0 <= now - started <= MAX_AGE_S:
            return started
    except (OSError, ValueError):
        pass
    try:
        marker.write_text(f"{now}\n")
    except OSError:
        pass
    return now


_FIRST_START = _first_start() if _enabled() else None


def health_should_fail() -> bool:
    global _onset_logged
    if _FIRST_START is None or time.time() - _FIRST_START < DELAY_S:
        return False
    if not _onset_logged:
        _onset_logged = True
        _log.error("VIT924_FAULT_ONSET ts=%s first_start=%s", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), _FIRST_START)
    return True
