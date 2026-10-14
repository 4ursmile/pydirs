"""Constants and helpers shared by the tinycache test modules."""

from __future__ import annotations

import time

SHORT_PX = 50  # milliseconds
EXPIRE_WAIT = 0.15  # seconds, comfortably longer than SHORT_PX


def wait_for_expiry() -> None:
    time.sleep(EXPIRE_WAIT)
