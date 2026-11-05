"""Quarantine: mark a run as untrusted so nothing downstream (chat, learning loop) consumes it."""
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class QuarantineError(RuntimeError):
    """Raised when the governance gate fails. Carries the failed checks for the run record."""

    def __init__(self, reason: str, failed: list[str]):
        super().__init__(reason)
        self.reason = reason
        self.failed = failed


def quarantine_run(db, run_id: str, reason: str) -> None:
    db.update_run(run_id, status="quarantined", completed_at=datetime.now(timezone.utc).replace(tzinfo=None),
                  error_message=reason[:1000])
    logger.warning("Run %s QUARANTINED: %s", run_id, reason)
