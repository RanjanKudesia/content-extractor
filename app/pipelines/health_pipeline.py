"""Health pipeline for the Content Extractor service."""
import logging

from app.adapters.system_health_adapter import SystemHealthAdapter


logger = logging.getLogger(__name__)


class HealthPipeline:
    """Pipeline that fetches system health metrics via the SystemHealthAdapter."""

    def __init__(self, adapter: SystemHealthAdapter) -> None:
        self.adapter = adapter

    def run(self) -> dict[str, str]:
        """Execute the health check and return a status dict."""
        logger.debug("Running health pipeline")
        result = self.adapter.fetch()
        logger.debug("Health pipeline completed", extra={
                     "status": result.get("status")})
        return result
