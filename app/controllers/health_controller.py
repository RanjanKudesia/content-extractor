import logging

from app.pipelines.health_pipeline import HealthPipeline
from app.schemas.health_schema import HealthResponse


logger = logging.getLogger(__name__)


class HealthController:
    def __init__(self, pipeline: HealthPipeline) -> None:
        self.pipeline = pipeline

    def execute(self) -> HealthResponse:
        logger.debug("Executing health controller")
        result = self.pipeline.run()
        logger.debug("Health controller completed", extra={
                     "status": result.get("status")})
        return HealthResponse(**result)
