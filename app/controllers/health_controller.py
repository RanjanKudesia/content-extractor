from app.pipelines.health_pipeline import HealthPipeline
from app.schemas.health_schema import HealthResponse


class HealthController:
    def __init__(self, pipeline: HealthPipeline) -> None:
        self.pipeline = pipeline

    def execute(self) -> HealthResponse:
        result = self.pipeline.run()
        return HealthResponse(**result)
