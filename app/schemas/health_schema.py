"""Health-check response schema."""
from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    """Response returned by the health-check endpoint."""
    status: str = Field(description="Overall health status.", examples=["ok"])
    service: str = Field(description="Service name.",
                         examples=["Content Extractor"])
    environment: str = Field(
        description="Runtime environment.", examples=["development"])
    timestamp: str = Field(
        description="ISO-8601 UTC timestamp when health was computed.")
    dependencies: dict[str, str] | None = Field(
        default=None,
        description="Dependency statuses keyed by dependency name (e.g., s3, mongodb).",
    )
