from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str
    service: str
    environment: str
    timestamp: str
    dependencies: dict[str, str] | None = None
