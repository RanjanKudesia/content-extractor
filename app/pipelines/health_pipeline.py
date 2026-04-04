from app.adapters.system_health_adapter import SystemHealthAdapter


class HealthPipeline:
    def __init__(self, adapter: SystemHealthAdapter) -> None:
        self.adapter = adapter

    def run(self) -> dict[str, str]:
        return self.adapter.fetch()
