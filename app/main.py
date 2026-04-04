import logging

from fastapi import FastAPI

from app.api.routes import router as api_router
from app.config.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

app = FastAPI(title="Content Extractor API")
app.include_router(api_router)
logger.info("Content Extractor API initialized")
