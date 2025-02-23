import uvicorn
import logging
from fastapi import FastAPI
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from contextlib import asynccontextmanager
from src.routes import setup_routes
from src.database import DatabaseManager
from src.config.settings import *

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up application...")
    db = DatabaseManager(POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, POSTGRES_HOST, POSTGRES_PORT)
    await db.initialize_database_execution()
    app.state.db = db
    app.state.sql_store = {}
    yield
    logger.info("Shutting down application...")
    db.close_database_execution()
    logger.info("Shutdown and cleaned database")

def create_app() -> FastAPI:
    app = FastAPI(
        title=APP_NAME,
        description="Natural language to SQL generator.",
        version="1.0.0",
        lifespan=lifespan
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=ALLOWED_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.add_middleware(
        TrustedHostMiddleware, 
        allowed_hosts=ALLOWED_HOSTS
    )

    app.mount(
        "/static", 
        StaticFiles(directory="static"), 
        name="static"
    )

    templates = Jinja2Templates(directory="templates")
    setup_routes(app, templates, API_PREFIX)

    return app

app = create_app()

if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        reload=True,
        workers=4
    )