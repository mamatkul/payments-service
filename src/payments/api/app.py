from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy import text

from payments.api.routes import router
from payments.config import Settings, get_settings
from payments.db.session import create_engine, create_session_factory
from payments.logging import configure_logging


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    configure_logging(settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = create_engine(settings.database_url)
        app.state.settings = settings
        app.state.engine = engine
        app.state.session_factory = create_session_factory(engine)
        try:
            yield
        finally:
            await engine.dispose()

    app = FastAPI(title="Payments Service", version="0.1.0", lifespan=lifespan)
    app.include_router(router)

    @app.get("/health", tags=["service"], include_in_schema=False)
    async def health() -> dict[str, str]:
        async with app.state.engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
        return {"status": "ok"}

    return app
