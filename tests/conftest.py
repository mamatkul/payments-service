import os
import threading
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import httpx
import pytest
from alembic import command
from alembic.config import Config
from pydantic import SecretStr
from sqlalchemy import make_url, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from payments.api.app import create_app
from payments.config import Settings
from payments.db.session import create_engine, create_session_factory

ROOT = Path(__file__).resolve().parents[1]
API_KEY = "test-api-key-0123456789"
DEFAULT_TEST_DATABASE_URL = "postgresql+asyncpg://payments:payments@localhost:5432/payments_test"


def _test_database_url() -> str:
    return os.environ.get("TEST_DATABASE_URL", DEFAULT_TEST_DATABASE_URL)


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings(
        database_url=_test_database_url(),
        api_key=SecretStr(API_KEY),
        gateway_min_delay_seconds=0,
        gateway_max_delay_seconds=0,
    )


def _in_thread(target: object) -> None:
    errors: list[BaseException] = []

    def run() -> None:
        try:
            target()  # type: ignore[operator]
        except BaseException as error:  # pragma: no cover - re-raised below
            errors.append(error)

    thread = threading.Thread(target=run)
    thread.start()
    thread.join()
    if errors:
        raise errors[0]


@pytest.fixture(scope="session")
def migrated_database(settings: Settings) -> Iterator[str]:
    url = settings.database_url
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "migrations"))
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = url
    try:
        _in_thread(lambda: _recreate_database(url))
        _in_thread(lambda: command.upgrade(config, "head"))
        yield url
    finally:
        if previous is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = previous


def _recreate_database(url: str) -> None:
    import asyncio

    from sqlalchemy.ext.asyncio import create_async_engine

    target = make_url(url)
    admin = create_async_engine(
        target.set(database="postgres"), isolation_level="AUTOCOMMIT", poolclass=None
    )

    async def recreate() -> None:
        async with admin.connect() as connection:
            await connection.execute(
                text(f'DROP DATABASE IF EXISTS "{target.database}" WITH (FORCE)')
            )
            await connection.execute(text(f'CREATE DATABASE "{target.database}"'))
        await admin.dispose()

    asyncio.run(recreate())


@pytest.fixture(scope="session")
async def engine(migrated_database: str) -> AsyncIterator[AsyncEngine]:
    engine = create_engine(migrated_database)
    yield engine
    await engine.dispose()


@pytest.fixture(scope="session")
def session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return create_session_factory(engine)


@pytest.fixture
async def clean_db(engine: AsyncEngine) -> None:
    async with engine.begin() as connection:
        await connection.execute(text("TRUNCATE payments, outbox"))


@pytest.fixture
async def client(settings: Settings, clean_db: None) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(settings)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
            headers={"X-API-Key": API_KEY},
        ) as http,
    ):
        yield http


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        if "integration" in item.path.parts:
            item.add_marker(pytest.mark.integration)
        if "rabbitmq" in item.path.parts:
            item.add_marker(pytest.mark.rabbitmq)
