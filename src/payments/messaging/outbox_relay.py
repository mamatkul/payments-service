import asyncio
import contextlib
import logging
import signal
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from faststream.rabbit import RabbitBroker
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payments.config import Settings, get_settings
from payments.db.models import OutboxMessage
from payments.db.session import create_engine, create_session_factory
from payments.logging import configure_logging
from payments.messaging.topology import ATTEMPT_HEADER, PAYMENTS_EXCHANGE, declare_topology

logger = logging.getLogger(__name__)


class EventPublisher(Protocol):
    async def publish(
        self, *, message_id: str, routing_key: str, payload: dict[str, Any]
    ) -> None: ...


class RabbitEventPublisher:
    def __init__(self, broker: RabbitBroker) -> None:
        self._broker = broker

    async def publish(self, *, message_id: str, routing_key: str, payload: dict[str, Any]) -> None:
        await self._broker.publish(
            payload,
            exchange=PAYMENTS_EXCHANGE,
            routing_key=routing_key,
            message_id=message_id,
            headers={ATTEMPT_HEADER: 1},
            persist=True,
            mandatory=True,
        )


@dataclass(frozen=True)
class RelayResult:
    published: int
    failed: bool


class Backoff:
    def __init__(self, initial: float, maximum: float) -> None:
        self._initial = initial
        self._maximum = maximum
        self._current = initial

    def next(self) -> float:
        delay = self._current
        self._current = min(self._current * 2, self._maximum)
        return delay

    def reset(self) -> None:
        self._current = self._initial


async def relay_batch(
    session_factory: async_sessionmaker[AsyncSession],
    publisher: EventPublisher,
    *,
    batch_size: int,
) -> RelayResult:
    published = 0
    failed = False
    async with session_factory() as session, session.begin():
        rows = (
            await session.scalars(
                select(OutboxMessage)
                .where(OutboxMessage.published_at.is_(None))
                .order_by(OutboxMessage.created_at)
                .limit(batch_size)
                .with_for_update(skip_locked=True)
            )
        ).all()
        for row in rows:
            row.attempts += 1
            try:
                await publisher.publish(
                    message_id=str(row.id), routing_key=row.routing_key, payload=row.payload
                )
            except Exception as error:
                row.last_error = f"{type(error).__name__}: {error}"[:500]
                logger.warning("outbox_publish_failed outbox_id=%s error=%s", row.id, error)
                failed = True
                break
            row.published_at = datetime.now(UTC)
            row.last_error = None
            published += 1
    if published:
        logger.info("outbox_published count=%s", published)
    return RelayResult(published=published, failed=failed)


async def relay_loop(
    session_factory: async_sessionmaker[AsyncSession],
    publisher: EventPublisher,
    stop: asyncio.Event,
    *,
    batch_size: int,
    poll_interval: float,
    max_backoff: float,
    wait: Callable[[float], Awaitable[None]],
) -> None:
    backoff = Backoff(poll_interval, max(poll_interval, max_backoff))
    while not stop.is_set():
        try:
            result = await relay_batch(session_factory, publisher, batch_size=batch_size)
        except Exception:
            logger.exception("outbox_relay_iteration_failed")
            result = RelayResult(published=0, failed=True)
        if result.published:
            backoff.reset()
        if result.failed:
            delay = backoff.next()
            logger.info("outbox_relay_backoff delay=%s", delay)
            await wait(delay)
        elif result.published < batch_size:
            await wait(poll_interval)


async def run_relay(settings: Settings, stop: asyncio.Event) -> None:
    engine = create_engine(settings.database_url)
    session_factory = create_session_factory(engine)
    broker = RabbitBroker(settings.rabbitmq_url.get_secret_value())
    await broker.connect()
    try:
        await declare_topology(
            broker, max_attempts=settings.max_attempts, base_delay=settings.retry_base_delay_seconds
        )
        logger.info("outbox_relay_started")

        async def wait(delay: float) -> None:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), delay)

        await relay_loop(
            session_factory,
            RabbitEventPublisher(broker),
            stop,
            batch_size=settings.outbox_batch_size,
            poll_interval=settings.outbox_poll_interval_seconds,
            max_backoff=settings.outbox_max_backoff_seconds,
            wait=wait,
        )
    finally:
        await broker.stop()
        await engine.dispose()
        logger.info("outbox_relay_stopped")


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)

    async def runner() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
        await run_relay(settings, stop)

    asyncio.run(runner())


if __name__ == "__main__":
    main()
