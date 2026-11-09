import asyncio
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payments.db.models import OutboxMessage
from payments.messaging.outbox_relay import RelayResult, relay_batch, relay_loop
from payments.services.payments import create_payment
from tests.factories import new_payment


class RecordingPublisher:
    def __init__(self, fail_on: set[str] | None = None, delay: float = 0) -> None:
        self.published: list[dict[str, Any]] = []
        self._fail_on = fail_on or set()
        self._delay = delay

    async def publish(self, *, message_id: str, routing_key: str, payload: dict[str, Any]) -> None:
        await asyncio.sleep(self._delay)
        if payload["payment_id"] in self._fail_on:
            raise ConnectionError("broker unavailable")
        self.published.append({"id": message_id, "routing_key": routing_key, **payload})


async def seed(session_factory: async_sessionmaker[AsyncSession], n: int) -> list[str]:
    ids = []
    for i in range(n):
        async with session_factory() as session:
            result = await create_payment(session, idempotency_key=f"k{i}", data=new_payment())
            ids.append(str(result.payment.id))
    return ids


async def outbox(session_factory: async_sessionmaker[AsyncSession]) -> list[OutboxMessage]:
    async with session_factory() as session:
        return list(
            (await session.scalars(select(OutboxMessage).order_by(OutboxMessage.created_at))).all()
        )


async def test_publishes_pending_events_in_order_and_marks_them(
    session_factory: async_sessionmaker[AsyncSession], clean_db: None
) -> None:
    ids = await seed(session_factory, 3)
    publisher = RecordingPublisher()

    assert await relay_batch(session_factory, publisher, batch_size=10) == RelayResult(
        3, failed=False
    )
    assert [m["payment_id"] for m in publisher.published] == ids
    assert all(m["routing_key"] == "payments.new" for m in publisher.published)
    assert all(row.published_at is not None for row in await outbox(session_factory))

    assert await relay_batch(session_factory, publisher, batch_size=10) == RelayResult(
        0, failed=False
    )
    assert len(publisher.published) == 3


async def test_failure_stops_the_batch_and_is_retried_next_round(
    session_factory: async_sessionmaker[AsyncSession], clean_db: None
) -> None:
    ids = await seed(session_factory, 3)

    assert await relay_batch(
        session_factory, RecordingPublisher(fail_on={ids[1]}), batch_size=10
    ) == RelayResult(1, failed=True)
    rows = await outbox(session_factory)
    assert [row.published_at is not None for row in rows] == [True, False, False]
    assert rows[1].attempts == 1
    assert rows[1].last_error == "ConnectionError: broker unavailable"

    retry = RecordingPublisher()
    assert await relay_batch(session_factory, retry, batch_size=10) == RelayResult(2, failed=False)
    assert [m["payment_id"] for m in retry.published] == ids[1:]
    assert all(row.published_at is not None for row in await outbox(session_factory))


async def test_concurrent_relays_never_publish_the_same_event_twice(
    session_factory: async_sessionmaker[AsyncSession], clean_db: None
) -> None:
    await seed(session_factory, 20)
    publishers = [RecordingPublisher(delay=0.01) for _ in range(4)]

    await asyncio.gather(*(relay_batch(session_factory, p, batch_size=5) for p in publishers))

    published = [m["id"] for p in publishers for m in p.published]
    assert len(published) == len(set(published)) == 20


class FlakyPublisher(RecordingPublisher):
    def __init__(self, failures: int) -> None:
        super().__init__()
        self.failures = failures

    async def publish(self, *, message_id: str, routing_key: str, payload: dict[str, Any]) -> None:
        if self.failures:
            self.failures -= 1
            raise ConnectionError("broker unavailable")
        await super().publish(message_id=message_id, routing_key=routing_key, payload=payload)


async def test_loop_backs_off_on_failures_and_resets_after_success(
    session_factory: async_sessionmaker[AsyncSession], clean_db: None
) -> None:
    await seed(session_factory, 1)
    publisher = FlakyPublisher(failures=7)
    stop = asyncio.Event()
    waits: list[float] = []

    async def wait(delay: float) -> None:
        waits.append(delay)
        if len(waits) == 8:
            await seed_one_more()
            publisher.failures = 1
        if len(waits) == 10:
            stop.set()

    async def seed_one_more() -> None:
        async with session_factory() as session:
            await create_payment(session, idempotency_key="later", data=new_payment())

    await relay_loop(
        session_factory,
        publisher,
        stop,
        batch_size=10,
        poll_interval=0.5,
        max_backoff=30,
        wait=wait,
    )

    assert waits == [0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 0.5, 0.5, 0.5]
    assert len(publisher.published) == 2
