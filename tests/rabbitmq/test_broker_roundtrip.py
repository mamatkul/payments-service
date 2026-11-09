import asyncio
import os
import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest
from faststream import AckPolicy
from faststream.rabbit import RabbitBroker, RabbitMessage

from payments.messaging.consumer import ERROR_HEADER, PaymentsConsumer
from payments.messaging.outbox_relay import RabbitEventPublisher
from payments.messaging.topology import (
    ATTEMPT_HEADER,
    DEAD_LETTER_QUEUE,
    NEW_PAYMENTS_QUEUE,
    NEW_PAYMENTS_ROUTING_KEY,
    PAYMENTS_EXCHANGE,
    declare_topology,
)

RABBITMQ_URL = os.environ.get("TEST_RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")


class FailingProcessor:
    def __init__(self) -> None:
        self.calls = 0

    async def process(self, payment_id: uuid.UUID) -> None:
        self.calls += 1
        raise RuntimeError("webhook down")


async def purge(broker: RabbitBroker) -> None:
    for queue in (NEW_PAYMENTS_QUEUE, DEAD_LETTER_QUEUE):
        declared = await broker.declare_queue(queue)
        await declared.purge()


@pytest.fixture
async def broker() -> AsyncIterator[RabbitBroker]:
    broker = RabbitBroker(RABBITMQ_URL)
    try:
        await asyncio.wait_for(broker.connect(), timeout=3)
    except Exception:
        pytest.skip("RabbitMQ is not available")
    await declare_topology(broker, max_attempts=3, base_delay=0.1)
    await purge(broker)
    yield broker
    await broker.stop()


async def test_failing_message_is_retried_with_backoff_then_dead_lettered(
    broker: RabbitBroker,
) -> None:
    processor = FailingProcessor()
    consumer = PaymentsConsumer(broker, processor, max_attempts=3, retry_base_delay_seconds=0.1)

    @broker.subscriber(NEW_PAYMENTS_QUEUE, PAYMENTS_EXCHANGE, ack_policy=AckPolicy.MANUAL)
    async def handle(message: RabbitMessage) -> None:
        await consumer.handle(message)

    await broker.start()
    await RabbitEventPublisher(broker).publish(
        message_id=str(uuid.uuid4()),
        routing_key=NEW_PAYMENTS_ROUTING_KEY,
        payload={"payment_id": str(uuid.uuid4())},
    )

    dead: dict[str, Any] | None = None
    dlq = await broker.declare_queue(DEAD_LETTER_QUEUE)
    for _ in range(50):
        message = await dlq.get(no_ack=True, fail=False)
        if message is not None:
            dead = dict(message.headers)
            break
        await asyncio.sleep(0.1)

    assert processor.calls == 3
    assert dead is not None
    assert dead[ATTEMPT_HEADER] == 3
    assert dead[ERROR_HEADER] == "RuntimeError: webhook down"
