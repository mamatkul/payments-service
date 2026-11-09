import uuid
from typing import Any

import pytest
from faststream import AckPolicy
from faststream.rabbit import RabbitBroker, RabbitMessage, RabbitQueue, TestRabbitBroker

from payments.messaging.consumer import ERROR_HEADER, PaymentsConsumer
from payments.messaging.topology import (
    ATTEMPT_HEADER,
    DLQ_ROUTING_KEY,
    DLX_EXCHANGE,
    NEW_PAYMENTS_QUEUE,
    NEW_PAYMENTS_ROUTING_KEY,
    PAYMENTS_EXCHANGE,
    RETRY_EXCHANGE,
    retry_routing_key,
)


class FakeProcessor:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.processed: list[uuid.UUID] = []

    async def process(self, payment_id: uuid.UUID) -> None:
        self.processed.append(payment_id)
        if self.error:
            raise self.error


class Harness:
    def __init__(self, processor: FakeProcessor) -> None:
        self.broker = RabbitBroker()
        self.consumer = PaymentsConsumer(
            self.broker, processor, max_attempts=3, retry_base_delay_seconds=1.0
        )
        self.retried: list[dict[str, Any]] = []
        self.dead: list[dict[str, Any]] = []

        @self.broker.subscriber(NEW_PAYMENTS_QUEUE, PAYMENTS_EXCHANGE, ack_policy=AckPolicy.MANUAL)
        async def handle(message: RabbitMessage) -> None:
            await self.consumer.handle(message)

        for attempt in (1, 2):

            @self.broker.subscriber(
                RabbitQueue(
                    f"capture.retry.{attempt}", routing_key=retry_routing_key(attempt, 1.0)
                ),
                RETRY_EXCHANGE,
            )
            async def capture_retry(message: RabbitMessage) -> None:
                self.retried.append(dict(message.headers))

        @self.broker.subscriber(
            RabbitQueue("capture.dlq", routing_key=DLQ_ROUTING_KEY), DLX_EXCHANGE
        )
        async def capture_dead(message: RabbitMessage) -> None:
            self.dead.append(dict(message.headers))

    async def deliver(self, body: Any, attempt: int | None = None) -> None:
        headers: dict[str, Any] = {} if attempt is None else {ATTEMPT_HEADER: attempt}
        async with TestRabbitBroker(self.broker) as broker:
            await broker.publish(
                body,
                exchange=PAYMENTS_EXCHANGE,
                routing_key=NEW_PAYMENTS_ROUTING_KEY,
                headers=headers,
            )


PAYMENT_ID = uuid.uuid4()


async def test_successful_processing_is_acked_without_forwarding() -> None:
    processor = FakeProcessor()
    harness = Harness(processor)

    await harness.deliver({"payment_id": str(PAYMENT_ID)})

    assert processor.processed == [PAYMENT_ID]
    assert harness.retried == harness.dead == []


@pytest.mark.parametrize(("attempt", "expected_next"), [(None, 2), (1, 2), (2, 3)])
async def test_failure_before_last_attempt_goes_to_the_retry_queue(
    attempt: int | None, expected_next: int
) -> None:
    harness = Harness(FakeProcessor(RuntimeError("db down")))

    await harness.deliver({"payment_id": str(PAYMENT_ID)}, attempt)

    assert harness.dead == []
    assert len(harness.retried) == 1
    assert harness.retried[0][ATTEMPT_HEADER] == expected_next
    assert harness.retried[0][ERROR_HEADER] == "RuntimeError: db down"


async def test_failure_on_last_attempt_goes_to_the_dead_letter_queue() -> None:
    harness = Harness(FakeProcessor(RuntimeError("webhook down")))

    await harness.deliver({"payment_id": str(PAYMENT_ID)}, attempt=3)

    assert harness.retried == []
    assert len(harness.dead) == 1
    assert harness.dead[0][ATTEMPT_HEADER] == 3
    assert harness.dead[0][ERROR_HEADER] == "RuntimeError: webhook down"


async def test_malformed_message_is_dead_lettered_immediately() -> None:
    processor = FakeProcessor()
    harness = Harness(processor)

    await harness.deliver({"unexpected": "shape"})

    assert processor.processed == []
    assert harness.retried == []
    assert harness.dead[0][ERROR_HEADER] == "invalid message body"
