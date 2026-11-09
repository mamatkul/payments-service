import logging
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Literal, Protocol

import httpx
from faststream import AckPolicy, ContextRepo, FastStream
from faststream.rabbit import RabbitBroker, RabbitMessage
from pydantic import ValidationError

from payments.config import Settings, get_settings
from payments.db.session import create_engine, create_session_factory
from payments.logging import configure_logging
from payments.messaging.events import PaymentCreatedEvent
from payments.messaging.topology import (
    ATTEMPT_HEADER,
    DLQ_ROUTING_KEY,
    DLX_EXCHANGE,
    NEW_PAYMENTS_QUEUE,
    PAYMENTS_EXCHANGE,
    RETRY_EXCHANGE,
    declare_topology,
    retry_routing_key,
)
from payments.services.gateway import EmulatedGateway
from payments.services.processing import PaymentProcessor
from payments.services.webhooks import WebhookSender

logger = logging.getLogger(__name__)

ERROR_HEADER = "x-last-error"


@dataclass(frozen=True, slots=True)
class FailureDecision:
    action: Literal["retry", "dead_letter"]
    next_attempt: int | None = None


def decide_on_failure(attempt: int, max_attempts: int) -> FailureDecision:
    if attempt < max_attempts:
        return FailureDecision("retry", next_attempt=attempt + 1)
    return FailureDecision("dead_letter")


def attempt_of(message: RabbitMessage) -> int:
    raw = message.headers.get(ATTEMPT_HEADER, 1)
    try:
        return max(1, int(str(raw)))
    except (TypeError, ValueError):
        return 1


class Processor(Protocol):
    async def process(self, payment_id: uuid.UUID) -> None: ...


class PaymentsConsumer:
    def __init__(
        self,
        broker: RabbitBroker,
        processor: Processor,
        *,
        max_attempts: int,
        retry_base_delay_seconds: float,
    ) -> None:
        self._broker = broker
        self._processor = processor
        self._max_attempts = max_attempts
        self._retry_base_delay = retry_base_delay_seconds

    async def handle(self, message: RabbitMessage) -> None:
        attempt = attempt_of(message)
        try:
            event = PaymentCreatedEvent.model_validate_json(message.body)
        except ValidationError:
            logger.error("invalid_message message_id=%s", message.message_id)
            await self._dead_letter(message, attempt, "invalid message body")
            return

        try:
            await self._processor.process(event.payment_id)
        except Exception as error:
            reason = f"{type(error).__name__}: {error}"[:500]
            decision = decide_on_failure(attempt, self._max_attempts)
            if decision.action == "retry" and decision.next_attempt is not None:
                logger.warning(
                    "processing_failed_retrying payment_id=%s attempt=%s error=%s",
                    event.payment_id,
                    attempt,
                    reason,
                )
                await self._retry(message, attempt, decision.next_attempt, reason)
            else:
                logger.error(
                    "processing_failed_dead_lettered payment_id=%s attempt=%s error=%s",
                    event.payment_id,
                    attempt,
                    reason,
                )
                await self._dead_letter(message, attempt, reason)
            return

        await message.ack()

    async def _retry(
        self, message: RabbitMessage, attempt: int, next_attempt: int, reason: str
    ) -> None:
        await self._forward(
            message,
            exchange_name="retry",
            routing_key=retry_routing_key(attempt, self._retry_base_delay),
            headers={**message.headers, ATTEMPT_HEADER: next_attempt, ERROR_HEADER: reason},
        )

    async def _dead_letter(self, message: RabbitMessage, attempt: int, reason: str) -> None:
        await self._forward(
            message,
            exchange_name="dlx",
            routing_key=DLQ_ROUTING_KEY,
            headers={**message.headers, ATTEMPT_HEADER: attempt, ERROR_HEADER: reason},
        )

    async def _forward(
        self,
        message: RabbitMessage,
        *,
        exchange_name: Literal["retry", "dlx"],
        routing_key: str,
        headers: dict[str, Any],
    ) -> None:
        exchange = RETRY_EXCHANGE if exchange_name == "retry" else DLX_EXCHANGE
        try:
            await self._broker.publish(
                message.body,
                exchange=exchange,
                routing_key=routing_key,
                headers=headers,
                message_id=message.message_id,
                content_type=message.content_type,
                persist=True,
                mandatory=True,
            )
        except Exception:
            logger.exception("forward_failed message_id=%s", message.message_id)
            if exchange_name == "dlx":
                await message.reject(requeue=False)
            else:
                await message.nack(requeue=True)
            return
        await message.ack()


def build_app(settings: Settings | None = None) -> FastStream:
    settings = settings or get_settings()
    configure_logging(settings.log_level)
    broker = RabbitBroker(settings.rabbitmq_url.get_secret_value())

    @asynccontextmanager
    async def lifespan(context: ContextRepo) -> AsyncIterator[None]:
        engine = create_engine(settings.database_url)
        async with httpx.AsyncClient(timeout=settings.webhook_timeout_seconds) as http:
            processor = PaymentProcessor(
                create_session_factory(engine),
                EmulatedGateway(
                    min_delay_seconds=settings.gateway_min_delay_seconds,
                    max_delay_seconds=settings.gateway_max_delay_seconds,
                    success_rate=settings.gateway_success_rate,
                ),
                WebhookSender(http),
            )
            context.set_global(
                "payments_consumer",
                PaymentsConsumer(
                    broker,
                    processor,
                    max_attempts=settings.max_attempts,
                    retry_base_delay_seconds=settings.retry_base_delay_seconds,
                ),
            )
            try:
                yield
            finally:
                await engine.dispose()

    @broker.subscriber(NEW_PAYMENTS_QUEUE, PAYMENTS_EXCHANGE, ack_policy=AckPolicy.MANUAL)
    async def handle_new_payment(message: RabbitMessage, context: ContextRepo) -> None:
        consumer: PaymentsConsumer = context.get("payments_consumer")
        await consumer.handle(message)

    app = FastStream(broker, lifespan=lifespan)

    @app.on_startup
    async def declare() -> None:
        await broker.connect()
        await declare_topology(
            broker, max_attempts=settings.max_attempts, base_delay=settings.retry_base_delay_seconds
        )

    return app


def __getattr__(name: str) -> FastStream:
    if name == "app":
        return build_app()
    raise AttributeError(name)
