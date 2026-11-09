import asyncio
import uuid
from datetime import UTC, datetime

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payments.db.models import Payment
from payments.domain import PaymentStatus
from payments.services.payments import create_payment
from payments.services.processing import PaymentProcessor
from payments.services.webhooks import WebhookDeliveryError
from tests.factories import new_payment


class FakeGateway:
    def __init__(self, outcome: PaymentStatus = PaymentStatus.SUCCEEDED) -> None:
        self.outcome = outcome
        self.calls = 0

    async def charge(self) -> PaymentStatus:
        self.calls += 1
        return self.outcome


class FakeWebhooks:
    def __init__(self, failures: int = 0) -> None:
        self.failures = failures
        self.delivered: list[tuple[uuid.UUID, str]] = []

    async def send(self, payment: Payment) -> None:
        if self.failures:
            self.failures -= 1
            raise WebhookDeliveryError("HTTP 503")
        self.delivered.append((payment.id, payment.status))


async def make(session_factory: async_sessionmaker[AsyncSession]) -> uuid.UUID:
    async with session_factory() as session:
        result = await create_payment(session, idempotency_key="k", data=new_payment())
    return result.payment.id


async def load(session_factory: async_sessionmaker[AsyncSession], payment_id: uuid.UUID) -> Payment:
    async with session_factory() as session:
        payment = await session.get(Payment, payment_id)
    assert payment is not None
    return payment


@pytest.mark.parametrize("outcome", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_charges_stores_status_and_sends_webhook(
    session_factory: async_sessionmaker[AsyncSession], clean_db: None, outcome: PaymentStatus
) -> None:
    payment_id = await make(session_factory)
    webhooks = FakeWebhooks()

    await PaymentProcessor(session_factory, FakeGateway(outcome), webhooks).process(payment_id)

    payment = await load(session_factory, payment_id)
    assert payment.status == outcome
    assert payment.processed_at is not None
    assert payment.webhook_delivered_at is not None
    assert payment.webhook_attempts == 1
    assert webhooks.delivered == [(payment_id, outcome.value)]


class SlowGateway(FakeGateway):
    async def charge(self) -> PaymentStatus:
        self.started_at = datetime.now(UTC)
        await asyncio.sleep(0.3)
        return await super().charge()


async def test_processed_at_is_set_after_gateway_responds(
    session_factory: async_sessionmaker[AsyncSession], clean_db: None
) -> None:
    payment_id = await make(session_factory)
    gateway = SlowGateway()

    await PaymentProcessor(session_factory, gateway, FakeWebhooks()).process(payment_id)

    payment = await load(session_factory, payment_id)
    assert payment.processed_at is not None
    assert (payment.processed_at - gateway.started_at).total_seconds() >= 0.25


async def test_webhook_failure_keeps_status_and_retry_does_not_charge_again(
    session_factory: async_sessionmaker[AsyncSession], clean_db: None
) -> None:
    payment_id = await make(session_factory)
    gateway, webhooks = FakeGateway(), FakeWebhooks(failures=1)
    processor = PaymentProcessor(session_factory, gateway, webhooks)

    with pytest.raises(WebhookDeliveryError):
        await processor.process(payment_id)
    payment = await load(session_factory, payment_id)
    assert payment.status == PaymentStatus.SUCCEEDED
    assert payment.webhook_delivered_at is None
    assert payment.last_webhook_error == "HTTP 503"

    await processor.process(payment_id)

    payment = await load(session_factory, payment_id)
    assert gateway.calls == 1
    assert payment.webhook_delivered_at is not None
    assert payment.webhook_attempts == 2
    assert payment.last_webhook_error is None


async def test_redelivery_of_a_finished_payment_does_nothing(
    session_factory: async_sessionmaker[AsyncSession], clean_db: None
) -> None:
    payment_id = await make(session_factory)
    gateway, webhooks = FakeGateway(), FakeWebhooks()
    processor = PaymentProcessor(session_factory, gateway, webhooks)

    await processor.process(payment_id)
    await processor.process(payment_id)

    assert gateway.calls == 1
    assert len(webhooks.delivered) == 1


async def test_unknown_payment_is_ignored(
    session_factory: async_sessionmaker[AsyncSession], clean_db: None
) -> None:
    gateway = FakeGateway()
    await PaymentProcessor(session_factory, gateway, FakeWebhooks()).process(uuid.uuid4())
    assert gateway.calls == 0
