import hashlib
import json
import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from payments.db.models import OutboxMessage, Payment
from payments.domain import PAYMENT_CREATED_EVENT, Currency, PaymentStatus
from payments.messaging.topology import NEW_PAYMENTS_ROUTING_KEY


class IdempotencyKeyReusedError(Exception): ...


@dataclass(frozen=True, slots=True)
class NewPayment:
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    webhook_url: str


@dataclass(frozen=True, slots=True)
class CreatePaymentResult:
    payment: Payment
    created: bool


def request_hash(data: NewPayment) -> str:
    canonical = {
        "amount": str(data.amount.normalize()),
        "currency": data.currency.value,
        "description": data.description,
        "metadata": data.metadata,
        "webhook_url": data.webhook_url,
    }
    encoded = json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode()).hexdigest()


async def create_payment(
    session: AsyncSession, *, idempotency_key: str, data: NewPayment
) -> CreatePaymentResult:
    fingerprint = request_hash(data)
    payment_id = uuid.uuid4()

    inserted_id = await session.scalar(
        insert(Payment)
        .values(
            id=payment_id,
            amount=data.amount,
            currency=data.currency.value,
            description=data.description,
            meta=data.metadata,
            status=PaymentStatus.PENDING.value,
            idempotency_key=idempotency_key,
            request_hash=fingerprint,
            webhook_url=data.webhook_url,
        )
        .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
        .returning(Payment.id)
    )

    if inserted_id is None:
        await session.rollback()
        existing = await session.scalar(
            select(Payment).where(Payment.idempotency_key == idempotency_key)
        )
        if existing is None:  # pragma: no cover - the conflicting row cannot vanish
            raise RuntimeError("idempotency conflict without an existing payment")
        if existing.request_hash != fingerprint:
            raise IdempotencyKeyReusedError
        return CreatePaymentResult(payment=existing, created=False)

    session.add(
        OutboxMessage(
            id=uuid.uuid4(),
            aggregate_id=payment_id,
            event_type=PAYMENT_CREATED_EVENT,
            routing_key=NEW_PAYMENTS_ROUTING_KEY,
            payload={"payment_id": str(payment_id)},
        )
    )
    await session.commit()

    payment = await session.get(Payment, payment_id)
    assert payment is not None
    return CreatePaymentResult(payment=payment, created=True)


async def get_payment(session: AsyncSession, payment_id: uuid.UUID) -> Payment | None:
    return await session.get(Payment, payment_id)
