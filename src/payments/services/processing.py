import logging
import uuid
from typing import Protocol

from sqlalchemy import func, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payments.db.models import Payment
from payments.domain import PaymentStatus
from payments.services.webhooks import WebhookDeliveryError

logger = logging.getLogger(__name__)


class Gateway(Protocol):
    async def charge(self) -> PaymentStatus: ...


class Webhooks(Protocol):
    async def send(self, payment: Payment) -> None: ...


class PaymentProcessor:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: Gateway,
        webhooks: Webhooks,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway
        self._webhooks = webhooks

    async def process(self, payment_id: uuid.UUID) -> None:
        async with self._session_factory() as session:
            payment = await session.get(Payment, payment_id)
            if payment is None:
                logger.error("payment_not_found payment_id=%s", payment_id)
                return

            if payment.status == PaymentStatus.PENDING:
                await self._charge(session, payment)

            if payment.webhook_delivered_at is None:
                await self._notify(session, payment)

    async def _charge(self, session: AsyncSession, payment: Payment) -> None:
        outcome = await self._gateway.charge()
        result = await session.execute(
            update(Payment)
            .where(Payment.id == payment.id, Payment.status == PaymentStatus.PENDING)
            .values(status=outcome, processed_at=func.now())
        )
        await session.commit()
        if result.rowcount == 0:  # type: ignore[attr-defined]
            logger.info("payment_already_processed payment_id=%s", payment.id)
        await session.refresh(payment)
        logger.info("payment_processed payment_id=%s status=%s", payment.id, payment.status)

    async def _notify(self, session: AsyncSession, payment: Payment) -> None:
        try:
            await self._webhooks.send(payment)
        except WebhookDeliveryError as error:
            await session.execute(
                update(Payment)
                .where(Payment.id == payment.id)
                .values(
                    webhook_attempts=Payment.webhook_attempts + 1,
                    last_webhook_error=str(error)[:500],
                )
            )
            await session.commit()
            logger.warning("webhook_failed payment_id=%s error=%s", payment.id, error)
            raise
        await session.execute(
            update(Payment)
            .where(Payment.id == payment.id)
            .values(
                webhook_attempts=Payment.webhook_attempts + 1,
                webhook_delivered_at=func.now(),
                last_webhook_error=None,
            )
        )
        await session.commit()
        logger.info("webhook_delivered payment_id=%s", payment.id)
