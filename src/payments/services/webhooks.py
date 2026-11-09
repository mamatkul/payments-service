from typing import Any

import httpx

from payments.db.models import Payment


class WebhookDeliveryError(Exception): ...


def webhook_payload(payment: Payment) -> dict[str, Any]:
    return {
        "event": f"payment.{payment.status}",
        "payment_id": str(payment.id),
        "status": payment.status,
        "amount": str(payment.amount),
        "currency": payment.currency,
        "description": payment.description,
        "metadata": payment.meta,
        "created_at": payment.created_at.isoformat(),
        "processed_at": payment.processed_at.isoformat() if payment.processed_at else None,
    }


class WebhookSender:
    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    async def send(self, payment: Payment) -> None:
        try:
            response = await self._client.post(
                payment.webhook_url,
                json=webhook_payload(payment),
                headers={"Idempotency-Key": f"{payment.id}:{payment.status}"},
            )
        except httpx.HTTPError as error:
            raise WebhookDeliveryError(type(error).__name__) from error
        if not response.is_success:
            raise WebhookDeliveryError(f"HTTP {response.status_code}")
