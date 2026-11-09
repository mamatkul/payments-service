import json
import uuid
from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest
import respx

from payments.db.models import Payment
from payments.services.webhooks import WebhookDeliveryError, WebhookSender

URL = "https://merchant.example/hook"


def make_payment() -> Payment:
    return Payment(
        id=uuid.UUID("00000000-0000-0000-0000-000000000001"),
        amount=Decimal("10.50"),
        currency="EUR",
        description="Coffee",
        meta={"table": 4},
        status="succeeded",
        idempotency_key="key",
        request_hash="0" * 64,
        webhook_url=URL,
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        processed_at=datetime(2026, 1, 1, 0, 0, 3, tzinfo=UTC),
    )


@respx.mock
async def test_posts_payment_result_with_idempotency_key() -> None:
    route = respx.post(URL).mock(return_value=httpx.Response(204))
    async with httpx.AsyncClient() as client:
        await WebhookSender(client).send(make_payment())

    request = route.calls.last.request
    assert request.headers["Idempotency-Key"] == f"{make_payment().id}:succeeded"
    assert json.loads(request.content) == {
        "event": "payment.succeeded",
        "payment_id": "00000000-0000-0000-0000-000000000001",
        "status": "succeeded",
        "amount": "10.50",
        "currency": "EUR",
        "description": "Coffee",
        "metadata": {"table": 4},
        "created_at": "2026-01-01T00:00:00+00:00",
        "processed_at": "2026-01-01T00:00:03+00:00",
    }


@respx.mock
async def test_non_2xx_is_a_delivery_error() -> None:
    respx.post(URL).mock(return_value=httpx.Response(503))
    async with httpx.AsyncClient() as client:
        with pytest.raises(WebhookDeliveryError, match="HTTP 503"):
            await WebhookSender(client).send(make_payment())


@respx.mock
async def test_network_error_is_a_delivery_error() -> None:
    respx.post(URL).mock(side_effect=httpx.ConnectError("refused"))
    async with httpx.AsyncClient() as client:
        with pytest.raises(WebhookDeliveryError, match="ConnectError"):
            await WebhookSender(client).send(make_payment())
