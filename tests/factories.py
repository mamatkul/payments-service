from decimal import Decimal
from typing import Any

from payments.domain import Currency
from payments.services.payments import NewPayment


def payment_body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Order #42",
        "metadata": {"order_id": 42},
        "webhook_url": "https://merchant.example/webhooks/payments",
    }
    body.update(overrides)
    return body


def new_payment(**overrides: Any) -> NewPayment:
    values: dict[str, Any] = {
        "amount": Decimal("1500.00"),
        "currency": Currency.RUB,
        "description": "Order #42",
        "metadata": {"order_id": 42},
        "webhook_url": "https://merchant.example/webhooks/payments",
    }
    values.update(overrides)
    return NewPayment(**values)
