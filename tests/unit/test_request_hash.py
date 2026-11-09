from decimal import Decimal

from payments.domain import Currency
from payments.services.payments import request_hash
from tests.factories import new_payment


def test_same_request_gives_same_hash_regardless_of_key_order_and_amount_format() -> None:
    first = new_payment(amount=Decimal("100"), metadata={"a": 1, "b": 2})
    second = new_payment(amount=Decimal("100.00"), metadata={"b": 2, "a": 1})
    assert request_hash(first) == request_hash(second)


def test_any_field_change_changes_the_hash() -> None:
    base = request_hash(new_payment())
    variants = [
        new_payment(amount=Decimal("1500.01")),
        new_payment(currency=Currency.USD),
        new_payment(description="Other"),
        new_payment(metadata={"order_id": 43}),
        new_payment(webhook_url="https://other.example/hook"),
    ]
    assert all(request_hash(variant) != base for variant in variants)
