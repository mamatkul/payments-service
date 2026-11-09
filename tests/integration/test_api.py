import asyncio
import uuid
from typing import Any

import httpx
import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payments.db.models import OutboxMessage, Payment
from tests.factories import payment_body

PAYMENTS = "/api/v1/payments"


async def create(client: httpx.AsyncClient, key: str = "key-1", **overrides: Any) -> httpx.Response:
    return await client.post(
        PAYMENTS, json=payment_body(**overrides), headers={"Idempotency-Key": key}
    )


async def count(session_factory: async_sessionmaker[AsyncSession], model: type) -> int:
    async with session_factory() as session:
        return int(await session.scalar(select(func.count()).select_from(model)) or 0)


async def test_create_returns_202_and_writes_payment_with_outbox_event(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    response = await create(client)

    assert response.status_code == 202
    body = response.json()
    assert set(body) == {"payment_id", "status", "created_at"}
    assert body["status"] == "pending"

    async with session_factory() as session:
        event = (await session.scalars(select(OutboxMessage))).one()
    assert event.payload == {"payment_id": body["payment_id"]}
    assert event.routing_key == "payments.new"
    assert event.published_at is None


async def test_get_returns_details(client: httpx.AsyncClient) -> None:
    payment_id = (await create(client)).json()["payment_id"]

    response = await client.get(f"{PAYMENTS}/{payment_id}")

    assert response.status_code == 200
    body = response.json()
    assert body["payment_id"] == payment_id
    assert body["amount"] == "1500.00"
    assert body["currency"] == "RUB"
    assert body["metadata"] == {"order_id": 42}
    assert body["status"] == "pending"
    assert body["idempotency_key"] == "key-1"
    assert body["processed_at"] is None


async def test_get_unknown_payment_is_404(client: httpx.AsyncClient) -> None:
    response = await client.get(f"{PAYMENTS}/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_repeated_request_returns_the_same_payment_without_a_second_event(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    first = await create(client)
    second = await create(client)

    assert second.status_code == 202
    assert second.json() == first.json()
    assert await count(session_factory, Payment) == 1
    assert await count(session_factory, OutboxMessage) == 1


async def test_key_reused_with_different_body_is_409(client: httpx.AsyncClient) -> None:
    await create(client)
    response = await create(client, amount="1.00")
    assert response.status_code == 409


async def test_concurrent_requests_with_one_key_create_one_payment(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    responses = await asyncio.gather(*(create(client, key="race") for _ in range(10)))

    assert {r.status_code for r in responses} == {202}
    assert len({r.json()["payment_id"] for r in responses}) == 1
    assert await count(session_factory, Payment) == 1
    assert await count(session_factory, OutboxMessage) == 1


async def test_idempotency_key_header_is_required(client: httpx.AsyncClient) -> None:
    response = await client.post(PAYMENTS, json=payment_body())
    assert response.status_code == 422


@pytest.mark.parametrize(
    "overrides",
    [
        {"amount": "0"},
        {"amount": "-5"},
        {"amount": "1.005"},
        {"currency": "GBP"},
        {"description": ""},
        {"webhook_url": "not-a-url"},
        {"webhook_url": "ftp://merchant.example/hook"},
        {"unexpected": "field"},
    ],
)
async def test_invalid_body_is_422(client: httpx.AsyncClient, overrides: dict[str, Any]) -> None:
    response = await create(client, **overrides)
    assert response.status_code == 422


@pytest.mark.parametrize("headers", [{}, {"X-API-Key": "wrong-key"}])
async def test_every_endpoint_requires_the_api_key(
    client: httpx.AsyncClient, headers: dict[str, str]
) -> None:
    client.headers.pop("X-API-Key")
    created = await client.post(
        PAYMENTS, json=payment_body(), headers={"Idempotency-Key": "k", **headers}
    )
    fetched = await client.get(f"{PAYMENTS}/{uuid.uuid4()}", headers=headers)
    assert created.status_code == fetched.status_code == 401


async def test_health_is_open(client: httpx.AsyncClient) -> None:
    client.headers.pop("X-API-Key")
    response = await client.get("/health")
    assert response.status_code == 200
