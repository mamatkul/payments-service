import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, status

from payments.api.dependencies import SessionDep, require_api_key
from payments.api.schemas import ErrorResponse, PaymentAccepted, PaymentCreate, PaymentDetail
from payments.db.models import Payment
from payments.services.payments import (
    IdempotencyKeyReusedError,
    NewPayment,
    create_payment,
    get_payment,
)

router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    dependencies=[Depends(require_api_key)],
    responses={401: {"model": ErrorResponse}},
)


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentAccepted,
    responses={409: {"model": ErrorResponse}},
)
async def create(
    body: PaymentCreate,
    session: SessionDep,
    idempotency_key: Annotated[str, Header(min_length=1, max_length=255)],
) -> PaymentAccepted:
    try:
        result = await create_payment(
            session,
            idempotency_key=idempotency_key,
            data=NewPayment(
                amount=body.amount,
                currency=body.currency,
                description=body.description,
                metadata=body.metadata,
                webhook_url=str(body.webhook_url),
            ),
        )
    except IdempotencyKeyReusedError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Idempotency-Key was already used with a different request body",
        ) from None
    payment = result.payment
    return PaymentAccepted(
        payment_id=payment.id, status=payment.status, created_at=payment.created_at
    )


@router.get(
    "/{payment_id}",
    response_model=PaymentDetail,
    responses={404: {"model": ErrorResponse}},
)
async def retrieve(payment_id: uuid.UUID, session: SessionDep) -> PaymentDetail:
    payment = await get_payment(session, payment_id)
    if payment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found")
    return _detail(payment)


def _detail(payment: Payment) -> PaymentDetail:
    return PaymentDetail(
        payment_id=payment.id,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.meta,
        status=payment.status,
        idempotency_key=payment.idempotency_key,
        webhook_url=payment.webhook_url,
        webhook_delivered_at=payment.webhook_delivered_at,
        created_at=payment.created_at,
        processed_at=payment.processed_at,
    )
