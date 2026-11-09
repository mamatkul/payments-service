import uuid

from pydantic import BaseModel, ConfigDict


class PaymentCreatedEvent(BaseModel):
    model_config = ConfigDict(extra="ignore")

    payment_id: uuid.UUID
