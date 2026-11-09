import asyncio
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from payments.domain import PaymentStatus


@dataclass(slots=True)
class EmulatedGateway:
    min_delay_seconds: float = 2.0
    max_delay_seconds: float = 5.0
    success_rate: float = 0.9
    rng: random.Random | None = None
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep

    def __post_init__(self) -> None:
        if self.min_delay_seconds > self.max_delay_seconds:
            raise ValueError("min_delay_seconds must not exceed max_delay_seconds")
        if self.rng is None:
            self.rng = random.Random()  # noqa: S311

    async def charge(self) -> PaymentStatus:
        assert self.rng is not None
        await self.sleep(self.rng.uniform(self.min_delay_seconds, self.max_delay_seconds))
        return (
            PaymentStatus.SUCCEEDED
            if self.rng.random() < self.success_rate
            else PaymentStatus.FAILED
        )
