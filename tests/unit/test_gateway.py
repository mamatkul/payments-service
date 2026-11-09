import random

import pytest

from payments.domain import PaymentStatus
from payments.services.gateway import EmulatedGateway


class RecordingSleep:
    def __init__(self) -> None:
        self.delays: list[float] = []

    async def __call__(self, delay: float) -> None:
        self.delays.append(delay)


async def test_charge_waits_within_bounds_and_mostly_succeeds() -> None:
    sleep = RecordingSleep()
    gateway = EmulatedGateway(rng=random.Random(7), sleep=sleep)

    outcomes = [await gateway.charge() for _ in range(2000)]

    assert all(2.0 <= delay <= 5.0 for delay in sleep.delays)
    success_share = outcomes.count(PaymentStatus.SUCCEEDED) / len(outcomes)
    assert 0.87 < success_share < 0.93
    assert set(outcomes) == {PaymentStatus.SUCCEEDED, PaymentStatus.FAILED}


@pytest.mark.parametrize(
    ("rate", "expected"), [(1.0, PaymentStatus.SUCCEEDED), (0.0, PaymentStatus.FAILED)]
)
async def test_success_rate_is_respected(rate: float, expected: PaymentStatus) -> None:
    gateway = EmulatedGateway(success_rate=rate, rng=random.Random(1), sleep=RecordingSleep())
    assert await gateway.charge() is expected


def test_invalid_delay_range_is_rejected() -> None:
    with pytest.raises(ValueError, match="min_delay_seconds"):
        EmulatedGateway(min_delay_seconds=5, max_delay_seconds=2)
