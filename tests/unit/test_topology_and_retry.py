import pytest

from payments.messaging.consumer import decide_on_failure
from payments.messaging.topology import (
    DLX_EXCHANGE,
    NEW_PAYMENTS_QUEUE,
    PAYMENTS_EXCHANGE,
    retry_delay_seconds,
    retry_queue,
    retry_routing_key,
)


def test_retry_delay_grows_exponentially() -> None:
    assert [retry_delay_seconds(n, 1.0) for n in (1, 2, 3)] == [1.0, 2.0, 4.0]
    assert retry_delay_seconds(2, 0.5) == 1.0


def test_retry_delay_rejects_attempt_zero() -> None:
    with pytest.raises(ValueError, match="attempt starts at 1"):
        retry_delay_seconds(0, 1.0)


def test_retry_queue_dead_letters_back_to_main_queue_after_its_ttl() -> None:
    queue = retry_queue(2, 1.0)
    assert queue.name == "payments.new.retry.2000ms"
    assert queue.arguments is not None
    assert {
        "x-message-ttl": 2000,
        "x-dead-letter-exchange": PAYMENTS_EXCHANGE.name,
        "x-dead-letter-routing-key": "payments.new",
    }.items() <= dict(queue.arguments).items()


def test_changing_the_base_delay_gives_new_retry_queues() -> None:
    assert retry_routing_key(1, 1.0) == "payments.new.retry.1000ms"
    assert retry_routing_key(1, 0.5) == "payments.new.retry.500ms"


def test_main_queue_dead_letters_to_dlx() -> None:
    assert NEW_PAYMENTS_QUEUE.arguments is not None
    assert NEW_PAYMENTS_QUEUE.arguments["x-dead-letter-exchange"] == DLX_EXCHANGE.name


@pytest.mark.parametrize(
    ("attempt", "action", "next_attempt"),
    [(1, "retry", 2), (2, "retry", 3), (3, "dead_letter", None), (7, "dead_letter", None)],
)
def test_three_attempts_then_dead_letter(
    attempt: int, action: str, next_attempt: int | None
) -> None:
    decision = decide_on_failure(attempt, max_attempts=3)
    assert decision.action == action
    assert decision.next_attempt == next_attempt
