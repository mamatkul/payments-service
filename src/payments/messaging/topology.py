from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

PAYMENTS_EXCHANGE = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)
DLX_EXCHANGE = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)
RETRY_EXCHANGE = RabbitExchange("payments.retry", type=ExchangeType.DIRECT, durable=True)

NEW_PAYMENTS_ROUTING_KEY = "payments.new"
DLQ_ROUTING_KEY = "payments.new.dlq"

ATTEMPT_HEADER = "x-attempt"

NEW_PAYMENTS_QUEUE = RabbitQueue(
    "payments.new",
    durable=True,
    routing_key=NEW_PAYMENTS_ROUTING_KEY,
    arguments={
        "x-dead-letter-exchange": DLX_EXCHANGE.name,
        "x-dead-letter-routing-key": DLQ_ROUTING_KEY,
    },
)

DEAD_LETTER_QUEUE = RabbitQueue("payments.new.dlq", durable=True, routing_key=DLQ_ROUTING_KEY)


def retry_routing_key(attempt: int, base_delay_seconds: float) -> str:
    delay_ms = int(retry_delay_seconds(attempt, base_delay_seconds) * 1000)
    return f"payments.new.retry.{delay_ms}ms"


def retry_delay_seconds(attempt: int, base_delay_seconds: float) -> float:
    if attempt < 1:
        raise ValueError("attempt starts at 1")
    return float(base_delay_seconds * 2 ** (attempt - 1))


def retry_queue(attempt: int, base_delay_seconds: float) -> RabbitQueue:
    return RabbitQueue(
        retry_routing_key(attempt, base_delay_seconds),
        durable=True,
        routing_key=retry_routing_key(attempt, base_delay_seconds),
        arguments={
            "x-message-ttl": int(retry_delay_seconds(attempt, base_delay_seconds) * 1000),
            "x-dead-letter-exchange": PAYMENTS_EXCHANGE.name,
            "x-dead-letter-routing-key": NEW_PAYMENTS_ROUTING_KEY,
        },
    )


async def declare_topology(broker: RabbitBroker, *, max_attempts: int, base_delay: float) -> None:
    payments = await broker.declare_exchange(PAYMENTS_EXCHANGE)
    dlx = await broker.declare_exchange(DLX_EXCHANGE)
    retry = await broker.declare_exchange(RETRY_EXCHANGE)

    new_payments = await broker.declare_queue(NEW_PAYMENTS_QUEUE)
    await new_payments.bind(payments, routing_key=NEW_PAYMENTS_ROUTING_KEY)

    dead_letters = await broker.declare_queue(DEAD_LETTER_QUEUE)
    await dead_letters.bind(dlx, routing_key=DLQ_ROUTING_KEY)

    for attempt in range(1, max_attempts):
        queue = await broker.declare_queue(retry_queue(attempt, base_delay))
        await queue.bind(retry, routing_key=retry_routing_key(attempt, base_delay))
