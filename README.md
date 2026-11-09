# Payments Service

Микросервис асинхронной обработки платежей: принимает платёж по API, проводит его через эмулятор платёжного шлюза и сообщает результат клиенту вебхуком.

Стек: FastAPI, Pydantic v2, SQLAlchemy 2.0 (async), PostgreSQL, RabbitMQ (FastStream), Alembic, Docker.

## Архитектура

```
 клиент ──POST /api/v1/payments──▶ api ──(одна транзакция)──▶ payments + outbox
                                                                    │
                                              outbox-relay ◀────────┘  SELECT … FOR UPDATE SKIP LOCKED
                                                   │ publish (publisher confirms)
                                                   ▼
                     exchange payments ──payments.new──▶ queue payments.new ──▶ consumer
                                                                                  │ шлюз (2–5 с, 90 % успех)
                                                                                  │ статус в БД
                                                                                  │ webhook
                              ошибка, попытка < 3                                 │
     queue payments.new.retry.<delay>ms ◀── exchange payments.retry ◀─────────────┤
           │ TTL истёк                                                            │
           └──────────────▶ exchange payments ──▶ payments.new                    │
                              ошибка, попытка = 3                                 │
     queue payments.new.dlq ◀── exchange payments.dlx ◀───────────────────────────┘
```

| Сервис | Что делает |
|---|---|
| `api` | REST API, пишет платёж и событие в outbox в одной транзакции |
| `outbox-relay` | публикует события из outbox в RabbitMQ |
| `consumer` | обрабатывает `payments.new`: шлюз → статус → вебхук, retry и DLQ |
| `migrations` | применяет миграции Alembic и завершается |
| `postgres`, `rabbitmq` | инфраструктура (UI RabbitMQ: http://localhost:15672, guest/guest) |

### Гарантии

- **Outbox.** Платёж и событие `payment.created` сохраняются одной транзакцией, поэтому событие не теряется, даже если брокер недоступен. Relay забирает строки через `FOR UPDATE SKIP LOCKED` (несколько relay не опубликуют одно событие одновременно), публикует с publisher confirms и только после подтверждения ставит `published_at`. Доставка at-least-once: при падении между подтверждением и коммитом событие уйдёт повторно, поэтому consumer идемпотентен.
- **Идемпотентность API.** `Idempotency-Key` уникален в БД. Вставка идёт через `INSERT … ON CONFLICT DO NOTHING`, так что параллельные запросы с одним ключом создают ровно один платёж. Повтор с тем же телом возвращает тот же платёж, с другим телом — `409 Conflict`.
- **Идемпотентность consumer.** Статус меняется через compare-and-set (`WHERE status = 'pending'`), поэтому повторная доставка не проводит платёж второй раз. Вебхук шлётся, пока не установлен `webhook_delivered_at`. В заголовке вебхука передаётся `Idempotency-Key: <payment_id>:<status>`, по нему получатель может отбросить дубль.
- **Retry.** При любой ошибке обработки (недоступен вебхук, БД и т. п.) сообщение уходит в retry-очередь своей попытки с экспоненциальной задержкой `RETRY_BASE_DELAY_SECONDS · 2^(n-1)`: по умолчанию 1 с, затем 2 с. У каждой задержки своя очередь с TTL: так нет head-of-line blocking, который бывает с per-message TTL. Задержка входит в имя очереди, поэтому смена настройки не ломает объявление уже существующих очередей.
- **DLQ.** После 3 неудачных попыток (`MAX_ATTEMPTS`) сообщение попадает в `payments.new.dlq` с заголовками `x-attempt` и `x-last-error`. Битое сообщение уходит в DLQ сразу. Подтверждение (ack) ручное и выполняется только после того, как брокер подтвердил публикацию в retry или DLQ, поэтому сообщение не теряется.
- **Отказ шлюза** (10 % случаев) — это бизнес-результат: платёж получает статус `failed`, клиент получает вебхук. Повторно такой платёж не проводится.

## Запуск

Нужны Docker и Docker Compose.

```bash
cp .env.example .env        # при желании поменяйте API_KEY
docker compose up -d --build
```

API: http://localhost:8000, Swagger: http://localhost:8000/docs.

Чтобы посмотреть вебхуки локально, поднимите стек вместе с тестовым получателем, который логирует всё, что ему приходит:

```bash
docker compose --profile demo up -d --build
docker compose logs -f webhook-receiver
```

Остальные команды: `make help`.

## Примеры

```bash
export API_KEY=change-me-to-a-long-random-key

curl -s -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: $API_KEY" \
  -H "Idempotency-Key: order-42" \
  -H "Content-Type: application/json" \
  -d '{
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Order #42",
        "metadata": {"order_id": 42},
        "webhook_url": "http://webhook-receiver:8080/payments"
      }'
```

```json
{"payment_id": "5d3c…", "status": "pending", "created_at": "2026-11-09T20:31:18.825591Z"}
```

Статус через несколько секунд:

```bash
curl -s http://localhost:8000/api/v1/payments/<payment_id> -H "X-API-Key: $API_KEY"
```

```json
{
  "payment_id": "5d3c…",
  "amount": "1500.00",
  "currency": "RUB",
  "description": "Order #42",
  "metadata": {"order_id": 42},
  "status": "succeeded",
  "idempotency_key": "order-42",
  "webhook_url": "http://webhook-receiver:8080/payments",
  "webhook_delivered_at": "2026-11-09T20:31:22.120265Z",
  "created_at": "2026-11-09T20:31:18.825591Z",
  "processed_at": "2026-11-09T20:31:21.905763Z"
}
```

Вебхук, который получит клиент:

```json
{
  "event": "payment.succeeded",
  "payment_id": "5d3c…",
  "status": "succeeded",
  "amount": "1500.00",
  "currency": "RUB",
  "description": "Order #42",
  "metadata": {"order_id": 42},
  "created_at": "…",
  "processed_at": "…"
}
```

Ответы API:

| Код | Когда |
|---|---|
| `202` | платёж принят (или повтор с тем же `Idempotency-Key` и телом) |
| `401` | нет `X-API-Key` или он неверный |
| `404` | платёж не найден |
| `409` | `Idempotency-Key` уже использован с другим телом |
| `422` | невалидное тело или нет `Idempotency-Key` |

Путь до DLQ: укажите недоступный `webhook_url` (например, `http://unreachable.invalid/hook`). В логах consumer будут три попытки с паузами 1 и 2 секунды, после чего сообщение окажется в очереди `payments.new.dlq` (видно в UI RabbitMQ).

## Разработка

Нужны Python 3.12 и [uv](https://docs.astral.sh/uv/).

```bash
make install                       # зависимости и pre-commit хуки
docker compose up -d postgres rabbitmq
make test                          # все тесты с покрытием
make lint typecheck                # ruff и mypy --strict
```

Тесты:

- `tests/unit`: логика без внешних зависимостей. Consumer проверяется на in-memory брокере FastStream: ack, retry с номером попытки, DLQ.
- `tests/integration`: нужен PostgreSQL. Схема создаётся настоящими миграциями. Проверяются API, идемпотентность (включая гонку параллельных запросов), outbox relay (порядок, сбой публикации, конкурентные relay) и обработка платежа.
- `tests/rabbitmq`: нужен RabbitMQ с management API. Сквозной путь сообщения через реальные retry-очереди с TTL до DLQ. Тест работает в отдельном vhost `payments_test` (создаётся автоматически), поэтому не конфликтует с запущенным стеком и не трогает его очереди. Если брокер недоступен, тест пропускается.

Адреса для тестов задаются через `TEST_DATABASE_URL`, `TEST_RABBITMQ_URL` и `TEST_RABBITMQ_MANAGEMENT_URL` (по умолчанию localhost из `docker compose`).

Запуск без Docker (PostgreSQL и RabbitMQ должны быть запущены):

```bash
make migrate
make run-api        # в отдельных терминалах
make run-relay
make run-consumer
```

## Конфигурация

Переменные окружения (см. `.env.example`):

| Переменная | По умолчанию | Описание |
|---|---|---|
| `API_KEY` | — | статический ключ для `X-API-Key`, минимум 16 символов |
| `DATABASE_URL` | `postgresql+asyncpg://payments:payments@localhost:5432/payments` | PostgreSQL |
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` | RabbitMQ |
| `OUTBOX_POLL_INTERVAL_SECONDS` | `0.5` | пауза relay, когда outbox пуст |
| `OUTBOX_BATCH_SIZE` | `100` | событий за одну транзакцию relay |
| `MAX_ATTEMPTS` | `3` | попыток обработки до DLQ |
| `RETRY_BASE_DELAY_SECONDS` | `1` | база экспоненциальной задержки |
| `WEBHOOK_TIMEOUT_SECONDS` | `5` | таймаут запроса вебхука |
| `GATEWAY_MIN_DELAY_SECONDS` / `GATEWAY_MAX_DELAY_SECONDS` | `2` / `5` | задержка эмулятора шлюза |
| `GATEWAY_SUCCESS_RATE` | `0.9` | доля успешных платежей |

## Структура

```
src/payments/
  api/          FastAPI: приложение, роуты, схемы, аутентификация
  db/           модели SQLAlchemy и сессии
  messaging/    топология RabbitMQ, outbox relay, consumer
  services/     создание платежа, эмулятор шлюза, обработка, вебхуки
migrations/     Alembic
tests/          unit, integration, rabbitmq
```

## Что можно улучшить

- Вместо поллинга outbox — `LISTEN/NOTIFY` или CDC (Debezium).
- Метрики (Prometheus) и трейсинг (OpenTelemetry) для relay и consumer.
- Подпись вебхуков (HMAC), чтобы получатель мог проверить отправителя.
- Очистка опубликованных строк outbox по расписанию.
- Ротация API-ключей, ключи на клиента.
