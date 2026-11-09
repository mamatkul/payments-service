from functools import lru_cache

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", hide_input_in_errors=True)

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbitmq_url: SecretStr = SecretStr("amqp://guest:guest@localhost:5672/")
    api_key: SecretStr = Field(min_length=16)

    log_level: str = "INFO"

    outbox_poll_interval_seconds: float = Field(default=0.5, gt=0)
    outbox_batch_size: int = Field(default=100, ge=1, le=1000)

    max_attempts: int = Field(default=3, ge=1, le=10)
    retry_base_delay_seconds: float = Field(default=1.0, gt=0)
    webhook_timeout_seconds: float = Field(default=5.0, gt=0)

    gateway_min_delay_seconds: float = Field(default=2.0, ge=0)
    gateway_max_delay_seconds: float = Field(default=5.0, ge=0)
    gateway_success_rate: float = Field(default=0.9, ge=0, le=1)


@lru_cache
def get_settings() -> Settings:
    return Settings()
