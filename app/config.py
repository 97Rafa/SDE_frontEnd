from pydantic_settings import BaseSettings # pyright: ignore[reportMissingImports]


class Settings(BaseSettings):
    # Kafka
    kafka_broker: str = "kafka1:9092"
    req_topic: str = "request_topic"
    dat_topic: str = "data_topic"
    est_topic: str = "estimation_topic"
    log_topic: str = "logging_topic"

    # Timeouts
    response_timeout: int = 8
    estimation_timeout: int = 5

    # Parallelism
    parallelism: int = 4

    class Config:
        env_file = ".env"


settings = Settings()
