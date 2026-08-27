from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

ENV_FILE_PATH = Path(__file__).resolve().parent.parent.parent / ".env"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(ENV_FILE_PATH) if ENV_FILE_PATH.exists() else ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS: str = Field(default="localhost:9092")
    KAFKA_RAW_TOPIC: str = Field(default="raw-telemetry")
    KAFKA_CHANGELOG_TOPIC: str = Field(default="changelog-topic")
    KAFKA_PROCESSED_TOPIC: str = Field(default="processed-averages")
    KAFKA_ALERTS_TOPIC: str = Field(default="alerts-topic")
    KAFKA_NUM_PARTITIONS: int = Field(default=6)
    KAFKA_REPLICATION_FACTOR: int = Field(default=1)

    # Stream Processing Engine Configuration
    WINDOW_SIZE_SECONDS: int = Field(default=300)
    WINDOW_SLIDE_SECONDS: int = Field(default=10)
    GRACE_PERIOD_SECONDS: int = Field(default=30)
    ROCKSDB_STATE_DIR: str = Field(default="./data/rocksdb_state")

    # Anomaly Detection Thresholds (Cold-Chain Frozen Food in Celsius)
    SAFE_TEMP_MIN: float = Field(default=-25.0)
    SAFE_TEMP_MAX: float = Field(default=-10.0)
    TEMP_SPIKE_TOLERANCE: float = Field(default=3.0)

    # Backend & Security
    API_HOST: str = Field(default="0.0.0.0")
    API_PORT: int = Field(default=8000)
    SECRET_KEY: str = Field(default="streamforge_super_secure_jwt_secret_key_2026_axlero_top_performer")
    JWT_ALGORITHM: str = Field(default="HS256")
    ACCESS_TOKEN_EXPIRE_MINUTES: int = Field(default=1440)

    # Webhook Alert Dispatcher
    WEBHOOK_ALERT_URL: str = Field(default="http://localhost:8000/api/v1/mock-webhook")
    WEBHOOK_RETRY_ATTEMPTS: int = Field(default=3)
    WEBHOOK_TIMEOUT_SECONDS: float = Field(default=5.0)

    # Simulation / Load Testing
    SIMULATION_RATE_PER_SEC: int = Field(default=100)
    SIMULATION_NUM_TRUCKS: int = Field(default=500)
    SIMULATION_NUM_CUSTOMERS: int = Field(default=5)


settings = Settings()
