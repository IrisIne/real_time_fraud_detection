from dataclasses import dataclass
import os


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    scores_topic: str = os.getenv("KAFKA_SCORES_TOPIC", "scores")
    consumer_group: str = os.getenv("KAFKA_CONSUMER_GROUP", "scores-to-postgres-v1")
    postgres_host: str = os.getenv("POSTGRES_HOST", "localhost")
    postgres_port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    postgres_db: str = os.getenv("POSTGRES_DB", "fraud_detection")
    postgres_user: str = os.getenv("POSTGRES_USER", "fraud_user")
    postgres_password: str = os.getenv("POSTGRES_PASSWORD", "fraud_password")

    @property
    def postgres_dsn(self) -> str:
        return (
            f"host={self.postgres_host} port={self.postgres_port} "
            f"dbname={self.postgres_db} user={self.postgres_user} "
            f"password={self.postgres_password} connect_timeout=5"
        )
