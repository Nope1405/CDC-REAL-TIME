"""Runtime configuration, read from environment variables."""

from __future__ import annotations

from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=None, extra="ignore")

    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_group_id: str = "cdc-es-sink"
    kafka_auto_offset_reset: Literal["earliest", "latest"] = "earliest"
    kafka_max_poll_interval_ms: int = 300_000
    kafka_session_timeout_ms: int = 45_000
    products_topic: str = "cdc.public.products"
    orders_topic: str = "cdc.outbox.order"
    # Comma-separated subset of: products, orders
    entities: str = "products,orders"
    dlq_topic_prefix: str = "cdc.dlq."

    # Elasticsearch (alias names by default; point at products_v2 during a rebuild)
    es_url: str = "http://localhost:9200"
    es_index_products: str = "products"
    es_index_orders: str = "orders"
    es_request_timeout_s: float = 30.0

    # Batching
    batch_size: int = 500
    flush_interval_ms: int = 500

    # Semantics
    delete_mode: Literal["soft", "hard"] = "soft"
    toast_placeholder: str = "__debezium_unavailable_value"

    # Retry / shutdown
    retry_initial_ms: int = 200
    retry_max_ms: int = 30_000
    revoke_flush_timeout_s: float = 10.0

    # Observability
    metrics_port: int = 8000
    log_level: str = "INFO"
    health_stale_after_s: float = 60.0

    def topic_entities(self) -> dict[str, str]:
        """Subscribed topic -> entity name."""
        wanted = {e.strip() for e in self.entities.split(",") if e.strip()}
        mapping = {self.products_topic: "products", self.orders_topic: "orders"}
        return {topic: entity for topic, entity in mapping.items() if entity in wanted}

    def index_for(self, entity: str) -> str:
        return {"products": self.es_index_products, "orders": self.es_index_orders}[entity]

    def dlq_topic_for(self, entity: str) -> str:
        return f"{self.dlq_topic_prefix}{entity}"
