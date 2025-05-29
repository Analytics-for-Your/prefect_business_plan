# config/settings.py
import os
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        extra="ignore",
        env_file=".env",
        env_file_encoding="utf-8",
    )

    # PREFECT
    prefect_api_base_url: str
    prefect_api_key: str
    prefect_api_ingress_port: int
    prefect_api_port: int

    # PostgreSQL
    psql_host: str
    psql_port: int
    psql_ingress_port: int
    psql_user: str
    psql_password: str
    psql_db: str
    psql_schema: str
    psql_sslmode: str

    @property
    def prefect_api_endpoint(self) -> str:
        """Return the Prefect API endpoint based on the environment."""
        port = self.prefect_api_port if os.getenv("KUBERNETES_SERVICE_HOST") else self.prefect_api_ingress_port
        # Use the base URL and replace the port dynamically
        return f"{self.prefect_api_base_url}:{port}/api"

    @property
    def postgres_connection_string_psql_db(self) -> str:
        port = self.psql_port if os.getenv("KUBERNETES_SERVICE_HOST") else self.psql_ingress_port
        return f"postgresql://{self.psql_user}:{self.psql_password}@{self.psql_host}:{port}/{self.psql_db}?sslmode={self.psql_sslmode}"

    @property
    def postgres_connection_string_postgres(self) -> str:
        port = self.psql_port if os.getenv("KUBERNETES_SERVICE_HOST") else self.psql_ingress_port
        return f"postgresql://{self.psql_user}:{self.psql_password}@{self.psql_host}:{port}/{'postgres'}?sslmode={self.psql_sslmode}"

# Вывод всех значений переменных окружения для диагностики
settings = Settings()
