"""
Central configuration. Reads from environment (.env locally, App Settings on Azure).
Never hardcode secrets - Azure deployment uses Managed Identity + Key Vault references.
"""
import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    # Storage (local: Azurite, Azure: real Blob/ADLS Gen2)
    azure_storage_conn_str: str = os.getenv(
        "AZURE_STORAGE_CONNECTION_STRING",
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
        "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq"
        "/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite:10000/devstoreaccount1;",
    )
    landing_container: str = os.getenv("LANDING_CONTAINER", "landing")
    bronze_container: str = os.getenv("BRONZE_CONTAINER", "bronze")

    # Warehouse (local: postgres container, Azure: Azure Database for PostgreSQL Flexible Server)
    pg_host: str = os.getenv("PG_HOST", "postgres")
    pg_port: str = os.getenv("PG_PORT", "5432")
    pg_db: str = os.getenv("PG_DB", "health_dw")
    pg_user: str = os.getenv("PG_USER", "etl_user")
    pg_password: str = os.getenv("PG_PASSWORD", "etl_password_local_only")

    # Observability
    app_insights_conn_str: str = os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING", "")
    log_level: str = os.getenv("LOG_LEVEL", "INFO")

    # Data quality gate
    max_quarantine_rate: float = float(os.getenv("MAX_QUARANTINE_RATE", "0.10"))  # 10%

    # Idempotency
    batch_date: str = os.getenv("BATCH_DATE", "")  # if blank, pipeline uses today (UTC)

    @property
    def pg_conn_str(self) -> str:
        return (
            f"postgresql+psycopg2://{self.pg_user}:{self.pg_password}"
            f"@{self.pg_host}:{self.pg_port}/{self.pg_db}"
        )


settings = Settings()
