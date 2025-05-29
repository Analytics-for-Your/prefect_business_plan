# database/db.py
import peewee
import os

from config.logger import setup_logger
from config.settings import settings

# Инициализация логгера
logger = setup_logger(__name__)

# Создаём подключение к базе данных
try:
    db = peewee.PostgresqlDatabase(
        database=settings.psql_db,
        user=settings.psql_user,
        password=settings.psql_password,
        host=settings.psql_host,
        port=settings.psql_port if os.getenv("KUBERNETES_SERVICE_HOST") else settings.psql_ingress_port,
        sslmode=settings.psql_sslmode
    )
    logger.info("Database connection initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize database connection: {str(e)}")
    raise