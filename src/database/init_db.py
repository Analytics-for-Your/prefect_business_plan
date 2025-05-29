import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from peewee import DatabaseError
from config.logger import setup_logger
from config.settings import settings
from database.db import db
from database.models import (
    ProjectModel,
    SalesModel,
    OrdersModel,
    LogisticsPaymentsTermsModel,
    OrdersPaymentsTermsModel,
    PaymentsModel,
    StockBudgetModel
)

# Инициализация логгера
logger = setup_logger(__name__)

MODELS = [
    ProjectModel,
    SalesModel,
    OrdersModel,
    LogisticsPaymentsTermsModel,
    OrdersPaymentsTermsModel,
    PaymentsModel,
    StockBudgetModel
]

def create_database_if_not_exists():
    """Создаёт базу данных, если она не существует."""
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user=settings.psql_user,
            password=settings.psql_password,
            host=settings.psql_host,
            port=settings.psql_port if os.getenv("KUBERNETES_SERVICE_HOST") else settings.psql_ingress_port
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()

        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (settings.psql_db,))
        exists = cursor.fetchone()

        if not exists:
            cursor.execute(f"CREATE DATABASE {settings.psql_db}")
            logger.info(f"Database '{settings.psql_db}' created successfully")
        else:
            logger.info(f"Database '{settings.psql_db}' already exists")

        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Failed to create/check database: {str(e)}")
        raise

def create_schema_if_not_exists():
    """Создаёт схему, если она не существует."""
    try:
        conn = psycopg2.connect(
            dbname=settings.psql_db,
            user=settings.psql_user,
            password=settings.psql_password,
            host=settings.psql_host,
            port=settings.psql_port if os.getenv("KUBERNETES_SERVICE_HOST") else settings.psql_ingress_port
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()

        schema = settings.psql_schema
        cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s", (schema,))
        exists = cursor.fetchone()

        if not exists:
            cursor.execute(f"CREATE SCHEMA {schema}")
            logger.info(f"Schema '{schema}' created successfully")
        else:
            logger.info(f"Schema '{schema}' already exists")

        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Failed to create/check schema: {str(e)}")
        raise

def create_tables_if_not_exists():
    """Создаёт таблицы, если они не существуют."""
    try:
        db.connect()
        
        # Устанавливаем search_path для текущего соединения
        schema = settings.psql_schema or "public"
        db.execute_sql(f"SET search_path TO {schema}")
        logger.info(f"Set search_path to schema '{schema}'")

        for model in MODELS:
            try:
                db.create_tables([model], safe=True)
                logger.info(f"Table '{model._meta.table_name}' checked/created successfully in schema '{schema}'")
            except DatabaseError as e:
                logger.error(f"Failed to create table '{model._meta.table_name}' in schema '{schema}': {str(e)}")
                raise
            except Exception as e:
                logger.error(f"Unexpected error creating table '{model._meta.table_name}' in schema '{schema}': {str(e)}")
                raise

        db.close()
    except DatabaseError as e:
        logger.error(f"Failed to create tables: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during table creation: {str(e)}")
        raise

def init_db():
    """Инициализирует базу данных, схему и таблицы."""
    logger.info("Starting database initialization")
    try:
        create_database_if_not_exists()
        create_schema_if_not_exists()
        create_tables_if_not_exists()
        logger.info("Database initialization completed successfully")
    except Exception as e:
        logger.error(f"Database initialization failed: {str(e)}")
        raise

def clear_all_tables():
    """Очищает все записи из всех таблиц, не удаляя сами таблицы."""
    try:
        db.connect()
        schema = settings.psql_schema or "public"
        db.execute_sql(f"SET search_path TO {schema}")
        for model in MODELS:
            model.delete().execute()
            logger.info(f"Table '{model._meta.table_name}' cleared successfully in schema '{schema}'")
        logger.info("All tables cleared successfully")
        db.close()
    except DatabaseError as e:
        logger.error(f"Failed to clear tables: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during table clearing: {str(e)}")
        raise

if __name__ == "__main__":
    init_db()