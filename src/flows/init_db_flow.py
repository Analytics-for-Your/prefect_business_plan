# flows/init_db_flow.py
from prefect import flow, task
from database.init_db import init_db
from config.logger import setup_logger

# Инициализация логгера
logger = setup_logger(__name__)

@task
def initialize_database():
    """Задача для инициализации базы данных."""
    try:
        init_db()
        logger.info("Database initialization task completed successfully")
        return True
    except Exception as e:
        logger.error(f"Database initialization task failed: {str(e)}")
        raise

@flow(name="Initialize Database Flow")
def init_db_flow():
    """Поток для инициализации базы данных."""
    logger.info("Starting Initialize Database Flow")
    initialize_database()
    logger.info("Initialize Database Flow completed")

if __name__ == "__main__":
    init_db_flow()