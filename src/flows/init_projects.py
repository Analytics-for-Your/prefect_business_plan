from prefect import flow, task
from data_init.initialize_projects import initialize_projects
from config.logger import setup_logger

# Инициализация логгера
logger = setup_logger(__name__)

@task
def initialize_projects_task():
    """Задача для инициализации списка проектов."""
    try:
        initialize_projects()
        logger.info("Projects initialization task completed successfully")
        return True
    except Exception as e:
        logger.error(f"Projects initialization task failed: {str(e)}")
        raise

@flow(name="Initialize Projects Flow")
def init_projects_flow():
    """Поток для инициализации базы данных и списка проектов."""
    logger.info("Starting Initialize Projects Flow")
    initialize_projects_task()
    logger.info("Initialize Projects Flow completed")

if __name__ == "__main__":
    init_projects_flow()