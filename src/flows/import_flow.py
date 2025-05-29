#flows/import_flow.py
from prefect import flow, task
from importers.import_sales import import_sales
from config.logger import setup_logger

# Initialize logger
logger = setup_logger(__name__)

@task(retries=2, retry_delay_seconds=60)
def import_sales_task():
    """Import sales data from Excel files."""
    try:
        import_sales()
        logger.info("Sales data import task completed successfully")
        return True
    except Exception as e:
        logger.error(f"Sales data import task failed: {str(e)}")
        raise

@flow(name="Import Sales Data Flow")
def import_flow():
    """Flow to import sales data."""
    logger.info("Starting Import Sales Data Flow")
    import_sales_task()
    logger.info("Import Sales Data Flow completed")

if __name__ == "__main__":
    import_flow()