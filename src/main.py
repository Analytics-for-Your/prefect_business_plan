# src/main.py
import sys
from config.logger import setup_logger
# import flows
from flows.init_db_flow import init_db_flow
from flows.init_projects import init_projects_flow
from flows.init_timelile import init_timeline_flow
from flows.import_flow import import_flow

logger = setup_logger(__name__)

def main():
    """Основная функция для запуска приложения."""
    try:
        logger.info("Starting main application")
        
        # Инициализация базы данных
        logger.info("Running database initialization")
        init_db_flow()

        # Инициализация проектов. названия и нулевые значения
        logger.info("Running tables initialization")
        init_projects_flow()
        init_timeline_flow()

        # # Очистка базы данных
        # logger.info("Running database clearance")
        # clean_db_flow()

        # Запуск основного pipeline импорта данных из файлов
        logger.info("Running Data Pipeline Flow")
        import_flow()
        
        # # Дорасчеты таблиц и полей перед основными вычислениями
        # logger.info("Running Data Preprocessing Flow")
        # init_data_preprocessing_flow()

        # # Вычисления
        # logger.info("Running Calculations Flow")
        # calculations_flow()

        # # Экспорт результатов
        # logger.info("Running Export Data Flow")
        # export_data_flow()

        # logger.info("Main application completed successfully")
        # return 0
    except Exception as e:
        logger.error(f"Main application failed: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main())