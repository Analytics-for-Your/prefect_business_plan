# data_init/initialize_projects.py
import uuid
from peewee import DatabaseError, DoesNotExist
from config.logger import setup_logger
from database.models import ProjectModel
from database.db import db
from config.settings import settings

# Инициализация логгера
logger = setup_logger(__name__)

def initialize_projects():
    """Обновляет статусы существующих проектов или создает новые в таблице ProjectModel."""
    currencies = ["USD"]  # Список валют "RUB", "CNY", "EUR"
    projects = [
        {"project_name": "DP Technology Wireless", "status": "close"},
        {"project_name": "EMOTIVE GuitarsAcoustic", "status": "new"},
        {"project_name": "EMOTIVE KeysPiano", "status": "new"},
        {"project_name": "ROCKDALE Benches", "status": "active"},
        {"project_name": "ROCKDALE Cables", "status": "active"},
        {"project_name": "ROCKDALE Drums", "status": "active"},
        {"project_name": "ROCKDALE DrumSticks", "status": "active"},
        {"project_name": "ROCKDALE GuitarBelts&Bags", "status": "active"},
        {"project_name": "ROCKDALE GuitarsAcoustic", "status": "active"},
        {"project_name": "ROCKDALE GuitarsElectric", "status": "active"},
        {"project_name": "ROCKDALE KeysPiano", "status": "active"},
        {"project_name": "ROCKDALE KeysSynth", "status": "active"},
        {"project_name": "ROCKDALE PRO Wireless", "status": "active"},
        {"project_name": "ROCKDALE Stands", "status": "active"},
        {"project_name": "ROCKDALE Strings", "status": "active"},
        {"project_name": "UPTONE Benches", "status": "active"},
        {"project_name": "UPTONE DrumSticks", "status": "active"},
        {"project_name": "UPTONE GuitarBelts", "status": "active"},
        {"project_name": "UPTONE Liquids", "status": "active"},
        {"project_name": "UPTONE Stands", "status": "active"},
        {"project_name": "UPTONE Strings", "status": "active"},
        {"project_name": "YARGO Drums", "status": "active"},
        {"project_name": "YARGO KeysSynth", "status": "active"}
    ]
    try:
        db.connect()
        schema = settings.psql_schema or "public"
        db.execute_sql(f"SET search_path TO {schema}")
        logger.info(f"Set search_path to schema '{schema}'")

        with db.atomic():
            updated = 0
            created = 0
            for project in projects:
                for currency in currencies:
                    try:
                        # Проверяем наличие проекта по project_name и currency
                        existing_project = ProjectModel.get(
                            ProjectModel.project_name == project["project_name"],
                            ProjectModel.currency == currency
                        )
                        # Обновляем статус, если проект существует
                        existing_project.status = project["status"]
                        existing_project.save()
                        updated += 1
                        logger.debug(f"Updated status to '{project['status']}' for project '{project['project_name']}' ({currency})")
                    except DoesNotExist:
                        # Создаем новый проект, если не существует
                        ProjectModel.create(
                            id=uuid.uuid4(),
                            status=project["status"],
                            project_name=project["project_name"],
                            currency=currency,
                            description=None
                        )
                        created += 1
                        logger.debug(f"Created project '{project['project_name']}' ({currency}) with status '{project['status']}'")
        logger.info(f"Processed {len(projects) * len(currencies)} projects: {created} created, {updated} updated")
    except DatabaseError as e:
        logger.error(f"Failed to initialize projects: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during projects initialization: {str(e)}")
        raise
    finally:
        db.close()

if __name__ == "__main__":
    initialize_projects()