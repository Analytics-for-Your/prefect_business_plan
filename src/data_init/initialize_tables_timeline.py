import uuid
from datetime import date
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
from utils.time_utils import get_next_month, get_previous_month

# Инициализация логгера
logger = setup_logger(__name__)

def generate_monthly_timeline(start_year, end_year):
    """Генерирует список дат начала месяцев от start_year до end_year."""
    timeline = []
    current_date = date(start_year, 1, 1)
    end_date = date(end_year + 1, 1, 1)
    while current_date < end_date:
        timeline.append(current_date)
        current_date = get_next_month(current_date)
    return timeline

def set_initial_values(initial_date: date):
    """Задает начальные нулевые значения для указанной даты."""
    try:
        db.connect()
        schema = settings.psql_schema or "public"
        db.execute_sql(f"SET search_path TO {schema}")
        logger.info(f"Set search_path to schema '{schema}'")

        projects = ProjectModel.select()
        if not projects:
            raise ValueError("No projects found in the database")

        segments = ["B2B", "B2C"]
        records = {
            "sales": [],
            "orders": [],
            "logistics": [],
            "orders_payments": [],
            "payments": [],
            "stock_budget": []
        }

        for project in projects:
            # SalesModel (для каждого сегмента)
            for segment in segments:
                records["sales"].append({
                    "id": str(uuid.uuid4()),
                    "project_id": project.id,
                    "segment": segment,
                    "date_of_month_begin": initial_date,
                    "total_gs": 0.0,
                    "total_ewc": 0.0,
                    "total_gm": 0.0
                })

            # OrdersModel
            records["orders"].append({
                "id": str(uuid.uuid4()),
                "project_id": project.id,
                "order_date": initial_date,
                "production_time": 0,
                "enroute_time": 0,
                "order_cost": 0.0
            })

            # LogisticsPaymentsTermsModel
            records["logistics"].append({
                "id": str(uuid.uuid4()),
                "project_id": project.id,
                "date_of_month_begin": initial_date,
                "delivery_duty_coef": 0.0
            })

            # OrdersPaymentsTermsModel
            records["orders_payments"].append({
                "id": str(uuid.uuid4()),
                "project_id": project.id,
                "date_of_month_begin": initial_date,
                "order_placement_date_payment_prc": 0.0,
                "order_shipment_date_payment_prc": 0.0
            })

            # PaymentsModel
            records["payments"].append({
                "id": str(uuid.uuid4()),
                "project_id": project.id,
                "date_of_month_begin": initial_date,
                "order_payment_total": 0.0,
                "delivery_duty_payment_total": 0.0
            })

            # StockBudgetModel
            records["stock_budget"].append({
                "id": str(uuid.uuid4()),
                "project_id": project.id,
                "date_of_month_begin": initial_date,
                "oh_ewc_begin": 0.0,
                "oh_ewc_incoming": 0.0,
                "oh_ewc_outgoing": 0.0,
                "oh_ewc_end": 0.0
            })

        with db.atomic():
            # SalesModel
            SalesModel.insert_many(records["sales"]).on_conflict(
                conflict_target=["project_id", "date_of_month_begin", "segment"],
                update={
                    "total_gs": SalesModel.total_gs,
                    "total_ewc": SalesModel.total_ewc,
                    "total_gm": SalesModel.total_gm
                }
            ).execute()

            # OrdersModel
            OrdersModel.insert_many(records["orders"]).on_conflict(
                conflict_target=["project_id", "order_date"],
                update={
                    "production_time": OrdersModel.production_time,
                    "enroute_time": OrdersModel.enroute_time,
                    "order_cost": OrdersModel.order_cost
                }
            ).execute()

            # LogisticsPaymentsTermsModel
            LogisticsPaymentsTermsModel.insert_many(records["logistics"]).on_conflict(
                conflict_target=["project_id", "date_of_month_begin"],
                update={
                    "delivery_duty_coef": LogisticsPaymentsTermsModel.delivery_duty_coef
                }
            ).execute()

            # OrdersPaymentsTermsModel
            OrdersPaymentsTermsModel.insert_many(records["orders_payments"]).on_conflict(
                conflict_target=["project_id", "date_of_month_begin"],
                update={
                    "order_placement_date_payment_prc": OrdersPaymentsTermsModel.order_placement_date_payment_prc,
                    "order_shipment_date_payment_prc": OrdersPaymentsTermsModel.order_shipment_date_payment_prc
                }
            ).execute()

            # PaymentsModel
            PaymentsModel.insert_many(records["payments"]).on_conflict(
                conflict_target=["project_id", "date_of_month_begin"],
                update={
                    "order_payment_total": PaymentsModel.order_payment_total,
                    "delivery_duty_payment_total": PaymentsModel.delivery_duty_payment_total
                }
            ).execute()

            # StockBudgetModel
            StockBudgetModel.insert_many(records["stock_budget"]).on_conflict(
                conflict_target=["project_id", "date_of_month_begin"],
                update={
                    "oh_ewc_begin": StockBudgetModel.oh_ewc_begin,
                    "oh_ewc_incoming": StockBudgetModel.oh_ewc_incoming,
                    "oh_ewc_outgoing": StockBudgetModel.oh_ewc_outgoing,
                    "oh_ewc_end": StockBudgetModel.oh_ewc_end
                }
            ).execute()

        logger.info(f"Initial values set for {initial_date} across all tables with zero values")
        db.close()
    except DatabaseError as e:
        logger.error(f"Failed to set initial values: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during initial values setup: {str(e)}")
        raise

def initialize_tables_timeline(start_year=2024, end_year=2027):
    """Инициализирует таблицы временными рядами и начальными значениями."""
    logger.info("Starting tables timeline initialization")
    try:
        # Set initial date to one month before start_year
        initial_date = get_previous_month(date(start_year, 1, 1))
        set_initial_values(initial_date=initial_date)
        init_timeline_data(start_year=start_year, end_year=end_year)
        logger.info("Tables timeline initialization completed successfully")
    except Exception as e:
        logger.error(f"Tables timeline initialization failed: {str(e)}")
        raise

def init_timeline_data(start_year=2024, end_year=2027):
    """Заполняет все таблицы временными рядами с нулевыми значениями."""
    try:
        db.connect()
        schema = settings.psql_schema or "public"
        db.execute_sql(f"SET search_path TO {schema}")
        logger.info(f"Set search_path to schema '{schema}'")

        # Получаем проекты
        projects = ProjectModel.select()
        if not projects:
            raise ValueError("No projects found in the database")

        # Генерация временного ряда
        timeline = generate_monthly_timeline(start_year, end_year)
        segments = ["B2B", "B2C"]

        records = {
            "sales": [],
            "orders": [],
            "logistics": [],
            "orders_payments": [],
            "payments": [],
            "stock_budget": []
        }

        for project in projects:
            for month in timeline:
                # SalesModel (для каждого сегмента)
                for segment in segments:
                    records["sales"].append({
                        "id": str(uuid.uuid4()),
                        "project_id": project.id,
                        "segment": segment,
                        "date_of_month_begin": month,
                        "total_gs": 0.0,
                        "total_ewc": 0.0,
                        "total_gm": 0.0
                    })

                # OrdersModel
                records["orders"].append({
                    "id": str(uuid.uuid4()),
                    "project_id": project.id,
                    "order_date": month,
                    "production_time": 0,
                    "enroute_time": 0,
                    "order_cost": 0.0
                })

                # LogisticsPaymentsTermsModel
                records["logistics"].append({
                    "id": str(uuid.uuid4()),
                    "project_id": project.id,
                    "date_of_month_begin": month,
                    "delivery_duty_coef": 0.0
                })

                # OrdersPaymentsTermsModel
                records["orders_payments"].append({
                    "id": str(uuid.uuid4()),
                    "project_id": project.id,
                    "date_of_month_begin": month,
                    "order_placement_date_payment_prc": 0.0,
                    "order_shipment_date_payment_prc": 0.0
                })

                # PaymentsModel
                records["payments"].append({
                    "id": str(uuid.uuid4()),
                    "project_id": project.id,
                    "date_of_month_begin": month,
                    "order_payment_total": 0.0,
                    "delivery_duty_payment_total": 0.0
                })

                # StockBudgetModel
                records["stock_budget"].append({
                    "id": str(uuid.uuid4()),
                    "project_id": project.id,
                    "date_of_month_begin": month,
                    "oh_ewc_begin": 0.0,
                    "oh_ewc_incoming": 0.0,
                    "oh_ewc_outgoing": 0.0,
                    "oh_ewc_end": 0.0
                })

        with db.atomic():
            # SalesModel
            SalesModel.insert_many(records["sales"]).on_conflict(
                conflict_target=["project_id", "date_of_month_begin", "segment"],
                update={
                    "total_gs": SalesModel.total_gs,
                    "total_ewc": SalesModel.total_ewc,
                    "total_gm": SalesModel.total_gm
                }
            ).execute()

            # OrdersModel
            OrdersModel.insert_many(records["orders"]).on_conflict(
                conflict_target=["project_id", "order_date"],
                update={
                    "production_time": OrdersModel.production_time,
                    "enroute_time": OrdersModel.enroute_time,
                    "order_cost": OrdersModel.order_cost
                }
            ).execute()

            # LogisticsPaymentsTermsModel
            LogisticsPaymentsTermsModel.insert_many(records["logistics"]).on_conflict(
                conflict_target=["project_id", "date_of_month_begin"],
                update={
                    "delivery_duty_coef": LogisticsPaymentsTermsModel.delivery_duty_coef
                }
            ).execute()

            # OrdersPaymentsTermsModel
            OrdersPaymentsTermsModel.insert_many(records["orders_payments"]).on_conflict(
                conflict_target=["project_id", "date_of_month_begin"],
                update={
                    "order_placement_date_payment_prc": OrdersPaymentsTermsModel.order_placement_date_payment_prc,
                    "order_shipment_date_payment_prc": OrdersPaymentsTermsModel.order_shipment_date_payment_prc
                }
            ).execute()

            # PaymentsModel
            PaymentsModel.insert_many(records["payments"]).on_conflict(
                conflict_target=["project_id", "date_of_month_begin"],
                update={
                    "order_payment_total": PaymentsModel.order_payment_total,
                    "delivery_duty_payment_total": PaymentsModel.delivery_duty_payment_total
                }
            ).execute()

            # StockBudgetModel
            StockBudgetModel.insert_many(records["stock_budget"]).on_conflict(
                conflict_target=["project_id", "date_of_month_begin"],
                update={
                    "oh_ewc_begin": StockBudgetModel.oh_ewc_begin,
                    "oh_ewc_incoming": StockBudgetModel.oh_ewc_incoming,
                    "oh_ewc_outgoing": StockBudgetModel.oh_ewc_outgoing,
                    "oh_ewc_end": StockBudgetModel.oh_ewc_end
                }
            ).execute()

        logger.info(f"Timeline data initialized for {start_year}-{end_year} across all tables with zero values")
        db.close()
    except DatabaseError as e:
        logger.error(f"Failed to initialize timeline data: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during timeline initialization: {str(e)}")
        raise

if __name__ == "__main__":
    initialize_tables_timeline()