import os
import pandas as pd
import polars as pl
import uuid
from datetime import datetime, date
from dateutil.parser import parse as parse_date
from database.db import db
from database.models import SalesModel, ProjectModel
from config.logger import setup_logger
from config.settings import settings

logger = setup_logger(__name__)

FOLDER_NAME = "data/initial/sales"
SHEET_NAME = "Sheet1"
BATCH_SIZE = 500

# Mapping Excel columns to SalesModel fields
FIELD_MAPPING = {
    "project": "project_name",
    "currency": "currency",
    "segment": "segment",
    "parameter": "parameter",
}

# Define fields and their types for casting
FIELD_TYPES = {
    "project_name": ("String", False),
    "currency": ("String", False),
    "segment": ("String", False),
    "parameter": ("String", False),
}

# Common date formats to try if dateutil fails
DATE_FORMATS = [
    "%m/%d/%y",  # MM/DD/YY (e.g., 12/1/45)
    "%m/%d/%Y",  # MM/DD/YYYY (e.g., 12/01/2025)
    "%d-%m-%Y",  # DD-MM-YYYY (e.g., 01-12-1945)
    "%Y-%m-%d",  # YYYY-MM-DD (e.g., 2024-12-01)
    "%m-%d-%Y",  # MM-DD-YYYY (e.g., 12-01-2024)
    "%d/%m/%Y",  # DD/MM/YYYY (e.g., 01/12/2024)
    "%b %d %Y",  # MMM DD YYYY (e.g., Dec 01 2024)
]

# Aggregation and validation fields
AGGREGATION_FIELDS = ["project_id", "segment", "date_of_month_begin"]
SUM_FIELDS = ["total_gs", "total_ewc", "total_gm"]
NEGATIVE_CLEAN_COLUMNS = ["value"]
METADATA_FIELDS = ["project_name", "currency", "segment"]

def get_all_files(folder_path: str) -> list[str]:
    try:
        files = [
            os.path.join(folder_path, f)
            for f in os.listdir(folder_path)
            if os.path.isfile(os.path.join(folder_path, f)) and f.endswith('.xlsx') and not f.startswith('~')
        ]
        logger.info(f"Found {len(files)} Excel files in {folder_path}")
        return files
    except Exception as e:
        logger.error(f"Failed to list files in {folder_path}: {str(e)}")
        raise

def parse_date_dynamic(date_str: str) -> date:
    date_str = str(date_str).strip()
    try:
        parsed = parse_date(date_str, dayfirst=False, fuzzy=False)
        logger.debug(f"Parsed date: {date_str} -> {parsed.date()}")
        return parsed.date()
    except ValueError:
        for fmt in DATE_FORMATS:
            try:
                parsed = datetime.strptime(date_str, fmt)
                logger.debug(f"Parsed date with {fmt}: {date_str} -> {parsed.date()}")
                return parsed.date()
            except ValueError:
                continue
        logger.warning(f"Failed to parse date: {date_str}")
        return None

def detect_date_columns(df: pl.DataFrame) -> list[str]:
    logger.debug(f"Input columns: {[(col, str(df[col].dtype)) for col in df.columns]}")
    logger.debug(f"First 2 rows: {df.head(2).to_dicts()}")
    date_columns = []
    
    for col in df.columns:
        if col in FIELD_MAPPING.keys():
            continue
        date = parse_date_dynamic(col)
        if date is not None:
            date_columns.append(col)
            logger.debug(f"Detected date column: {col} -> {date}")
    
    if not date_columns:
        logger.warning("No date columns detected in DataFrame")
    
    logger.info(f"Detected {len(date_columns)} date columns: {date_columns}")
    return date_columns

def map_excel_to_model_fields(row: dict, project_id: str, date: date, value: float, parameter: str) -> dict:
    return {
        "project_id": project_id,
        "segment": row.get("segment"),
        "date_of_month_begin": date,
        f"total_{parameter.lower()}": value,
    }

def clean_negative_values(df: pl.DataFrame, columns: list[str]) -> pl.DataFrame:
    try:
        for column in columns:
            if column in df.columns:
                df = df.with_columns(
                    pl.when(pl.col(column) < 0)
                    .then(0)
                    .otherwise(pl.col(column))
                    .alias(column)
                )
        logger.info("Negative values cleaned successfully")
        return df
    except Exception as e:
        logger.error(f"Failed to clean negative values: {str(e)}")
        raise

def cast_column_types(df: pl.DataFrame) -> pl.DataFrame:
    try:
        for model_field, (type_name, _) in FIELD_TYPES.items():
            excel_field = next((k for k, v in FIELD_MAPPING.items() if v == model_field), None)
            if excel_field and excel_field in df.columns:
                df = df.with_columns(pl.col(excel_field).cast(pl.Utf8, strict=False))
        return df
    except Exception as e:
        logger.error(f"Failed to cast column types: {str(e)}")
        raise

def get_project_id(project_name: str, currency: str) -> str:
    try:
        project = ProjectModel.get_or_none(
            ProjectModel.project_name == project_name,
            ProjectModel.currency == currency
        )
        if not project:
            logger.error(f"Project '{project_name}' with currency '{currency}' not found")
            raise ValueError(f"Project '{project_name}' not found")
        return str(project.id)
    except Exception as e:
        logger.error(f"Failed to get project {project_name}: {str(e)}")
        raise

def check_tables_exist():
    try:
        schema = settings.psql_schema or "public"
        with db.connection_context():
            project_table_exists = db.table_exists(ProjectModel._meta.table_name, schema=schema)
            sales_table_exists = db.table_exists(SalesModel._meta.table_name, schema=schema)
            if not project_table_exists:
                logger.error(f"Table '{ProjectModel._meta.table_name}' does not exist in schema '{schema}'")
                raise ValueError(f"Table '{ProjectModel._meta.table_name}' does not exist")
            if not sales_table_exists:
                logger.error(f"Table '{SalesModel._meta.table_name}' does not exist in schema '{schema}'")
                raise ValueError(f"Table '{SalesModel._meta.table_name}' does not exist")
        logger.info(f"Tables '{ProjectModel._meta.table_name}' and '{SalesModel._meta.table_name}' exist in schema '{schema}'")
    except Exception as e:
        logger.error(f"Failed to check table existence: {str(e)}")
        raise

def aggregate_sales_data(df: pl.DataFrame, date_columns: list[str]) -> pl.DataFrame:
    try:
        records = []
        for row in df.to_dicts():
            project_name = row.get("project")
            currency = row.get("currency")
            segment = row.get("segment")
            parameter = row.get("parameter", "").split("_")[1].lower()  # Extract gs, ewc, gm
            project_id = get_project_id(project_name, currency)
            
            for date_col in date_columns:
                date = parse_date_dynamic(date_col)
                if date is None:
                    continue
                value = float(row.get(date_col, 0.0) or 0.0)
                records.append({
                    "project_id": project_id,
                    "segment": segment,
                    "date_of_month_begin": date,
                    "parameter": parameter,
                    "value": value
                })

        aggregated_df = pl.DataFrame(records)
        logger.info(f"Transformed {aggregated_df.height} rows")

        # Aggregate by project_id, segment, date_of_month_begin
        agg_exprs = [
            pl.col("value").sum().alias("value")
        ]
        aggregated_df = aggregated_df.group_by(["project_id", "segment", "date_of_month_begin", "parameter"]).agg(agg_exprs)
        logger.info(f"Aggregated data to {aggregated_df.height} rows")

        return aggregated_df
    except Exception as e:
        logger.error(f"Failed to aggregate sales data: {str(e)}")
        raise

def bulk_insert(data: pl.DataFrame, batch_size: int = BATCH_SIZE):
    if data.is_empty():
        logger.warning("DataFrame is empty, no data to import")
        return

    try:
        records = []
        skipped_rows = 0
        seen_keys = set()
        for row in data.to_dicts():
            project_id = row.get("project_id")
            segment = row.get("segment")
            date_of_month_begin = row.get("date_of_month_begin")
            parameter = row.get("parameter")
            value = row.get("value")

            if not all([project_id, segment, date_of_month_begin, parameter]):
                logger.warning(f"Skipping row with missing required fields: {row}")
                skipped_rows += 1
                continue

            key = (project_id, segment, date_of_month_begin, parameter)
            if key in seen_keys:
                logger.warning(f"Duplicate record: {key}")
                continue
            seen_keys.add(key)

            mapped_data = {
                "project_id": project_id,
                "segment": segment,
                "date_of_month_begin": date_of_month_begin,
                f"total_{parameter}": value,
            }
            records.append(mapped_data)

        logger.info(f"Skipped {skipped_rows} rows due to missing required fields")

        if not records:
            logger.error("No valid records to insert")
            raise ValueError("No valid records to insert")

        with db.atomic():
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                SalesModel.insert_many(batch).on_conflict(
                    conflict_target=["project_id", "date_of_month_begin", "segment"],
                    update={
                        "total_gs": SalesModel.total_gs,
                        "total_ewc": SalesModel.total_ewc,
                        "total_gm": SalesModel.total_gm,
                    }
                ).execute()

        logger.info(f"Inserted/updated {len(records)} records")
    except Exception as e:
        logger.error(f"Failed to insert records: {str(e)}")
        raise

def main_job(excel_file: str) -> None:
    if not os.path.exists(excel_file):
        logger.error(f"File '{excel_file}' not found")
        return

    try:
        db.connect()
        schema = settings.psql_schema or "public"
        db.execute_sql(f"SET search_path TO {schema}")
        logger.info(f"Set search_path to schema '{schema}'")

        # Check if tables exist
        check_tables_exist()

        # Чтение заголовков с помощью Pandas
        temp_df = pd.read_excel(
            excel_file,
            sheet_name=SHEET_NAME,
            engine="openpyxl",
            header=0,
            nrows=0  # Читаем только заголовки
        )
        column_names = [str(col) if col is not None else f"Col_{i}" for i, col in enumerate(temp_df.columns)]

        # Определение столбцов, которые могут содержать даты
        date_columns = ["Дата начала недели", "Order Date"]
        schema_overrides = {col: pl.Float64 for col in column_names if col not in FIELD_MAPPING.keys() and col not in date_columns}
        for col in date_columns:
            if col in column_names:
                schema_overrides[col] = pl.Datetime

        # Чтение данных с Polars
        df = pl.read_excel(
            excel_file,
            sheet_name=SHEET_NAME,
            engine="openpyxl",
            has_header=True,
            schema_overrides=schema_overrides
        )
        # Установка исправленных имен столбцов
        if len(df.columns) == len(column_names):
            df = df.rename({old: new for old, new in zip(df.columns, column_names)})
        
        logger.info(f"Read {df.height} rows from {excel_file}")
        logger.debug(f"Columns: {df.columns}")
        logger.debug(f"First row: {df.head(1).to_dicts()[0] if not df.is_empty() else 'Empty'}")

        df = df.filter(~pl.all_horizontal(pl.all().is_null()))
        logger.info(f"After filtering null rows, {df.height} rows remain")

        if df.is_empty():
            logger.error("No non-null rows remain")
            raise ValueError("All rows are null")

        date_columns = detect_date_columns(df)
        if not date_columns:
            logger.error("No valid date columns detected in the Excel file")
            raise ValueError("No valid date columns detected")

        df = cast_column_types(df)
        df = aggregate_sales_data(df, date_columns)
        df = clean_negative_values(df, NEGATIVE_CLEAN_COLUMNS)
        df = df.with_columns([pl.col(col).cast(pl.Utf8, strict=False).replace("null", None) for col in df.columns if col in METADATA_FIELDS])

        logger.info(f"Processed {df.height} rows after aggregation and cleaning")
        bulk_insert(df)

        db.close()
        logger.info(f"Import completed for {excel_file}")
    except Exception as e:
        logger.error(f"Failed to process {excel_file}: {str(e)}")
        db.close()
        raise

def import_sales() -> None:
    try:
        files_to_proceed = get_all_files(FOLDER_NAME)
        logger.info(f"Files to process: {files_to_proceed}")
        if not files_to_proceed:
            logger.info("No Excel files found for processing")
            return
        for file in files_to_proceed:
            logger.info(f"Processing file: {file}")
            main_job(file)
    except Exception as e:
        logger.error(f"Sales import process failed: {str(e)}")
        raise

if __name__ == "__main__":
    import_sales()