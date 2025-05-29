import os
import pandas as pd
import uuid
from dateutil.parser import parse as parse_date
from database.db import db
from database.models import SalesModel, ProjectModel
from config.logger import setup_logger
from config.settings import settings
from utils.time_utils import parse_date_dynamic
from utils.files_utils import get_all_files, read_excel_file_pandas, read_excel_file_polars
logger = setup_logger(__name__)

FOLDER_NAME = "data/initial/sales"
SHEET_NAME = "Sheet1"
BATCH_SIZE = 500
MIN_VALID_YEAR = 2000  # Filter dates before 2000

# Mapping Excel columns to SalesModel fields
FIELD_MAPPING = {
    "project_name": "project_name",
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

# Aggregation and validation fields
AGGREGATION_FIELDS = ["project", "segment", "date_of_month_begin", "parameter"]
SUM_FIELDS = ["total_gs", "total_ewc", "total_gm"]
NEGATIVE_CLEAN_COLUMNS = ["value"]


def detect_date_columns(df: pd.DataFrame) -> list[str]:
    logger.debug(f"Input columns: {[(col, str(df[col].dtype)) for col in df.columns]}")
    logger.debug(f"First 2 rows: {df.head(2).to_dict('records')}")
    date_columns = []
    
    for col in df.columns:
        if col in FIELD_MAPPING.keys():
            continue
        date = parse_date_dynamic(col)
        if date is not None and date.year >= MIN_VALID_YEAR:
            date_columns.append(col)
            logger.debug(f"Detected date column: {col} -> {date}")
        elif date is not None:
            logger.warning(f"Skipping invalid date column: {col} (year {date.year} < {MIN_VALID_YEAR})")
    
    if not date_columns:
        logger.warning("No valid date columns detected in DataFrame")
    
    logger.info(f"Detected {len(date_columns)} date columns: {date_columns}")
    return date_columns

def clean_negative_values(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    try:
        for column in columns:
            if column in df.columns:
                df[column] = df[column].apply(lambda x: 0 if x < 0 else x)
        logger.info("Negative values cleaned successfully")
        return df
    except Exception as e:
        logger.error(f"Failed to clean negative values: {str(e)}")
        raise

def cast_column_types(df: pd.DataFrame) -> pd.DataFrame:
    try:
        for model_field, (type_name, _) in FIELD_TYPES.items():
            excel_field = next((k for k, v in FIELD_MAPPING.items() if v == model_field), None)
            if excel_field and excel_field in df.columns:
                df[excel_field] = df[excel_field].astype(str)
        return df
    except Exception as e:
        logger.error(f"Failed to cast column types: {str(e)}")
        raise

def get_project_id(project_name: str, currency: str) -> str:
    if not project_name or pd.isna(project_name) or project_name == "None":
        logger.error(f"Invalid project name: {project_name}")
        raise ValueError(f"Invalid project name: {project_name}")
    try:
        schema = settings.psql_schema or "public"
        logger.debug(f"Querying project '{project_name}' with currency '{currency}' in schema '{schema}'")
        query = f"""
            SELECT id FROM {schema}.projects
            WHERE project_name = %s AND currency = %s
            LIMIT 1
        """
        with db.connection_context():
            cursor = db.execute_sql("SHOW search_path")
            current_schema = cursor.fetchone()[0]
            logger.debug(f"Current search_path: {current_schema}")
            cursor = db.execute_sql(query, (project_name, currency))
            result = cursor.fetchone()
            if not result:
                logger.error(f"Project '{project_name}' with currency '{currency}' not found in {schema}.projects")
                raise ValueError(f"Project '{project_name}' not found")
            project_id = str(result[0])
            logger.debug(f"Found project_id: {project_id}")
            return project_id
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
            db.execute_sql(f"SELECT 1 FROM {schema}.projects LIMIT 1")
            db.execute_sql(f"SELECT 1 FROM {schema}.sales LIMIT 1")
            cursor = db.execute_sql("SHOW search_path")
            current_schema = cursor.fetchone()[0]
            logger.debug(f"Current search_path after checks: {current_schema}")
        logger.info(f"Tables '{ProjectModel._meta.table_name}' and '{SalesModel._meta.table_name}' exist and are accessible in schema '{schema}'")
    except Exception as e:
        logger.error(f"Failed to check table existence or accessibility: {str(e)}")
        raise

def transform_to_model_format(df: pd.DataFrame) -> pd.DataFrame:
    """Преобразует агрегированный DataFrame в формат, соответствующий SalesModel."""
    try:
        if df.empty:
            logger.warning("Empty DataFrame provided for transformation")
            return df

        # Проверяем наличие необходимых столбцов
        required_columns = ["project", "segment", "date_of_month_begin", "parameter", "value"]
        if not all(col in df.columns for col in required_columns):
            missing = [col for col in required_columns if col not in df.columns]
            logger.error(f"Missing required columns: {missing}")
            raise ValueError(f"Missing required columns: {missing}")

        # Создаем новый DataFrame с полями, соответствующими SalesModel
        model_df = pd.DataFrame({
            "id": [str(uuid.uuid4()) for _ in range(len(df))],
            "project": df["project"],
            "segment": df["segment"],
            "date_of_month_begin": df["date_of_month_begin"],
            "total_gs": df.apply(lambda x: x["value"] if x["parameter"] == "gs" else 0.0, axis=1),
            "total_ewc": df.apply(lambda x: x["value"] if x["parameter"] == "ewc" else 0.0, axis=1),
            "total_gm": df.apply(lambda x: x["value"] if x["parameter"] == "gm" else 0.0, axis=1)
        })

        # Группируем, чтобы объединить параметры (gs, ewc, gm) в одну запись
        model_df = model_df.groupby(["project", "segment", "date_of_month_begin"]).agg({
            "id": "first",  # Берем первый UUID
            "total_gs": "sum",
            "total_ewc": "sum",
            "total_gm": "sum"
        }).reset_index()

        logger.info(f"Transformed DataFrame to model format with {len(model_df)} rows")
        return model_df
    except Exception as e:
        logger.error(f"Failed to transform DataFrame to model format: {str(e)}")
        raise

def aggregate_sales_data(df: pd.DataFrame, date_columns: list[str]) -> pd.DataFrame:
    try:
        records = []
        skipped_rows = 0
        for _, row in df.iterrows():
            project_name = row.get("project_name")
            currency = row.get("currency", "USD")  # Default to USD if missing
            segment = row.get("segment")
            parameter = row.get("parameter")

            # Skip rows with invalid or missing project
            if not project_name or pd.isna(project_name) or project_name == "None":
                logger.warning(f"Skipping row with invalid project: {row.to_dict()}")
                skipped_rows += 1
                continue

            try:
                project_id = get_project_id(project_name, currency)
            except ValueError as e:
                logger.warning(f"Skipping row due to project lookup failure: {row.to_dict()}. Error: {str(e)}")
                skipped_rows += 1
                continue

            if not all([segment, parameter]):
                logger.warning(f"Skipping row with missing segment or parameter: {row.to_dict()}")
                skipped_rows += 1
                continue

            parameter = parameter.split("_")[1].lower() if "_" in parameter else parameter.lower()
            for date_col in date_columns:
                date = parse_date_dynamic(date_col)
                if date is None:
                    continue
                value = float(row.get(date_col, 0.0) or 0.0)
                records.append({
                    "project": project_id,
                    "segment": segment,
                    "date_of_month_begin": date,
                    "parameter": parameter,
                    "value": value
                })

        logger.info(f"Skipped {skipped_rows} rows due to invalid or missing data")
        aggregated_df = pd.DataFrame(records)
        logger.info(f"Transformed {len(aggregated_df)} rows")

        if aggregated_df.empty:
            logger.warning("No valid records after aggregation")
            return aggregated_df

        aggregated_df = aggregated_df.groupby(["project", "segment", "date_of_month_begin", "parameter"])["value"].sum().reset_index()
        logger.info(f"Aggregated data to {len(aggregated_df)} rows")
        return aggregated_df
    except Exception as e:
        logger.error(f"Failed to aggregate sales data: {str(e)}")
        raise

def bulk_insert(data: pd.DataFrame, batch_size: int = BATCH_SIZE):
    if data.empty:
        logger.warning("DataFrame is empty, no data to import")
        return

    try:
        schema = settings.psql_schema or "public"
        skipped_rows = 0
        updated_rows = 0
        inserted_rows = 0

        with db.connection_context():
            for _, row in data.iterrows():
                project_id = row.get("project")
                segment = row.get("segment")
                date_of_month_begin = row.get("date_of_month_begin")
                total_gs = row.get("total_gs")
                total_ewc = row.get("total_ewc")
                total_gm = row.get("total_gm")

                if not all([project_id, segment, date_of_month_begin]):
                    logger.warning(f"Skipping row with missing required fields: {row.to_dict()}")
                    skipped_rows += 1
                    continue

                try:
                    # Find existing record by project_id, segment, and date_of_month_begin
                    existing_record = SalesModel.get_or_none(
                        (SalesModel.project == project_id) &
                        (SalesModel.segment == segment) &
                        (SalesModel.date_of_month_begin == date_of_month_begin)
                    )

                    if existing_record:
                        # Update existing record
                        SalesModel.update(
                            total_gs=total_gs,
                            total_ewc=total_ewc,
                            total_gm=total_gm
                        ).where(SalesModel.id == existing_record.id).execute()
                        logger.debug(f"Updated record ID {existing_record.id} for project_id={project_id}, segment={segment}, date={date_of_month_begin}")
                        updated_rows += 1
                    else:
                        # Insert new record
                        SalesModel.create(
                            id=uuid.uuid4(),
                            project=project_id,
                            segment=segment,
                            date_of_month_begin=date_of_month_begin,
                            total_gs=total_gs,
                            total_ewc=total_ewc,
                            total_gm=total_gm
                        )
                        logger.debug(f"Inserted new record for project_id={project_id}, segment={segment}, date={date_of_month_begin}")
                        inserted_rows += 1

                except Exception as e:
                    logger.error(f"Failed to process row for project_id={project_id}, segment={segment}, date={date_of_month_begin}: {str(e)}")
                    skipped_rows += 1
                    continue

        logger.info(f"Skipped {skipped_rows} rows due to errors or missing fields")
        logger.info(f"Inserted {inserted_rows} new records")
        logger.info(f"Updated {updated_rows} existing records")
    except Exception as e:
        logger.error(f"Failed to insert/update records: {str(e)}")
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

        df = read_excel_file_polars(excel_file, SHEET_NAME)
        # # Чтение данных с Pandas
        # df = pd.read_excel(
        #     excel_file,
        #     sheet_name=SHEET_NAME,
        #     engine="openpyxl",
        #     header=0
        # )
        # # Приведение имен столбцов к строкам
        # df.columns = [str(col) if col is not None else f"Col_{i}" for i, col in enumerate(df.columns)]
        
        # logger.info(f"Read {len(df)} rows from {excel_file}")
        # logger.debug(f"Columns: {df.columns.tolist()}")
        # logger.debug(f"First row: {df.head(1).to_dict('records')[0] if not df.empty else 'Empty'}")

        # df = df.dropna(how='all')
        # logger.info(f"After filtering null rows, {len(df)} rows remain")

        # if df.empty:
        #     logger.error("No non-null rows remain")
        #     raise ValueError("All rows are null")

        date_columns = detect_date_columns(df)
        if not date_columns:
            logger.error("No valid date columns detected in the Excel file")
            raise ValueError("No valid date columns detected")

        df = cast_column_types(df)
        df = aggregate_sales_data(df, date_columns)
        df = clean_negative_values(df, NEGATIVE_CLEAN_COLUMNS)
        logger.debug(f"After clean_negative_values:\n{df.head().to_string()}")
        df = transform_to_model_format(df)
        logger.debug(f"After transform_to_model_format:\n{df.head().to_string()}")

        logger.info(f"Processed {len(df)} rows after transformation")
        bulk_insert(df)

        db.close()
        logger.info(f"Import completed for {excel_file}")
    except Exception as e:
        logger.error(f"Failed to process {excel_file}: {str(e)}")
        db.close()
        raise

def import_sales():
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