import os
import pandas as pd
import polars as pl
import uuid
from database.models import ProjectModel, SalesModel
from config.logger import setup_logger
from config.settings import settings
from utils.time_utils import parse_date_dynamic
from utils.files_utils import get_all_files, read_excel_file_polars
from utils.df_utils import detect_date_columns, clean_negative_values
from utils.db_utils import check_tables_exist, bulk_insert, get_record_id
from typing import List, Union
from database.db import db

logger = setup_logger(__name__)

FOLDER_NAME = "data/initial/sales"
SHEET_NAME = "Sheet1"
BATCH_SIZE = 500
MIN_VALID_YEAR = 2000

# Mapping Excel columns to model fields
FIELD_MAPPING = {
    "project_name": "project_name",
    "currency": "currency",
    "segment": "segment",
    "data_fields": "data_fields",
}

# Fields for project ID lookup
ID_FIELDS = ["project_name", "currency"]

# Define fields and their types for casting
FIELD_TYPES = {
    "project_name": ("String", False),
    "currency": ("String", False),
    "segment": ("String", False),
    "data_fields": ("String", False),
}

# Aggregation and validation fields
AGGREGATION_FIELDS = ["project_id", "segment", "date_of_month_begin", "data_fields"]

# Fields for data metrics (matches SalesModel fields)
DATA_FIELDS = ["total_gs", "total_ewc", "total_gm"]

NEGATIVE_CLEAN_COLUMNS = ["value"]

def cast_column_types(df: Union[pd.DataFrame, pl.DataFrame]) -> Union[pd.DataFrame, pl.DataFrame]:
    try:
        for model_field, (type_name, _) in FIELD_TYPES.items():
            excel_field = next((k for k, v in FIELD_MAPPING.items() if v == model_field), None)
            if excel_field and excel_field in df.columns:
                if isinstance(df, pd.DataFrame):
                    df[excel_field] = df[excel_field].astype(str)
                else:
                    df = df.with_columns(pl.col(excel_field).cast(pl.Utf8))
        return df
    except Exception as e:
        logger.error(f"Failed to cast column types: {e}")
        raise

def aggregate_sales_data(df: pl.DataFrame, date_columns: List[str]) -> pl.DataFrame:
    try:
        required_cols = list(FIELD_MAPPING.keys())
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.error(f"Missing required columns: {missing_cols}")
            raise ValueError(f"Missing required columns: {missing_cols}")

        # Filter out rows with null or empty required columns
        filter_conditions = [
            (~pl.col(col).is_null()) & (pl.col(col).cast(pl.Utf8) != "") & (pl.col(col) != "None")
            for col in required_cols
        ]
        df = df.filter(pl.all_horizontal(*filter_conditions))
        logger.info(f"Filtered to {len(df)} valid rows")

        if ID_FIELDS:
            project_conditions = [col for col in ID_FIELDS if col in df.columns]
            if not project_conditions:
                logger.error(f"None of project ID fields {ID_FIELDS} found in DataFrame")
                raise ValueError(f"Missing project ID fields: {ID_FIELDS}")

            def fetch_project_id(row: dict) -> str:
                conditions = {field: row[field] for field in project_conditions}
                # Skip rows with empty or null conditions
                if not all(v and v != "None" for v in conditions.values()):
                    logger.warning(f"Skipping row with invalid project conditions: {conditions}")
                    return None
                try:
                    project_id = get_record_id(ProjectModel, conditions)
                    logger.debug(f"Fetched project_id: {project_id} for conditions: {conditions}")
                    return project_id
                except ValueError:
                    with db.connection_context():
                        project = ProjectModel.create(**conditions)
                        logger.info(f"Created new project: {conditions} with id: {project.id}")
                        return str(project.id)

            df = df.with_columns(
                pl.struct(project_conditions).map_elements(
                    fetch_project_id,
                    return_dtype=pl.Utf8
                ).alias("project_id")
            )
            # Filter out rows with null project_id
            initial_len = len(df)
            df = df.filter(pl.col("project_id").is_not_null())
            logger.info(f"Filtered out {initial_len - len(df)} rows with null project_id")
        else:
            logger.warning("No ID_FIELDS defined; skipping project ID mapping")

        parameter_field = next((k for k, v in FIELD_MAPPING.items() if v == "data_fields"), None)
        if parameter_field:
            df = df.with_columns(
                pl.col(parameter_field).str.to_lowercase().alias(parameter_field)
            )

        id_vars = [col for col in AGGREGATION_FIELDS if col != "date_of_month_begin" and col in df.columns]
        if not id_vars:
            logger.error(f"No valid index columns found for unpivot operation")
            raise ValueError(f"Invalid AGGREGATION_FIELDS configuration")

        df_long = df.unpivot(
            index=id_vars,
            on=date_columns,
            variable_name="date_of_month_begin",
            value_name="value"
        )

        df_long = df_long.with_columns(
            pl.col("date_of_month_begin").map_elements(parse_date_dynamic, return_dtype=pl.Date)
        ).filter(
            pl.col("date_of_month_begin").is_not_null() &
            (pl.col("date_of_month_begin").dt.year() >= MIN_VALID_YEAR)
        )

        df_long = df_long.with_columns(
            pl.col("value").cast(pl.Float64, strict=False).fill_null(0.0)
        )

        df_long = df_long.group_by(AGGREGATION_FIELDS).agg(
            pl.col("value").sum()
        )

        logger.info(f"Aggregated data to {len(df_long)} rows")
        logger.debug(f"Aggregated DataFrame:\n{df_long.head().to_pandas().to_string()}")
        return df_long
    except Exception as e:
        logger.error(f"Failed to aggregate sales data: {e}")
        raise

def transform_to_model_format(df: pl.DataFrame) -> pl.DataFrame:
    try:
        if df.is_empty():
            logger.warning("Empty DataFrame provided for transformation")
            return df

        required_columns = AGGREGATION_FIELDS
        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            logger.error(f"Missing required columns: {missing}")
            raise ValueError(f"Missing required columns: {missing}")

        parameter_field = next((k for k, v in FIELD_MAPPING.items() if v == "data_fields"), None)
        if not parameter_field:
            logger.error("No parameter field defined in FIELD_MAPPING")
            raise ValueError("Missing parameter field in FIELD_MAPPING")

        model_df = df.pivot(
            values="value",
            index=[col for col in AGGREGATION_FIELDS if col != parameter_field],
            on=parameter_field,
            aggregate_function="sum",
            maintain_order=True
        )

        for field in DATA_FIELDS:
            if field not in model_df.columns:
                model_df = model_df.with_columns(pl.lit(0.0).alias(field))
            model_df = model_df.with_columns(
                pl.col(field).fill_null(0.0).fill_nan(0.0).alias(field)
            )

        model_df = model_df.with_columns(
            pl.Series([str(uuid.uuid4()) for _ in range(len(model_df))]).alias("id")
        ).select([
            "id",
            pl.col("project_id").alias("project"),  # Rename to match SalesModel field
            *[col for col in AGGREGATION_FIELDS if col != parameter_field and col != "project_id"],
            *DATA_FIELDS
        ])

        group_by_fields = ["project", "segment", "date_of_month_begin"]
        model_df = model_df.group_by(group_by_fields).agg([
            pl.col("id").first(),
            *[pl.col(field).sum().fill_null(0.0).fill_nan(0.0).alias(field) for field in DATA_FIELDS]
        ])

        logger.info(f"Transformed DataFrame to model format with {len(model_df)} rows")
        logger.debug(f"Final DataFrame:\n{model_df.head().to_pandas().to_string()}")
        return model_df
    except Exception as e:
        logger.error(f"Failed to transform DataFrame to model format: {e}")
        raise

def main_job(excel_file: str) -> pl.DataFrame:
    if not os.path.exists(excel_file):
        logger.error(f"File '{excel_file}' not found")
        return pl.DataFrame()

    try:
        check_tables_exist([SalesModel._meta.table_name, ProjectModel._meta.table_name])

        df = read_excel_file_polars(excel_file, SHEET_NAME)
        logger.debug(f"Raw DataFrame:\n{df.head().to_pandas().to_string()}")

        date_columns = detect_date_columns(df)
        if not date_columns:
            logger.error("No valid date columns detected in the Excel file")
            raise ValueError("No valid date columns detected")

        df = cast_column_types(df)
        df = clean_negative_values(df, date_columns)
        logger.debug(f"After clean_negative_values:\n{df.head().to_pandas().to_string()}")

        df = aggregate_sales_data(df, date_columns)
        df = clean_negative_values(df, NEGATIVE_CLEAN_COLUMNS)
        logger.debug(f"After aggregate_sales_data:\n{df.head().to_pandas().to_string()}")

        df = transform_to_model_format(df)
        logger.debug(f"After transform_to_model_format:\n{df.to_pandas().to_string()}")

        if not df.is_empty():
            bulk_insert(SalesModel, df, batch_size=BATCH_SIZE)
            logger.info(f"Inserted/updated {len(df)} rows into SalesModel table")
        else:
            logger.warning("No data to insert into database")

        logger.info(f"Processed {len(df)} rows after transformation")
        return df
    except Exception as e:
        logger.error(f"Failed to process {excel_file}: {e}")
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
            df = main_job(file)
            print(f"Final DataFrame:\n{df.to_pandas().to_string()}")
    except Exception as e:
        logger.error(f"Sales import process failed: {e}")
        raise

if __name__ == "__main__":
    import_sales()