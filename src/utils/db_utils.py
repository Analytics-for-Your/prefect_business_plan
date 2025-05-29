from typing import List, Union, Type, Dict, Optional
from config.logger import setup_logger
from peewee import Database, Model, UUIDField, ForeignKeyField, SQL
import pandas as pd
import polars as pl
from config.settings import settings
from database.db import db
import uuid

logger = setup_logger(__name__)

def check_tables_exist(tables: List[str]) -> None:
    try:
        schema = getattr(settings, 'psql_schema', 'public')
        with db.connection_context():
            for table in tables:
                if not db.table_exists(table, schema=schema):
                    logger.error(f"Table '{table}' does not exist in schema '{schema}'")
                    raise ValueError(f"Table '{table}' does not exist")
                db.execute_sql(f"SELECT 1 FROM {schema}.{table} LIMIT 1")

            cursor = db.execute_sql("SHOW search_path")
            current_schema = cursor.fetchone()[0]
            logger.debug(f"Current search_path after checks: {current_schema}")

        logger.info(f"Tables {tables} exist and are accessible in schema '{schema}'")
    except Exception as e:
        logger.error(f"Failed to check table existence or accessibility: {str(e)}")
        raise

def get_record_id(model: Type[Model], conditions: Dict[str, any]) -> str:
    try:
        schema = getattr(settings, 'psql_schema', 'public')
        
        if not conditions or not all(k in model._meta.fields and conditions[k] is not None for k in conditions):
            invalid_fields = [k for k in conditions if k not in model._meta.fields or conditions[k] is None]
            logger.error(f"Invalid or missing conditions: {invalid_fields}")
            raise ValueError(f"Invalid or missing conditions: {invalid_fields}")

        query_conditions = [getattr(model, field) == value for field, value in conditions.items()]
        with db.connection_context():
            cursor = db.execute_sql("SHOW search_path")
            current_schema = cursor.fetchone()[0]
            logger.debug(f"Current search_path: {current_schema}")

            query = model.select(model.id).where(*query_conditions).limit(1)
            record = query.get_or_none()

            if not record:
                logger.error(f"Record not found in {schema}.{model._meta.table_name} with conditions: {conditions}")
                raise ValueError(f"Record not found with conditions: {conditions}")

            record_id = str(record.id)
            logger.debug(f"Found record_id: {record_id} for conditions: {conditions}")
            return record_id
    except Exception as e:
        logger.error(f"Failed to get record from {model._meta.table_name} with conditions: {conditions}: {str(e)}")
        raise

def bulk_insert(model: Type[Model], data: Union[pd.DataFrame, pl.DataFrame], batch_size: int = 500, update_fields: Optional[List[str]] = None) -> None:
    if isinstance(data, pd.DataFrame) and data.empty or isinstance(data, pl.DataFrame) and data.is_empty():
        logger.warning("DataFrame empty, no data to import")
        return

    try:
        schema = getattr(settings, 'psql_schema', 'public')
        skipped_rows = 0
        processed_rows = 0

        # Define required fields, excluding ForeignKeyField
        required_fields = [field.name for field in model._meta.fields.values() if not field.null and not field.default and not isinstance(field, (UUIDField, ForeignKeyField))]
        model_fields = set(model._meta.fields.keys())
        logger.debug(f"Model fields: {model_fields}, Required fields: {required_fields}")

        # Determine unique index fields
        unique_fields = []
        for index in model._meta.indexes:
            if index[1]:  # Check if index is unique
                unique_fields = [field if isinstance(field, str) else field.name for field in index[0]]
                break
        if not unique_fields and hasattr(model._meta, 'constraints'):
            for constraint in model._meta.constraints:
                if 'UNIQUE' in str(constraint).upper():
                    constraint_str = str(constraint).lower()
                    fields = [f.strip() for f in constraint_str.split('(')[1].split(')')[0].split(',')]
                    unique_fields = [f for f in fields if f in model_fields]
                    break
        if not unique_fields:
            logger.error("No unique index or constraint found for model")
            raise ValueError("No unique index or constraint defined for model")

        logger.debug(f"Using unique fields for upsert: {unique_fields}")

        # Define update fields, excluding ForeignKeyField
        if update_fields:
            update_fields = [field for field in update_fields if field in model_fields and field not in unique_fields + ['id'] and not isinstance(model._meta.fields[field], ForeignKeyField)]
            if not update_fields:
                logger.error("No valid update fields provided")
                raise ValueError("No valid update fields provided")
        else:
            update_fields = [field for field in model_fields if field not in unique_fields + ['id'] and not isinstance(model._meta.fields[field], ForeignKeyField)]
        logger.debug(f"Update fields: {update_fields}")

        # Convert data to list of dictionaries
        data_list = data.to_dict('records') if isinstance(data, pd.DataFrame) else data.to_dicts()
        valid_data = []
        for row in data_list:
            missing_fields = [field for field in required_fields if field not in row or row[field] is None]
            if missing_fields:
                logger.warning(f"Skipping row with missing required fields {missing_fields}: {row}")
                skipped_rows += 1
                continue
            if 'project' not in row or row['project'] is None:
                logger.warning(f"Skipping row with null project: {row}")
                skipped_rows += 1
                continue
            valid_data.append({field: row[field] for field in row if field in model_fields})

        if not valid_data:
            logger.warning("No valid rows to insert after validation")
            return

        with db.connection_context():
            for i in range(0, len(valid_data), batch_size):
                batch = valid_data[i:i + batch_size]
                query = model.insert_many(batch).on_conflict(
                    conflict_target=unique_fields,
                    update={field: SQL(f'EXCLUDED.{field}') for field in update_fields}
                )
                result = query.execute()
                processed_rows += len(batch)
                logger.debug(f"Processed {len(batch)} records in batch (inserted or updated)")

        logger.info(f"Skipped {skipped_rows} rows due to errors or missing fields")
        logger.info(f"Processed {processed_rows} records (inserted or updated)")
        logger.info("Note: Exact insert/update counts may vary due to upsert operation")
    except Exception as e:
        logger.error(f"Failed to insert/update records: {str(e)}")
        raise