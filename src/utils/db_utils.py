from typing import List, Union, Type, Dict, Optional
from config.logger import setup_logger
from peewee import Database, Model, UUIDField
import pandas as pd
import polars as pl
from config.settings import settings
from database.db import db
import uuid

logger = setup_logger(__name__)

def check_tables_exist(tables: List[str]) -> None:
    """
    Проверяет существование и доступность таблиц в указанной схеме базы данных (Peewee).

    Args:
        tables (List[str]): Список имен таблиц для проверки.

    Raises:
        ValueError: Если какая-либо таблица не существует или недоступна.
        Exception: При любых других ошибках доступа к базе данных.
    """
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
    """
    Универсальная функция для поиска ID записи в модели по заданным условиям.

    Args:
        model (Type[Model]): Модель Peewee, в которой выполняется поиск.
        conditions (Dict[str, any]): Словарь условий для поиска (ключ - имя поля, значение - значение поля).

    Returns:
        str: ID найденной записи.

    Raises:
        ValueError: Если условия недействительны или запись не найдена.
        Exception: При ошибках доступа к базе данных.
    """
    try:
        schema = getattr(settings, 'psql_schema', 'public')
        
        # Проверка условий
        if not conditions or not all(k in model._meta.fields and conditions[k] is not None for k in conditions):
            invalid_fields = [k for k in conditions if k not in model._meta.fields or conditions[k] is None]
            logger.error(f"Invalid or missing conditions: {invalid_fields}")
            raise ValueError(f"Invalid or missing conditions: {invalid_fields}")

        # Формируем запрос
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
        logger.error(f"Failed to get record from {model._meta.table_name} with conditions {conditions}: {str(e)}")
        raise

def bulk_insert(model: Type[Model], data: Union[pd.DataFrame, pl.DataFrame], batch_size: int = 500) -> None:
    """
    Универсальная функция для массовой вставки или обновления записей в таблицу базы данных (Peewee).

    Args:
        model (Type[Model]): Модель Peewee, соответствующая таблице.
        data (Union[pd.DataFrame, pl.DataFrame]): Данные для вставки (Pandas или Polars DataFrame).
        batch_size (int): Размер пакета для обработки (по умолчанию 500).

    Raises:
        ValueError: Если DataFrame пустой или отсутствуют обязательные поля.
        Exception: При ошибках вставки/обновления.
    """
    if isinstance(data, pd.DataFrame) and data.empty or isinstance(data, pl.DataFrame) and data.is_empty():
        logger.warning("DataFrame is empty, no data to import")
        return

    try:
        schema = getattr(settings, 'psql_schema', 'public')
        skipped_rows = 0
        updated_rows = 0
        inserted_rows = 0

        # Определяем обязательные поля модели
        required_fields = [field.name for field in model._meta.fields.values() if not field.null and not field.default]
        if isinstance(model._meta.fields.get('id'), UUIDField) and 'id' not in required_fields:
            required_fields.append('id')

        # Определяем поля уникального индекса (если есть)
        unique_fields = []
        for index in model._meta.indexes:
            if index[1]:  # Проверяем, является ли индекс уникальным
                unique_fields = [field for field in index[0] if isinstance(field, str)]
                break
        if not unique_fields:
            logger.warning("No unique index found; updates will not be performed, only inserts")
            unique_fields = []

        # Преобразуем данные в список словарей
        data_list = data.to_dict('records') if isinstance(data, pd.DataFrame) else data.to_dicts()

        # Фильтруем записи с отсутствующими обязательными полями
        valid_data = []
        for row in data_list:
            if all(field in row and row[field] is not None for field in required_fields):
                valid_data.append(row)
            else:
                logger.warning(f"Skipping row with missing required fields: {row}")
                skipped_rows += 1

        if not valid_data:
            logger.warning("No valid rows to insert after validation")
            return

        with db.connection_context():
            # Получаем существующие записи, если есть уникальные поля
            existing_records = {}
            if unique_fields:
                # Извлекаем уникальные комбинации ключей
                keys = {tuple(row.get(field) for field in unique_fields) for row in valid_data if all(field in row for field in unique_fields)}
                if keys:
                    query_conditions = []
                    for i, field in enumerate(unique_fields):
                        field_values = [k[i] for k in keys]
                        query_conditions.append(getattr(model, field).in_(field_values))
                    existing_query = model.select(model.id, *unique_fields).where(*query_conditions)
                    existing_records = {
                        tuple(getattr(rec, field) for field in unique_fields): rec.id
                        for rec in existing_query
                    }

            # Разделяем данные на вставку и обновление
            insert_data = []
            update_data = []
            for row in valid_data:
                row_key = tuple(row.get(field) for field in unique_fields) if unique_fields else None
                if unique_fields and row_key in existing_records:
                    update_data.append({
                        'id': existing_records[row_key],
                        **{field: row[field] for field in row if field in model._meta.fields and field not in unique_fields + ['id']}
                    })
                else:
                    insert_row = {field: row[field] for field in row if field in model._meta.fields}
                    if 'id' not in insert_row and isinstance(model._meta.fields.get('id'), UUIDField):
                        insert_row['id'] = uuid.uuid4()
                    insert_data.append(insert_row)

            # Массовая вставка
            if insert_data:
                for i in range(0, len(insert_data), batch_size):
                    batch = insert_data[i:i + batch_size]
                    model.insert_many(batch).execute()
                    inserted_rows += len(batch)
                    logger.debug(f"Inserted {len(batch)} records in batch")

            # Массовая обновление
            if update_data:
                for i in range(0, len(update_data), batch_size):
                    batch = update_data[i:i + batch_size]
                    for row in batch:
                        model.update(**{k: v for k, v in row.items() if k != 'id'}).where(model.id == row['id']).execute()
                        updated_rows += 1
                    logger.debug(f"Updated {len(batch)} records in batch")

        logger.info(f"Skipped {skipped_rows} rows due to errors or missing fields")
        logger.info(f"Inserted {inserted_rows} new records")
        logger.info(f"Updated {updated_rows} existing records")
    except Exception as e:
        logger.error(f"Failed to insert/update records: {str(e)}")
        raise