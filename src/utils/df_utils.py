import pandas as pd
import polars as pl

from dateutil.parser import parse
from typing import Union, List

from config.logger import setup_logger
logger = setup_logger(__name__)


def detect_date_columns(df: Union[pd.DataFrame, pl.DataFrame]) -> List[str]:
    logger.debug(f"Input columns: {[(col, str(df[col].dtype)) for col in df.columns]}")
    logger.debug(f"First 2 rows: {df.head(2).to_dict('records') if isinstance(df, pd.DataFrame) else df.head(2).to_dicts()}")
    date_columns = []
    
    for col in df.columns:
        try:
            date = parse(col, fuzzy=False)
            date_columns.append(col)
            logger.debug(f"Detected date column: {col} -> {date}")
        except ValueError:
            continue
            
    if not date_columns:
        logger.warning("No valid date columns detected in DataFrame")
    
    logger.info(f"Detected {len(date_columns)} date columns: {date_columns}")
    return date_columns

def clean_negative_values(df: Union[pd.DataFrame, pl.DataFrame], columns: List[str]) -> Union[pd.DataFrame, pl.DataFrame]:
    try:
        for column in columns:
            if column in df.columns:
                if isinstance(df, pd.DataFrame):
                    df[column] = df[column].apply(lambda x: 0 if x < 0 else x)
                else:  # pl.DataFrame
                    df = df.with_columns(pl.col(column).map_elements(lambda x: 0 if x < 0 else x, return_dtype=df[column].dtype))
        logger.info("Negative values cleaned successfully")
        return df
    except Exception as e:
        logger.error(f"Failed to clean negative values: {str(e)}")
        raise

