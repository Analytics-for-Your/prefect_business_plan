# utils/file_utils.py
import os
import pandas as pd
import polars as pl
import openpyxl
from config.logger import setup_logger

logger = setup_logger(__name__)

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

def read_excel_file_pandas(excel_file: str, sheet_name: str) -> pd.DataFrame:
    try:
        df = pd.read_excel(
            excel_file,
            sheet_name=sheet_name,
            engine="openpyxl",
            header=0
        )
        df.columns = [str(col) if col is not None else f"Col_{i}" for i, col in enumerate(df.columns)]
        logger.info(f"Read {len(df)} rows from {excel_file}")
        logger.debug(f"Columns: {df.columns.tolist()}")
        logger.debug(f"First row: {df.head(1).to_dict('records')[0] if not df.empty else 'Empty'}")

        df = df.dropna(how='all')
        logger.info(f"After filtering null rows, {len(df)} rows remain")

        if df.empty:
            logger.error("No non-null rows remain")
            raise ValueError("All rows are null")
        return df
    except Exception as e:
        logger.error(f"Failed to read Excel file {excel_file}: {str(e)}")
        raise

def read_excel_file_polars(excel_file: str, sheet_name: str) -> pl.DataFrame:
    try:
        wb = openpyxl.load_workbook(excel_file, data_only=True)
        ws = wb[sheet_name]
        data = [list(row) for row in ws.iter_rows(values_only=True)]
        if not data:
            logger.error("No data found in the sheet")
            raise ValueError("Empty sheet")
        
        df = pl.DataFrame(data[1:], schema=[str(col) if col is not None else f"Col_{i}" for i, col in enumerate(data[0])], orient="row")
        logger.info(f"Read {len(df)} rows from {excel_file} using Polars")
        logger.debug(f"Columns: {df.columns}")
        logger.debug(f"First row: {df.head(1).to_dicts()[0] if not df.is_empty() else 'Empty'}")

        df = df.filter(~pl.all_horizontal(pl.col("*").is_null()))
        logger.info(f"After filtering null rows, {len(df)} rows remain")

        if df.is_empty():
            logger.error("No non-null rows remain")
            raise ValueError("All rows are null")
        return df
    except Exception as e:
        logger.error(f"Failed to read Excel file {excel_file} with Polars: {str(e)}")
        raise

if __name__ == '__main__':
    FOLDER_NAME = "data/initial/sales"
    SHEET_NAME = "Sheet1"
    files = get_all_files(FOLDER_NAME)
    df_pandas = read_excel_file_pandas(files[0], SHEET_NAME)
    df_polars = read_excel_file_polars(files[0], SHEET_NAME)
    print("Pandas DataFrame:\n", df_pandas.head())
    print("Polars DataFrame:\n", df_polars.head())