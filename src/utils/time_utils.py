# utils/time_utils.py
from datetime import date, timedelta
from dateutil.parser import parse as parse_date
from datetime import datetime, date

import polars as pl
from config.logger import setup_logger

logger = setup_logger(__name__)

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

def get_previous_month(date):
    """
    Возвращает дату начала предыдущего месяца.

    Args:
        date: Дата начала месяца (datetime.date)

    Returns:
        datetime.date: Дата начала предыдущего месяца
    """
    year = date.year
    month = date.month
    if month == 1:
        month = 12
        year -= 1
    else:
        month -= 1
    return date.replace(year=year, month=month, day=1)

def get_current_month(date):
    """
    Возвращает дату начала текущего месяца. Поскольку данные организованы по месяцам,
    возвращает входную дату.

    Args:
        date: Дата начала месяца (datetime.date)

    Returns:
        datetime.date: Дата начала текущего месяца
    """
    return date

def get_next_month(date):
    """
    Возвращает дату начала следующего месяца.

    Args:
        date: Дата начала месяца (datetime.date)

    Returns:
        datetime.date: Дата начала следующего месяца
    """
    year = date.year
    month = date.month
    if month == 12:
        month = 1
        year += 1
    else:
        month += 1
    return date.replace(year=year, month=month, day=1)

def get_sliding_window(data, date):
    """
    Создает Polars DataFrame с данными для скользящего окна (предыдущий, текущий, следующий месяц).

    Args:
        data: Polars DataFrame с колонкой 'date_of_month_begin' (тип pl.Date)
        date: Дата текущего месяца (datetime.date)

    Returns:
        Polars DataFrame: Данные за три месяца (предыдущий, текущий, следующий)
    """
    prev_month = get_previous_month(date)
    curr_month = get_current_month(date)
    next_month = get_next_month(date)

    window_df = data.filter(
        pl.col('date_of_month_begin').is_in([prev_month, curr_month, next_month])
    )

    if window_df.is_empty():
        logger.warning(f"No data found in sliding window for date {date}")
    else:
        logger.debug(f"Sliding window for {date}: {len(window_df)} records for months {prev_month}, {curr_month}, {next_month}")

    return window_df


