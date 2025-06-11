import requests
import pandas as pd
from io import StringIO
import os
from datetime import datetime, timedelta
from typing import Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
API_KEY = os.getenv("KMA_API_KEY", apikey)  # Default key for testing
BASE_URL = "https://apihub.kma.go.kr/api/typ01/url/typ_now.php"

# Column definitions
COLUMNS = [
    "FT", "YY", "TYP", "SEQ", "TMD", "TYP_TM", "FT_TM",
    "LAT", "LON", "DIR", "SP", "PS", "WS", "RAD15", "RAD25",
    "RAD", "ED15", "ER15", "LOC", "ED25", "ER25R"
]


FINAL_COLUMNS = [
    "YY", "TYP", "SEQ", "FT", "TYP_TM",
    "LAT", "LON", "DIR",
    "SP", "PS", "WS",
    "RAD15", "RAD25"
]

def fetch_typhoon_data(tm: str, mode: int = 1, disp: int = 0, help_: int = 1) -> pd.DataFrame:
    """
    Fetch typhoon data from KMA API for a specific timestamp.

    Args:
        tm (str): Timestamp in format 'YYYYMMDDHHMM'
        mode (int): API mode parameter
        disp (int): Display parameter
        help_ (int): Help parameter

    Returns:
        pd.DataFrame: Processed typhoon data
    """
    params = {
        "tm": tm,
        "mode": mode,
        "disp": disp,
        "help": help_,
        "authKey": API_KEY
    }

    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()

        logger.info(f"[{tm}] API 호출 성공 ✅")
        data_text = response.text

        # Remove comments and empty lines
        lines = data_text.splitlines()
        data_lines = [line for line in lines if not line.startswith("#") and line.strip() != '']

        if not data_lines:
            logger.info(f"[{tm}] 조회된 태풍 데이터 없음 ✅")
            return pd.DataFrame(columns=COLUMNS)

        cleaned_text = "\n".join(data_lines)
        df = pd.read_fwf(StringIO(cleaned_text), names=COLUMNS)
        return df

    except requests.exceptions.RequestException as e:
        logger.error(f"API 호출 실패 ❌ : {str(e)}")
        raise

def process_typhoon_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process and clean typhoon data.

    Args:
        df (pd.DataFrame): Raw typhoon data

    Returns:
        pd.DataFrame: Processed and cleaned data
    """
    if df.empty:
        return df

    # Replace missing values
    missing_values = ['-9', '-99', '-999', '-9999', -9, -99, -999, -9999]
    df = df.replace(missing_values, pd.NA)

    # Parse datetime
    df['TYP_TM'] = pd.to_datetime(df['TYP_TM'], format='%Y%m%d%H%M')

    # Convert numeric columns
    num_cols = ['LAT', 'LON', 'SP', 'PS', 'WS', 'RAD15', 'RAD25']
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    return df[FINAL_COLUMNS]

def get_historical_dates(days_back: int = 3) -> list:
    """
    Generate a list of dates for historical data collection.

    Args:
        days_back (int): Number of days to look back

    Returns:
        list: List of dates in YYYYMMDDHHMM format
    """
    dates = []
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)

    current_date = start_date
    while current_date <= end_date:
        # Generate timestamps for every 6 hours
        for hour in [0, 6, 12, 18]:
            date_str = current_date.replace(hour=hour, minute=0).strftime('%Y%m%d%H%M')
            dates.append(date_str)
        current_date += timedelta(days=1)

    return dates