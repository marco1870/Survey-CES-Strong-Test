import pandas as pd
from dateutil.relativedelta import relativedelta
import base64
from datetime import datetime, timedelta
import time
from typing import Literal
import pandas as pd
import hashlib

def dt_to_ms (normal_datetime) -> int:
    
    '''
    The aim of dt_to_ms is to transform a normal timestamp to a timestamp in milliseconds
    '''
    
    dt_obj = datetime.strptime(normal_datetime, "%Y-%m-%d %H:%M:%S")
    dt_ms = int(dt_obj.timestamp()*1000)
    return dt_ms

def ms_to_dt (timestamp_milliseconds: int) -> str:
    
    '''
    The aim of ms_to_dt is to transform a timestamp in milliseconds to a normal timestamp
    '''
        
    seconds = timestamp_milliseconds / 1000
    normal_datetime = datetime.fromtimestamp(seconds)
    return normal_datetime

def get_authentication_basic(username, password) -> str:
    credentials = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("utf-8")
    return credentials

def convert_unix_millis_to_local(ts_millis, result_type: Literal["string","integer"], local_timezone='Europe/Rome', ):
    dt_utc = pd.to_datetime(ts_millis / 1000, unit='s', utc=True)
    dt_local = dt_utc.tz_convert(local_timezone).tz_localize(None)
    dt_local = dt_local.round('s')
    if result_type == "string":
        result_dt_local = str(dt_local)
    else:
        result_dt_local = dt_local
    return result_dt_local

def print_progress(current: int, total: int) -> None:
    if total != 0:
        j = (current + 1) / total
        print("[%-20s] %d%% %d/%d" % ('='*int(20*j), 100*j, current, total), end='\r', flush=True)
    return None

def today_now() -> str:
    """
    Summuray:
        The function returns the current datetime in a strin format (format es. 15-07-2024 15:07:15)
    Arg:
        None
    Return:
        today_now_str (str)
    """

    today_now_tuple = time.localtime()
    today_now_str = time.strftime("%Y-%m-%d %H:%M:%S", today_now_tuple)

    return today_now_str

def take_isoweek(datetime_str: str) -> str:
    """
    Summary:
        The function take as input a timestamp (format string es. 31-07-2024 12:23:23) and retrieve the iso week number
    Arg:
        datetime_str: str
            timestamp (format string es. 31-07-2024 12:23:23)
    Return:
        isoweek: str
    """

    dttm = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
    isocalendar = datetime.isocalendar(dttm)

    index = 0
    for i in isocalendar:
        if index == 1:
            isoweek = str(i)
        index +=1

    return isoweek

def take_month(datetime_str: str, type:Literal["number","letter"]) -> str:
    """
    Summary:
        The function take as input a timestamp (format string es. 31-07-2024 12:23:23) and retrieve the month
    Arg:
        datetime_str: str
            timestamp (format string es. 31-07-2024 12:23:23)
        type: str
            could be "number" (format es. "07") or string (format es. "Jul")
    Return:
        month: str
    """

    dttm = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")

    if type == "number":
        month = dttm.strftime("%m")
    elif type == "letter":
        month = dttm.strftime("%B")

    return month

def take_year(datetime_str: str) -> int:
    """
    Summary:
        The function take as input a timestamp (format string es. 31-07-2024 12:23:23) and retrieve the year
    Arg:
        datetime_str: str
            timestamp (format string es. 31-07-2024 12:23:23)
    Return:
        year: int
    """

    dttm = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
    year = dttm.year

    return year

def take_day(datetime_str: str) -> int:
    """
    Summary:
        The function take as input a timestamp (format string es. 31-07-2024 12:23:23) and retrieve the day
    Arg:
        datetime_str: str
            timestamp (format string es. 31-07-2024 12:23:23)
    Return:
        day: int
    """

    dttm = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
    day = dttm.day

    return day

def take_dt(datetime_str: str) -> str:
    """
    Summary:
        The function take as input a timestamp (format string es. 31-07-2024 12:23:23) and retrieve the dt
    Arg:
        datetime_str: str
            timestamp (format string es. 31-07-2024 12:23:23)
    Return:
        dt: int
    """

    dttm = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
    dt = dttm.strftime("%d/%m/%Y")

    return dt

def take_start_dt_week(datetime_str: str) -> str:
    """
    Summary:
        The function take as input a timestamp (format string es. 31-07-2024 12:23:23) and retrieve the dt
    Arg:
        datetime_str: str
            timestamp (format string es. 31-07-2024 12:23:23)
    Return:
        dt: int
    """

    dttm = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
    start_dt_week = dttm - timedelta(days=dttm.weekday())
    start_dt_week_formatted = start_dt_week.strftime("%d/%m/%Y")

    return start_dt_week_formatted

def add_1_second(datetime_str: str) -> str:
    """
    Summary:
        The function take as input a timestamp (format string es. 31-07-2024 12:23:23) and add 1 second
    Arg:
        datetime_str: str
            timestamp (format string es. 31-07-2024 12:23:23)
    Return:
        dttm_plus_1_second_str: str
    """

    dttm = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
    plus_1_second = timedelta(seconds=1)
    dttm_plus_1_second = dttm + plus_1_second
    dttm_plus_1_second_str = dttm_plus_1_second.strftime("%Y-%m-%d %H:%M:%S")

    return dttm_plus_1_second_str

def remove_timezone(dttm_timezone):
    """
    Summary:
        The function take as input a timestamp (format es. 2024-03-21 23:41:34+00:00) and return
        a timestamp (format es. 2024-09-25 12:43:13)
    Arg:
        dttm_timezone:
            timestamp (format es. 2024-03-21 23:41:34+00:00)
    Return:
        dttm_no_timezone
    """

    dttm_no_timezone = dttm_timezone.replace(tzinfo=None)
    
    return dttm_no_timezone

def hashing_sha256(value_to_hash):
    value_clean = value_to_hash.lower().replace(" ","")
    hashed_value = hashlib.sha256(value_clean.encode("utf-8")).hexdigest()
    return hashed_value