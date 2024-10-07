from datetime import datetime

def datetime_to_str(dt: datetime, dt_fmt: str ='%Y-%m-%d %H:%M:%S'):
    return dt.strftime(dt_fmt)

def datetime_to_date_str(dt: datetime, dt_fmt: str = '%Y-%m-%d'):
    return datetime_to_str(dt, dt_fmt)

def str_to_datetime(dt: str, dt_fmt: str='%Y-%m-%d %H:%M:%S'):
    return datetime.strptime(dt, dt_fmt)

def str_to_date(dt: str, dt_fmt: str='%Y-%m-%d'):
    return str_to_datetime(dt, dt_fmt)