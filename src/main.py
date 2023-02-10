from fastapi import FastAPI
from pydantic import BaseModel
from fastapi_pagination import Page, add_pagination, paginate
from data_ingest import get_mysql_connection
app = FastAPI()

class WxData(BaseModel):
    id:int
    date: str
    station: str
    max_temp: int
    min_temp: int
    precipitation: int

class WxDataAnalysis(BaseModel):
    id:int
    year: int
    station: str
    avg_max_temp: int
    avg_min_temp: int
    total_precipitation: int


@app.get("/weather", response_model=Page[WxData])
async def wx_data(date: str = None, station: str = None):
    connection, cursor = get_mysql_connection(allow_dictionary=True)
    if date and station:
        query = f"select id, DATE_FORMAT(date, '%Y-%m-%d') as date, station, max_temp, min_temp, precipitation from wx_data where date = '{date}' and station = '{station}'"
    elif date:
        query = f"select id, DATE_FORMAT(date, '%Y-%m-%d') as date, station, max_temp, min_temp, precipitation from wx_data where date = '{date}'"
    elif station:
        query = f"select id, DATE_FORMAT(date, '%Y-%m-%d') as date, station, max_temp, min_temp, precipitation from wx_data where station = '{station}'" 
    else:
        query = f"select id, DATE_FORMAT(date, '%Y-%m-%d') as date, station, max_temp, min_temp, precipitation from wx_data"
    cursor.execute(query)
    records = cursor.fetchall()
    connection.close()
    return paginate(records)

@app.get("/api/weather/stats", response_model=Page[WxDataAnalysis])
async def wx_data(date: str = None, station: str = None):
    connection, cursor = get_mysql_connection(allow_dictionary=True)
    if date and station:
        query = f"select id, date as year, station, avg_max_temp, avg_min_temp, total_precipitation from wx_analysis where date = '{date}' and station = '{station}'"
    elif date:
        query = f"select id, date as year, station, avg_max_temp, avg_min_temp, total_precipitation from wx_analysis where date = '{date}'"
    elif station:
        query = f"select id, date as year, station, avg_max_temp, avg_min_temp, total_precipitation from wx_analysis where station = '{station}'" 
    else:
        query = f"select id, date as year, station, avg_max_temp, avg_min_temp, total_precipitation from wx_analysis"
    cursor.execute(query)
    records = cursor.fetchall()
    print(records[0])
    connection.close()
    return paginate(records)

add_pagination(app)