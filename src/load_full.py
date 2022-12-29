#!/usr/bin/env python3
from alpha_vantage.timeseries import TimeSeries
from time import sleep
from datetime import date
from sqlalchemy import create_engine
import psycopg2
import psycopg2.sql
import pandas as pd
import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument("ticker", help="Ticker of stock to get")
parser.add_argument("apikey", help="Alpha Vantage API key")
parser.add_argument("-d", "--dummy", help="Skip real API calls (useful for script testing)",
  action="store_true")
parser.add_argument("-i", "--interval",
  help="Time interval between two consecutive data points in the time series. The following values are supported: 1min, 5min, 15min, 30min, 60min, default 15min",
  default="15min")
parser.add_argument("-o", "--output",
  help="Save data to directory. Filenames format example GOOGL_year2_month12.csv")
parser.add_argument("-H", "--host",
  help="Postgres hostname (default: localhost)",
  default="localhost")
parser.add_argument("-P", "--port",
  help="Postgres port (default: 5432)",
  default="5432")
parser.add_argument("-u", "--user",
  help="Postgres username (default: postgres)",
  default="postgres")
parser.add_argument("-p", "--password",
  help="Postgres password (default: postgres)",
  default="postgres")
parser.add_argument("-D", "--database",
  help="Postgres database (default: Stocks)",
  default="Stocks")

args = parser.parse_args()

apikey = args.apikey
#apikey='7ZYTGG8N0LLVQ9FJ'
#engine = create_engine('postgresql://postgres:postgres@localhost:5432/Stocks')
engine = create_engine('postgresql://' + args.user + ':' + \
  args.password + '@' + args.host + ':' + args.port + \
  '/' + args.database)

#stocks_list = ('AAPL', 'GOOGL', 'MSFT', 'IBM', 'NVDA', 'ORCL', 'INTC', 'QCOM', 'AMD', 'TSM')

# Загружает данные из API за месяц и сохраняет их в csv файл
def get_month_slice(stock_id, interval, slice):
  if (args.dummy):
    print("get_month_slice", stock_id, interval, slice)
    return 0

  ts = TimeSeries(key=apikey, output_format='csv')
  data = ts.get_intraday_extended(symbol=stock_id, interval=interval, slice = slice)
  df = pd.DataFrame(list(data[0]))
  header_row=0
  df.columns = df.iloc[header_row]
  df = df.drop(header_row)
  df['time'] = pd.to_datetime(df['time'])
  df['open'] = pd.to_numeric(df['open'])
  df['high'] = pd.to_numeric(df['high'])
  df['low'] = pd.to_numeric(df['low'])
  df['close'] = pd.to_numeric(df['close'])
  df['volume'] = pd.to_numeric(df['volume'])
  df.set_index('time', inplace=True)

  if (args.output):
    if (not os.path.exists(args.output)):
      os.makedirs(args.output)
    # имя файла по шаблону, например GOOGL_year2_month12.csv
    fname = args.output + "/" + stock_id + '_' + slice + '.csv'
    print(fname)
    df.to_csv(fname)

  df.index.names = ['dt']
  df.to_sql(stock_id.lower(), engine, if_exists='append')

# Загружает полный дамп данных за два года для данного тикера (24 куска помесячно)
for current_year in [1, 2]:
  for current_month in range(1, 13):
#  for current_month in range(1, 2):
    month_slice = 'year' + str(current_year) + 'month' + str(current_month)
    print(args.ticker, month_slice)
    get_month_slice(args.ticker, args.interval, month_slice)
    # на стандартном (бесплатном) плане  ограничение 5 API запросов в минуту, поэтому спим между запросами
    sleep(15)

# Создаём промежуточный слой (материализованное представление
# с некоторыми предвычисленными значениями
conn = psycopg2.connect(database=args.database, user=args.user,
  password=args.password, host=args.host, port=args.port)

conn.autocommit = True
cursor = conn.cursor()

viewname=args.ticker.lower()+"view"
temp_table=args.ticker.lower()+"temp_table"
temp_table_wtime=args.ticker.lower()+"temp_table_wtime"
sql = """
CREATE MATERIALIZED VIEW {viewname} AS
WITH
{temp_table} AS (
SELECT *, date(dt) AS day, dt::time AS time FROM {ticker} ORDER BY dt
),
{temp_table_wtime} AS (
SELECT *, min(dt::time) OVER w AS open_time, max(dt::time) OVER w AS close_time,
    max(volume) OVER w AS maxvolume, max(high) OVER w AS maxrate, min(low) OVER w AS minrate 
FROM {temp_table}
WINDOW w AS (PARTITION BY day)
)
SELECT * FROM {temp_table_wtime};
"""

cursor.execute(psycopg2.sql.SQL(sql).format(
  viewname=psycopg2.sql.Identifier(viewname),
  ticker=psycopg2.sql.Identifier(args.ticker.lower()),
  temp_table=psycopg2.sql.Identifier(temp_table),
  temp_table_wtime=psycopg2.sql.Identifier(temp_table_wtime)))
conn.commit()
conn.close()
