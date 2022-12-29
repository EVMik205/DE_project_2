#!/usr/bin/env python3
from time import sleep
from datetime import date
import psycopg2
import psycopg2.sql
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("stocks_list", nargs='+', help="List of tickers to create data mart")
parser.add_argument("-d", "--dummy", help="Skip real SQL calls (useful for script testing)",
  action="store_true")
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

conn = psycopg2.connect(database=args.database, user=args.user,
  password=args.password, host=args.host, port=args.port)

conn.autocommit = True
cursor = conn.cursor()

mart_query = psycopg2.sql.SQL("CREATE MATERIALIZED VIEW stocks_mart AS (with")
sql_template = """
{tckr}_sum_volume AS (
select day, sum(volume) as sum_volume from {tckr}view group by day order by day
),
{tckr}_open AS (
  select day, open from {tckr}view where time=open_time
),
{tckr}_close AS (
  select day, close from {tckr}view where time=close_time
),
{tckr}_volume AS (
  select day, min(time) as maxvolume_time from {tckr}view where volume=maxvolume group by day order by day
),
{tckr}_maxrate AS (
  select day, min(time) as maxrate_time from {tckr}view where high=maxrate group by day order by day
),
{tckr}_minrate AS (
  select day, min(time) as minrate_time from {tckr}view where low=minrate group by day order by day
),
{tckr}_mart AS (
select '{tckr}' as ticker, {tckr}sv.*, {tckr}o.open, {tckr}c.close, (({tckr}c.close-{tckr}o.open)*100)/{tckr}o.open as percent_diff, {tckr}v.maxvolume_time, {tckr}max.maxrate_time, {tckr}min.minrate_time
from {tckr}_sum_volume {tckr}sv
join {tckr}_open {tckr}o on {tckr}sv.day={tckr}o.day
join {tckr}_close {tckr}c on {tckr}sv.day={tckr}c.day
join {tckr}_volume {tckr}v on {tckr}sv.day={tckr}v.day
join {tckr}_maxrate {tckr}max on {tckr}sv.day={tckr}max.day
join {tckr}_minrate {tckr}min on {tckr}sv.day={tckr}min.day
order by {tckr}sv.day
),"""

sql_union = """
select * from {tckr}_mart
UNION"""

sql_footer = psycopg2.sql.SQL("""
)
select row_number() over () as id, * from all_mart
)
""")

for stock_id in args.stocks_list:
  sql_cte = psycopg2.sql.SQL(sql_template).format(
  tckr=psycopg2.sql.Identifier(stock_id.lower()))
  mart_query = mart_query + sql_cte

mart_query = mart_query + psycopg2.sql.SQL(" all_mart AS (")

for stock_id in args.stocks_list[:-1]:
  sql_cte = psycopg2.sql.SQL(sql_union).format(
  tckr=psycopg2.sql.Identifier(stock_id.lower()))
  mart_query = mart_query + sql_cte

sql_cte = psycopg2.sql.SQL(" select * from {tckr}_mart order by day").format(
  tckr=psycopg2.sql.Identifier(args.stocks_list[-1].lower()))
mart_query = mart_query + sql_cte + sql_footer
mart_query = mart_query.as_string(conn).replace('"', '')

if (args.dummy):
  print(mart_query)
else:
  cursor.execute(mart_query)

conn.commit()
conn.close()
