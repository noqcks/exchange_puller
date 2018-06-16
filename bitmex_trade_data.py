
# Recent Trade Data
# {
#  "foreignNotional": 200,
#  "grossValue": 3164600,
#  "homeNotional": 0.031646,
#  "price": 6320,
#  "side": "Buy",
#  "size": 200,
#  "symbol": "XBTUSD",
#  "tickDirection": "ZeroPlusTick",
#  "timestamp": "2018-06-13T23:12:36.909Z",
#  "trdMatchID": "50de0849-c19a-9589-5f5f-4f6731f2031d"
# }

from bitmex_websocket import BitMEXWebsocket
from google.cloud import bigquery
import logging
from time import sleep
import json
import time
from libraries.bigquery import BigQuery

schema = [
  bigquery.SchemaField('timestamp', 'TIMESTAMP', mode='REQUIRED'),
  bigquery.SchemaField('symbol', 'STRING', mode='REQUIRED'),
  bigquery.SchemaField('side', 'STRING', mode='REQUIRED'),
  bigquery.SchemaField('size', 'INTEGER', mode='REQUIRED'),
  bigquery.SchemaField('price', 'FLOAT', mode='REQUIRED'),
  bigquery.SchemaField('tickDirection', 'STRING', mode='REQUIRED'),
  bigquery.SchemaField('trdMatchID', 'STRING', mode='REQUIRED'),
  bigquery.SchemaField('grossValue', 'INTEGER', mode='REQUIRED'),
  bigquery.SchemaField('homeNotional', 'FLOAT', mode='REQUIRED'),
  bigquery.SchemaField('foreignNotional', 'INTEGER', mode='REQUIRED'),
]

def run():
    if "BITMEX_API_KEY" not in os.environ:
      print("BITMEX_API_KEY env var must be set")
      exit(1)

    if "BITMEX_API_SECRET" not in os.environ:
      print("BITMEX_API_SECRET env var must be set")
      exit(1)

    bq = BigQuery(
      table="XBTUSD_trade_data",
      dataset="bitmex",
      schema=schema)
    logger = setup_logger()

    ws = BitMEXWebsocket(
      endpoint="https://www.bitmex.com/api/v1",
      symbol="XBTUSD",
      api_key=os.environ.get('BITMEX_API_KEY'),
      api_secret=os.environ.get('BITMEX_API_SECRET')
    )

    while(ws.ws.sock.connected):
      if ws.api_key:
        rows_to_insert = ws.recent_trades()
        errors = bq.client.insert_rows(bq.tbl, rows_to_insert)
        if errors != []:
          print(errors)

def setup_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger

if __name__ == "__main__":
    run()
