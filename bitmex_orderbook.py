
# Recent Order Book Data
# {
#  "id": 8799271500,
#  "side": "Sell",
#  "size": 6200,
#  "price": 7285.0,
# }

from bitmex_websocket import BitMEXWebsocket
from google.cloud import bigquery
import logging
from time import sleep
import json
import time
import uuid
from libraries.bigquery import BigQuery
import os

schema = [
  bigquery.SchemaField('id', 'INTEGER', mode='REQUIRED'),
  bigquery.SchemaField('side', 'STRING', mode='REQUIRED'),
  bigquery.SchemaField('size', 'STRING', mode='REQUIRED'),
  bigquery.SchemaField('price', 'FLOAT', mode='REQUIRED'),
]


def run():
    if "BITMEX_API_KEY" not in os.environ:
      print("BITMEX_API_KEY env var must be set")
      exit(1)

    if "BITMEX_API_SECRET" not in os.environ:
      print("BITMEX_API_SECRET env var must be set")
      exit(1)

    bq = BigQuery(
      table="XBTUSD_orderbook",
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
        rows_to_insert = ws.market_depth()
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
