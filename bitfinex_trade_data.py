
# Recent Order Book Data
# {
#  "id": 8799271500,
#  "side": "Sell",
#  "size": 6200,
#  "price": 7285.0,
# }

from btfxwss import BtfxWss
from google.cloud import bigquery
import logging
from time import sleep
import json
import time
import uuid
from libraries.bigquery import BigQuery
from itertools import chain

schema = [
  bigquery.SchemaField('ts', 'TIMESTAMP', mode='REQUIRED'),
  bigquery.SchemaField('id', 'INTEGER', mode='REQUIRED'),
  bigquery.SchemaField('side', 'STRING', mode='REQUIRED'),
  bigquery.SchemaField('size', 'STRING', mode='REQUIRED'),
  bigquery.SchemaField('price', 'FLOAT', mode='REQUIRED'),
]


def depth(l):
    depths = []
    for item in l:
        if isinstance(item, list):
            depths.append(depth(item))
    if len(depths) > 0:
        return 1 + max(depths)
    return 1

def run():
    bq = BigQuery(
      table="BTCUSD_trade_data",
      dataset="bitfinex",
      schema=schema)
    logger = setup_logger()

    wss = BtfxWss()
    wss.start()

    while not wss.conn.connected.is_set():
      sleep(1)

    wss.subscribe_to_trades('BTCUSD')

    t = time.time()
    while time.time() - t < 10:
      pass

    # access queue object
    books_q = wss.trades('BTCUSD')
    while wss.conn.connected.is_set():
      rows_to_insert = books_q.get()
      rows = []

      if depth(rows_to_insert[0]) == 3:
        continue
      if rows_to_insert[0][0] == 'te':
        continue

      side = "Sell" if rows_to_insert[0][1][2] < 0 else "Buy"
      r = {"ts": rows_to_insert[1], "price": rows_to_insert[0][1][3],"id": rows_to_insert[0][1][1], "side": side, "size": abs(rows_to_insert[0][1][2])}
      rows.append(r)

      errors = bq.client.insert_rows(bq.tbl, rows)
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
