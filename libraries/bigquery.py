from google.cloud import bigquery

class BigQuery:
  def __init__(self, dataset, table, schema):
    self.dataset = dataset
    self.schema = schema
    self.table = table

    self.client = bigquery.Client()
    self.dataset_ref = self.client.dataset(dataset)
    self.dataset = self.create_dataset()
    self.tbl = self.create_table()

  def create_dataset(self):
    dataset = bigquery.Dataset(self.dataset_ref)
    dataset.location = 'US'
    try:
      dataset = self.client.create_dataset(dataset)
    except Exception as e:
      print("Dataset exists: %s", e)
    return dataset

  def create_table(self):
    table_ref = self.dataset_ref.table(self.table)
    table = bigquery.Table(table_ref, schema=self.schema)
    try:
      table = self.client.create_table(table)
    except Exception as e:
      print("Table exists: %s", e)
    return table
