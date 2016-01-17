import postgres
import settings
import yahoo_finance
import tickers
import os

from source import sql_statements

########################################################
# Get historical Yahoo! Finance data
# Check if load and target table exists (create both tables)
# For each ticker:
### Copy CSV file to Postgres load table
### Insert data from load to target table
### Drop staging table
########################################################

def _yahoo_finance_wrapper(symbol):
  yahoo_client = yahoo_finance.YahooFinance(symbol)
  return yahoo_client.response

def _create_table_wrapper(symbol):
  _settings = settings.Settings(symbol)
  postgres_client = postgres.Postgres(_settings.db_connection)
  raw_sql = sql_statements.Raw_sql(_settings.postgres_tables[symbol])
  postgres_client.execute_query(raw_sql.create_table(), 
                                result=False, 
                                file=False)
  return _copy_csv_wrapper(symbol)

def _copy_csv_wrapper(symbol):
  _settings = settings.Settings(symbol)
  postgres_client = postgres.Postgres(_settings.db_connection)
  postgres_client.copy_csv_with_header(_yahoo_finance_wrapper(symbol), 
                                       _settings.postgres_tables[symbol]["load_table"])
  return _insert_records_wrapper(symbol)

def _insert_records_wrapper(symbol):
  _settings = settings.Settings(symbol)
  postgres_client = postgres.Postgres(_settings.db_connection)
  postgres_client.last_update_date = postgres_client.get_last_update_date(_settings.postgres_tables[symbol]["target_table"])
  postgres_client.last_insert_date = postgres_client.get_last_insert_date(_settings.postgres_tables[symbol]["target_table"])
  raw_sql = sql_statements.Raw_sql(_settings.postgres_tables[symbol])
  postgres_client.execute_query(raw_sql.insert_records(),
                                result=False, 
                                file=False)
  
  count_updated = int(postgres_client.get_count_updated(_settings.postgres_tables[symbol]["target_table"]))
  count_inserted = int(postgres_client.get_count_inserted(_settings.postgres_tables[symbol]["target_table"]))
  
  print("%s rows updated" % str(count_updated))
  print("%s rows inserted" % str(count_inserted))

  if (count_updated > 0) or (count_inserted > 0):
    return _drop_table_wrapper(symbol)

def _drop_table_wrapper(symbol):
  _settings = settings.Settings(symbol)
  postgres_client = postgres.Postgres(_settings.db_connection)
  raw_sql = sql_statements.Raw_sql(_settings.postgres_tables[symbol])
  postgres_client.execute_query(raw_sql.drop_table(),
                                result=False, 
                                file=False)
  return _file_cleanup()

def _file_cleanup():
  os.remove("csv_file_to_load")

for symbol in tickers.symbols:
  print(symbol)
  _create_table_wrapper(symbol)
