import os
import yaml

class Settings(object):

  def __init__(self, symbol, options={}):
    env = options.get("env")

    if not env:
      default_env = 'staging' # Set config file
      env = os.getenv('ENV', default_env)
      pg_user = os.getenv('PG_STAGE_USER')
      pg_pw = os.getenv('PG_STAGE_PASS')
    else:
      pg_user = os.getenv('PGUSER')
      pg_pw = os.getenv('PGPASS')

    self.symbol = symbol

    # load db access info
    self.db_connection = self._load_config("config/db_connection.yml")[env]
    self.db_connection["pg_user"] = pg_user
    self.db_connection["pg_pw"] = pg_pw
    
    # load postgres tables info
    self.postgres_tables = self._load_config("config/postgres_tables.yml")
    
    # self.aws_creds = {
    #   "access_key": os.getenv("ACCESSKEY"),
    #   "secret_key": os.getenv("SECRETKEY")
    # }

  # Load config file
  def _load_config(self, config_file):
    with open(config_file, 'r') as f:
      config = yaml.load(f)
    return config