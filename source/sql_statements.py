class Raw_sql(object):
 
  def __init__(self, params):
    self.load_table = params["load_table"]
    self.target_table = params["target_table"]

  def create_table(self):
    sql_statements = """
      BEGIN TRANSACTION;

      CREATE TABLE IF NOT EXISTS %(load_table)s
      (
        date date,
        open double precision,
        high double precision,
        low double precision,
        close double precision,
        volume bigint,
        adj_close double precision
      );

      CREATE TABLE IF NOT EXISTS %(target_table)s
      (
         id bigserial,
         date date,
         open double precision,
         high double precision,
         low double precision,
         close double precision,
         volume bigint,
         adj_close double precision,
         crt_dttm timestamp default now()::timestamp,
         updt_dttm timestamp default now()::timestamp
      );
      
      COMMIT;
      END TRANSACTION;
      """ % {"load_table": self.load_table, "target_table": self.target_table}
    return sql_statements

  def insert_records(self):
    sql_statements = """
      BEGIN TRANSACTION;

      DELETE FROM %(target_table)s
      USING %(load_table)s
      WHERE %(target_table)s.date = %(load_table)s.date;

      INSERT INTO %(target_table)s (date, open, high, low, close, volume, adj_close)
      SELECT date,
             open,
             high,
             low,
             close,
             volume,
             adj_close
      FROM %(load_table)s;

      END TRANSACTION;
      """ % {"load_table": self.load_table, "target_table": self.target_table}
    return sql_statements

  def drop_table(self):
    sql_statements = """
      BEGIN TRANSACTION;

      DROP TABLE IF EXISTS %(load_table)s; 

      END TRANSACTION;
      """ % {"load_table": self.load_table}
    return sql_statements

