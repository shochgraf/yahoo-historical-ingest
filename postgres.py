import sys
import sqlalchemy
import psycopg2

class Postgres(object):
  
  def __init__(self, params):
    self.host = params["pg_host"]
    self.dbname = params["pg_db_name"]
    self.user = params["pg_user"]
    self.password = params["pg_pw"]
    self.port = params["pg_port"]

  def create_engine(self, user, password, host, port, dbname):
    connStr = "postgresql+psycopg2://%s:%s@%s:%s/%s" % (user, password, host, port, dbname)
    engine = sqlalchemy.create_engine(connStr)
    return engine
  
  def get_last_update_date(self, table):
    last_updated_query = "SELECT MAX(updt_dttm) FROM %s;" %(table)
    last_update_date = self.execute_query(last_updated_query, result=True)
    if last_update_date == "None":
      return self.execute_query("SELECT NOW()::timestamp", result=True)
    else:
      return last_update_date

  def get_last_insert_date(self, table):
    last_inserted_query = "SELECT MAX(crt_dttm) FROM %s;" %(table)
    last_insert_date = self.execute_query(last_inserted_query, result=True)
    if last_insert_date == "None":
      return self.execute_query("SELECT NOW()::timestamp", result=True)
    else:
      return last_insert_date       

  def get_count_inserted(self, table):
    count_inserted = "SELECT COUNT(*) FROM %s WHERE (crt_dttm = updt_dttm) AND (crt_dttm > cast('%s' as timestamp));" %(table, self.last_insert_date)
    return self.execute_query(count_inserted, result=True)

  def get_count_updated(self, table):
    count_updated = "SELECT COUNT(*) FROM %s WHERE (crt_dttm != updt_dttm) and (updt_dttm > cast('%s' as timestamp));" %(table, self.last_update_date)
    return self.execute_query(count_updated, result=True)

  def send_data_frame(self, targetSchema, targetTable, dataFrame, chunkSize): # Load Python dataframe into database
    try:
      engine = self.create_engine(self.user, self.password, self.host, self.port, self.dbname)
      dataFrame.to_sql(targetTable, engine, schema=targetSchema, if_exists='append', index=False, chunksize=chunkSize)
    except:
      StandardError

  def copy_csv_with_header(self, csvFile, load_table):
    engine = self.create_engine(self.user, self.password, self.host, self.port, self.dbname)
    connection = engine.raw_connection()
    cursor = connection.cursor()
    copy_sql = """
           COPY %(load_table)s FROM stdin WITH CSV HEADER
           DELIMITER as ','
           """ % {"load_table": load_table}
    with open(csvFile, 'r') as f:
      cursor.copy_expert(sql=copy_sql, file=f)
      connection.commit()
      cursor.close()

  def execute_query(self, sqlStatement, result=False, file=False): # Send query to SQL database
    if file:
      with open(sqlStatement, 'r') as script: # Open and read the file as a single buffer
        sqlFile = script.read()
      if not script.close:
        script.close()
      sqlCommands = sqlFile.split(';') # all SQL commands (split on ';')
    else:
      sqlCommands = [sqlStatement]

    engine = self.create_engine(self.user, self.password, self.host, self.port, self.dbname)
    connection = engine.raw_connection()
    cursor = connection.cursor()
    for command in sqlCommands:    # Execute every command from the input file
      if command and (not command.isspace()):
        print("Running... \n%s" %(command))
        try:
          cursor.execute(command)
          if result:
            result = cursor.fetchone()
            return str(result[0])
        except psycopg2.OperationalError as msg:
          print("Command skipped: %s" %(msg))
    cursor.close()
    connection.close()
