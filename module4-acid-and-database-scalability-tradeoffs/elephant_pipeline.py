import os
import psycopg2
from dotenv import load_dotenv


class ElephantPipeline:

  def __init__(self):
    # Load .env file and get credentials
    load_dotenv()
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASS = os.getenv("DB_PASS")
    DB_HOST = os.getenv("DB_HOST")

    # Connect to ElephantSQL-hosted PostgreSQL DB
    self.conn = psycopg2.connect(dbname=DB_NAME,
                                 user=DB_USER,
                                 password=DB_PASS,
                                 host=DB_HOST)

    self.cursor = self.conn.cursor()


  def create_table(self, table_name, columns):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS {0} (
      {1}
    );
    """.format(table_name, ",".join([k + " " + v for k, v in columns.items()]))

    self.cursor.execute(create_table_query)


  def drop_table(self, table_name):
    drop_table_query = f"""
    DROP TABLE {table_name}
    """
    self.cursor.execute(drop_table_query)

  @staticmethod
  def to_sql_value(val):
    return "'" + val.replace("'", "''") + "'"


  @staticmethod
  def to_sql_row(row):
    return "(" + ",".join(map(ElephantPipeline.to_sql_value, row)) + ")"


  def insert_rows(self, table_name, col_names, rows):
    insert_query = """
    INSERT INTO {0}
      ({1})
    VALUES {2};
    """.format(table_name,
               ",".join(col_names),
               ",".join(map(ElephantPipeline.to_sql_row, rows)))

    self.cursor.execute(insert_query)

  def make_query(self, query):
      self.cursor.execute(query)
      return self.cursor.fetchall()



  def copy_table(self, table_name, columns, rows):
    self.create_table(table_name, columns)
    self.insert_rows(table_name, columns.keys(), rows)
    self.conn.commit()
