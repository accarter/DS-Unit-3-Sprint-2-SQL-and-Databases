import os
import psycopg2
from dotenv import load_dotenv


class ElephantPipeline:

  def __init__(self):
    self._init_el_conn()

  def _init_el_conn(self):
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


  @staticmethod
  def to_tuple(row):
    return "(" + ", ".join(
      "'{}'".format(col.replace("'", "''")) for col in row) + ")"

  def insert_rows(self, table_name, columns, rows):
    insert_query = """
    INSERT INTO {0}
      ({1})
    VALUES {2};
    """.format(table_name, 
    ",".join(columns.keys()), 
    ",\n".join(map(lambda x: str(self.to_tuple(x)), rows)))

    print(insert_query[:1000])
    self.cursor.execute(insert_query)


  def copy_table(self, table_name, columns, rows):
    self.create_table(table_name, columns)
    self.insert_rows(table_name, columns, rows)
    self.conn.commit()


tables = {
  "armory_item": {
    "item_id": "SERIAL PRIMARY KEY",
    "name": "VARCHAR(30)",
    "value": "INT",
    "weight": "INT"
  },
  "armory_weapon": {
    "item_ptr_id": "SERIAL PRIMARY KEY",
    "power": "INT"
  },
  "charactercreator_character_inventory": {
    "id": "SERIAL PRIMARY KEY",
    "character_id": "INT",
    "item_id": "INT"
  },
  "charactercreator_cleric": {
    "character_ptr_id": "INT",
    "using_shield": "INT",
    "mana": "INT"
  },
  "charactercreator_fighter": {
    "character_ptr_id": "INT",
    "using_shield": "INT",
    "rage": "INT"
  },
  "charactercreator_mage": {
    "character_ptr_id": "INT",
    "has_pet": "INT",
    "mana": "INT"
  },
  "charactercreator_thief": {
    "character_ptr_id": "INT",
    "is_sneaking": "INT",
    "energy": "INT"
  },
  "charactercreator_necromancer": {
    "mage_ptr_id": "INT",
    "talisman_charged": "INT"
  }
}

# for table, columns in tables.items():
#   elephant_pipeline(table, columns)


# delete_query = """
# DELETE FROM rpg_characters
# WHERE character_id < 9
# """

# update_query = """
# UPDATE rpg_characters SET intelligence=1
# WHERE character_id = 9
# """
