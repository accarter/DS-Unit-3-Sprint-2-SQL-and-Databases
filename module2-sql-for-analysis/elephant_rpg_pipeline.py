import os
import psycopg2
from dotenv import load_dotenv

# Load .env file and get credentials
load_dotenv()
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")

# Connect to ElephantSQL-hosted PostgreSQL DB
conn = psycopg2.connect(dbname=DB_NAME,
                        user=DB_USER,
                        password=DB_PASS,
                        host=DB_HOST)

cursor = conn.cursor()

cursor.execute("SELECT * FROM test_table;")
results = cursor.fetchall()
# print(results)


# Connect to SQLite DB for RPG Data

import sqlite3

sl_conn = sqlite3.connect('rpg_db.sqlite3')
sl_cursor = sl_conn.cursor()
characters = sl_cursor.execute('SELECT * FROM charactercreator_character;').fetchall()
# print(characters)


# Create the Character Table in Postgres and Insert Data

create_character_table_query = """
CREATE TABLE IF NOT EXISTS rpg_characters (
    character_id SERIAL PRIMARY KEY,
    name VARCHAR(30),
    level INT,
    exp INT,
    hp INT,
    strength INT, 
    intelligence INT,
    dexterity INT,
    wisdom INT
)
"""

cursor.execute(create_character_table_query)
conn.commit()


insert_query = """
INSERT INTO rpg_characters
  (character_id, name, level, exp, hp, strength, intelligence, dexterity, wisdom)
VALUES {0}
""".format(", ".join(map(str, characters)))



conn.commit()


# Create the Armory Item Table in Postgres and Insert Data

def get_rows(table_name):
  return sl_cursor.execute(f'SELECT * FROM {table_name};').fetchall()


def create_table(table_name, columns):
  create_table_query = """
  CREATE TABLE IF NOT EXISTS {0} (
    {1}
  );
  """.format(table_name, ",".join([k + " " + v for k, v in columns.items()]))

  cursor.execute(create_table_query)


def insert_rows(table_name, columns, rows):
  insert_query = """
  INSERT INTO {0}
    ({1})
  VALUES {2};
  """.format(table_name, ",".join(columns.keys()), ", ".join(map(str, rows)))

  cursor.execute(insert_query)


def elephant_pipeline(table_name, columns):
  create_table(table_name, columns)
  insert_rows(table_name, columns, get_rows(table_name))


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

for table, columns in tables.items():
  elephant_pipeline(table, columns)

conn.commit()

# delete_query = """
# DELETE FROM rpg_characters
# WHERE character_id < 9
# """

# update_query = """
# UPDATE rpg_characters SET intelligence=1
# WHERE character_id = 9
# """
