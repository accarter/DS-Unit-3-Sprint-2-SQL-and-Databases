import csv
from collections import defaultdict

from elephant_pipeline import ElephantPipeline


fieldnames = {
  'Survived': 'INT',
  'Pclass': 'INT',
  'Name': 'VARCHAR(128)',
  'Sex': 'VARCHAR(30)',
  'Age': 'real',
  'Siblings_Spouses_Aboard': 'INT',
  'Parents_Children_Aboard': 'INT',
  'Fare': 'real'
}

table_name = 'titanic'

with open('titanic.csv', 'r') as f:
  reader = csv.reader(f)
  header = next(reader)
  pipeline = ElephantPipeline()
  pipeline.drop_table(table_name)
  pipeline.copy_table(table_name, fieldnames, reader)
