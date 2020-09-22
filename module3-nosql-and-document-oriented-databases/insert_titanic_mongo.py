import os
from dotenv import load_dotenv
import pymongo
import csv


load_dotenv()

MONGO_USER = os.getenv("MONGO_USER", "OOPS")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "OOPS")
MONGO_CLUSTERNAME = os.getenv("MONGO_CLUSTERNAME", "OOPS")

uri = f"mongodb+srv://{MONGO_USER}" \
      f":{MONGO_PASSWORD}@{MONGO_CLUSTERNAME}" \
      f"?retryWrites=true&w=majority"

client = pymongo.MongoClient(uri)
print("URI:", uri)


my_db = client.titanic_data
passenger_table = my_db.passengers

with open('titanic.csv') as f:
  passenger_table.insert_many(csv.DictReader(f))

print(passenger_table.columns)
