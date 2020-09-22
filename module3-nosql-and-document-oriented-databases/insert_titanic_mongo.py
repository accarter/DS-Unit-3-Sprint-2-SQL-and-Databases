import os
from dotenv import load_dotenv
import pymongo
import csv


load_dotenv()


MONGO_USER = os.getenv("MONGO_USER", "OOPS")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "OOPS")
MONGO_CLUSTERNAME = os.getenv("MONGO_CLUSTERNAME", "OOPS")
PROTOCOL = "mongodb+srv"
QUERY = "retryWrites=true&w=majority"
URI = f"{QUERY}://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_CLUSTERNAME}?{QUERY}"


client = pymongo.MongoClient(URI)
my_db = client.titanic_data
passenger_table = my_db.passengers


with open('titanic.csv') as f:
  passenger_table.insert_many(csv.DictReader(f))

