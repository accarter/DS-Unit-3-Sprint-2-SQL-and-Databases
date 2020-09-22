import os
from dotenv import load_dotenv
import pymongo
import pandas
from pdb import set_trace as breakpoint


load_dotenv()

MONGO_USER = os.getenv("MONGO_USER", "OOPS")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "OOPS")
MONGO_CLUSTERNAME = os.getenv("MONGO_CLUSTERNAME", "OOPS")

uri = f"mongodb+srv://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_CLUSTERNAME}?retryWrites=true&w=majority"
client = pymongo.MongoClient(uri)
print("URI:", uri)

# breakpoint()

# Set the DB to Analytics
analytics_db = client.sample_analytics
print(analytics_db.list_collection_names())

# Access a specific collection
transactions = analytics_db.transactions
print(transactions.count_documents({"transaction_count": {"$gt": 50}}))

# Get all the customers into a DataFrame
customers = analytics_db.customers
all_customers = customers.find({})

import pandas as pd

df = pd.DataFrame(all_customers)


customers.insert_one({'full_name': 'Adam Carter'})

import json

with open('test_data_json.txt') as json_file:
  rpg_data = json.load(json_file)

my_db = client.rpg_data

character_table = my_db.characters

character_table.insert_many(rpg_data)
print(character_table.columns)
