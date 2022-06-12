# Import some necessary modules
import json
from pymongo import MongoClient
from kafka import KafkaConsumer
from constants import data_pull_limit

topic = 'crypto_data_kafkatopic'

# Connect to MongoDB and distance_data database
try:
    client = MongoClient('mongodb+srv://data-engineering:data.engineering@cryptocluster.hyh7kfb.mongodb.net/?retryWrites=true&w=majority')
    db = client.Crypto_db
    collection = db.Crypto_collection
    print("Connected successfully!")
except:
    print("Could not connect to MongoDB")


consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest', value_deserializer=lambda x: json.loads(x.decode('utf-8')))


for idx, message in enumerate(consumer):
    msg = message.value
    if idx == (data_pull_limit - 1):
        msg = message.value[:-1]

    record = json.loads(msg)
    try:
        collection.insert_one(record)
        print(f'{record} added to {collection}')
    except:
        print(f"Could not insert {record} into MongoDB")
