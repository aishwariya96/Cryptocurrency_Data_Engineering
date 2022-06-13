from pymongo import MongoClient

client = MongoClient('mongodb+srv://dataengineeringproject:data.engineering@cluster0.hz7bq3b.mongodb.net/?retryWrites=true&w=majority')

db = client['Coincap']
collection = db['Cryptocollection']

print(collection.find_one())

