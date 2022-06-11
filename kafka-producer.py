import json
from kafka import KafkaConsumer, KafkaProducer

coincap_df = json.load(open('crypto_data.json'))
coincap_data = coincap_df.split('},')

topic = 'crypto_data_kafkatopic'

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: json.dumps(x).encode('utf-8'))
consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest', value_deserializer=lambda x: json.loads(x.decode('utf-8')))


for jsondata in coincap_data:
    jsondata = jsondata.replace('[', '')
    jsondata = jsondata.replace(']', '')
    jsondata = jsondata + '}'
    producer.send(topic, value=jsondata)

for message in consumer:
    print(f"Message: {message.value}")
