from time import sleep
from json import dumps
from kafka import KafkaProducer
import random

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))


for e in range(1,1000):
    data = {'location_id' : str(random.randint(1, 264)),'total_tip':str(random.randint(1, 50)) }
    producer.send('customer_realtime_topic', value=data)
    print(data)
    sleep(0.5)