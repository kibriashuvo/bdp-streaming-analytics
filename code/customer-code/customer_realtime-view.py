from kafka import KafkaConsumer
from json import loads
import pandas as pd
import matplotlib.pyplot as plt


df = pd.DataFrame({'location_id': [], 'total_tip': []})
for i in range(1,264):
    df = df.append({'location_id': i, 'total_tip': 0}, ignore_index=True)

print(df)



def updateOutputPlot(location_id,total_tip):
    df.iat[location_id-1,1] = total_tip
    #df.plot(kind='bar',x='location_id',y='total_tip')
    #plt.savefig("test.png")
    df_plot = df.nlargest(5, 'total_tip')
    print(df_plot)
    #df_plot.plot(kind='bar',x='location_id',y='total_tip')
    #plt.savefig("test.png")



consumer = KafkaConsumer(
    'customer_realtime_topic',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

for message in consumer:
    message = message.value
    updateOutputPlot(int(message['location_id']),float(message['total_tip']))  
    print('{} added to'.format(message))
