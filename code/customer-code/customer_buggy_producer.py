from kafka import KafkaProducer
import csv,sys
import logging
import argparse
import os
import pandas as pd 
import json

curr_path = os.path.dirname(os.path.abspath(__file__))


logging.basicConfig(filename=curr_path+'/../../logs/customer_buggy_producer.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)

parser = argparse.ArgumentParser()
#parser.add_argument('--data',help='Data in CSV format')
parser.add_argument('--rows',help='Number of rows')
parser.add_argument('--err',help='Error Rate')

args = parser.parse_args()

csv.field_size_limit(sys.maxsize)

producer = KafkaProducer(bootstrap_servers='localhost:9092')


data = pd.read_csv(curr_path+"/../../data/yellow_tripdata_2019-01.csv",nrows=int(args.rows)) 

error_rate = int(int(args.rows)*(int(args.err)/100))

interval_of_wrong_messages = int(int(args.rows)/error_rate)

#print(interval_of_wrong_messages)

count = 0
wmc = 0
def f(x):
    a = x.to_json()
    global count
    count += 1
    if count % interval_of_wrong_messages == 0:
        global wmc
        wmc += 1
        wrong_data = dict({
            ('SomeRandom', 'Data')        
        })
        #print(json.dumps(wrong_data).encode('utf-8'))
        logging.info("Sent a wrong data")
        producer.send('customerstreamapp-input',json.dumps(wrong_data).encode('utf-8'))
    else:
        producer.send('customerstreamapp-input',a.encode('utf-8'))

logging.info("----------------------------------------------------")
logging.info("----------------------------------------------------")
logging.info("----------------------------------------------------")
logging.info("--------------------Fresh Run-----------------------")
logging.info("----------------------------------------------------")
logging.info("----------------------------------------------------")
logging.info("----------------------------------------------------")
logging.info("Kafka Producer started sending data")

data.apply(f, axis=1)
producer.close()


#print(wmc)


logging.info("Kafka Producer finished sending data")
logging.info("Total # of rows sent = "+args.rows)
logging.info("Total # Wrong data sent = "+wmc)
logging.shutdown()


