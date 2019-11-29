from kafka import KafkaProducer
import csv,sys
import logging
import argparse
import os
import pandas as pd 
import json

curr_path = os.path.dirname(os.path.abspath(__file__))


logging.basicConfig(filename=curr_path+'/../../logs/customer_producer.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)

parser = argparse.ArgumentParser()
#parser.add_argument('--data',help='Data in CSV format')
parser.add_argument('--rows',help='Number of rows')


args = parser.parse_args()

csv.field_size_limit(sys.maxsize)

producer = KafkaProducer(bootstrap_servers='localhost:9092')


data = pd.read_csv(curr_path+"/../../data/yellow_tripdata_2019-01.csv",nrows=int(args.rows)) 

def f(x):
    a = x.to_json()
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


logging.info("Kafka Producer finished sending data")
logging.info("Total # of rows sent = "+args.rows)
logging.shutdown()




'''
with open("../../data/yellow_tripdata_2019-01.csv", 'rt') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
        count = 0
        for row in spamreader:
            count += 1
            if count == 1:
                continue
            colSize = len(row)
            new_dict = dict()
            for i in range(colSize):
                logging.info(row[i])

'''