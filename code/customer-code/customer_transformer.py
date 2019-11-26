import json
import redis
import os

curr_path = os.path.dirname(os.path.abspath(__file__))

r = redis.Redis(host='localhost', port=6379, db=0)

def view_all():
    for i in range(1,263):
        print(i)
        print(r.get(str(i)))

def truncate_redis():
    r.flushdb()




def populate_redis():
    with open(curr_path+"/../../data/nyu-2451-36743-geojson.json") as f:
        data = json.load(f)

    for feature in data['features']:    
        bbox = feature['properties']['bbox']
        location_id = feature['properties']['locationid']
        lon = (bbox[0]+bbox[2])/2
        lat = (bbox[1]+bbox[3])/2      
        r.set(str(location_id),str(lat)+ ","+str(lon))


populate_redis()