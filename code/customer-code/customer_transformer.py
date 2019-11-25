import json
import redis

r = redis.Redis(host='localhost', port=6379, db=0)

def view_all():
    for i in range(1,263):
        print(i)
        print(r.get(str(i)))

def truncate_redis():
    r.flushdb()


def populate_redis():
    with open('/home/kibria/BDP/taxi_zones/nyu-2451-36743-geojson.json') as f:
        data = json.load(f)

    for feature in data['features']:    
        bbox = feature['properties']['bbox']
        location_id = feature['properties']['locationid']
        lon = (bbox[0]+bbox[2])/2
        lat = (bbox[1]+bbox[3])/2      
        r.set(str(location_id),str(lat)+ ","+str(lon))


view_all()