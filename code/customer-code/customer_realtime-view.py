import json
import pandas as pd
import matplotlib.pyplot as plt
import redis,os
import argparse
import matplotlib.colors as mcolors

r = redis.Redis(host='localhost', port=6379, db=0)

curr_path = os.path.dirname(os.path.abspath(__file__))

parser = argparse.ArgumentParser()
parser.add_argument('--n',help='Number of locations')

args = parser.parse_args()


df = pd.DataFrame({'location_id': [], 'total_tip': []})
for i in range(1,264):
    df = df.append({'location_id': i, 'total_tip': 0}, ignore_index=True)

print(df)

print("Customer Realtimeview App")

print("This will start updating as soon as streaming results are out")





def updateDataframe(location_id,total_tip,n):
    df.iat[location_id-1,1] = total_tip
    #Getting the top n tip earning areas 
    df_plot = df.nlargest(n, 'total_tip')
   
    
    df_plot.plot(kind='bar',x='location_id',y='total_tip',color=mcolors.TABLEAU_COLORS)

    plt.title('Ranking of locations w.r.t total tip given by passengers')
    plt.xlabel('LocationID')
    plt.ylabel('Tip Amount')
    plt.tight_layout()
    
    plt.savefig(curr_path+"/../../reports/near-realtime_results/near-realtime-bar-chart.png")
    plt.clf()

    print(df_plot)


while True:
    for i in range(1,264):
        key = "L"+str(i)
        value = r.get(key)
        n = args.n
        if value is not None:
            #obj = value.decode('utf-8')
            obj = json.loads(value.decode('utf-8'))
            updateDataframe(int(obj['location_id']),float(obj['total_tip']),int(n))

        
        