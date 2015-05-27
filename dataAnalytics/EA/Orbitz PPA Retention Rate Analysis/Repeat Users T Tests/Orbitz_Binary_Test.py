
import numpy as np
import pandas as pd
import vertica_python as vp
from scipy.stats import binom_test


arr = []
col_name = ['Differences', 'pop']


def row_handler(row):
    arr.append(np.asarray(row))

conn = vp.connect({
    'host': 'production-vertica-cluster-with-failover.internal.intentmedia.net',
    'port': 5433,
    'user': 'tableau',
    'password': '9WOGfffN',
    'database': 'intent_media',
    'option': 'CurrentLoadBalance=1'
})

cur = conn.cursor(row_handler=row_handler)
sql_string = """ 
            SELECT 
            CAST(SUM(Case  WHEN abs(advertiser_price_2 - advertiser_price_3) > 1 THEN 1 ELSE 0 END) as float) as Differences, 
            --CAST(SUM(Case  WHEN advertiser_price_2 - advertiser_price_3 < -1 THEN 1 ELSE 0 END) as float) as wins,  
            cast( COUNT(*) as float) as pop
            FROM Intent_media_sandbox_production.EA_ppa_orbitz_channel_all
            WHERE Expedia_Present = 1 AND Hotels_present = 1

            """

print 'Pulling Data'

cur.execute(sql_string)
conn.close()

print 'data fetch process completed..'

# raw_data = pd.DataFrame(arr, columns=col_name)
# print raw_data
# cheaptickets = raw_data['Advertiser_1'] == 'Cheaptickets'

print 'Pandas Tranformation complete..'

# success = float(arr[0])
# pop = float(arr[1])

print arr[0][0]
print arr[0][1]

print binom_test(arr[0][0], arr[0][1], 0.02)
