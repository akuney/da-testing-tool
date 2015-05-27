
import numpy as np
import pandas as pd
import vertica_python as vp
from scipy.stats import ttest_ind
from scipy.stats import pearsonr
from scipy.stats import ttest_1samp

arr = []
col_name = ['adv_1_price', 'adv_2_price', 'pct_diff_3_4', 'pct_diff_4_3']


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
            SELECT CAST(advertiser_price_2 AS FLOAT), CAST(advertiser_price_3 AS FLOAT), CAST(PRCT_Differences_2_3 AS FLOAT), CAST(PRCT_Differences_3_2 AS FLOAT)     
            FROM Intent_media_sandbox_production.EA_ppa_orbitz_channel_all
            WHERE Expedia_Present = 1 AND Hotels_present = 1
            AND requested_at_date_in_et = '02/12/2015'
            """

print 'Pulling Data'

cur.execute(sql_string)
conn.close()

print 'data fetch process completed..'

raw_data = pd.DataFrame(arr, columns=col_name)

# cheaptickets = raw_data[raw_data['Advertiser_1'] == 'Cheaptickets']

print raw_data[1:10]

print 'Pandas Tranformation complete..'

print ttest_ind(raw_data['adv_1_price'], raw_data['adv_2_price'])

print ttest_1samp(raw_data['pct_diff_3_4'],0)
print ttest_1samp(raw_data['pct_diff_4_3'],0)

print pearsonr(raw_data['adv_1_price'], raw_data['adv_2_price'])