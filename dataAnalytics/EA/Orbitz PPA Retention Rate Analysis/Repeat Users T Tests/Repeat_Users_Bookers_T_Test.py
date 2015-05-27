
import numpy as np
import pandas as pd
import vertica_python as vp
from scipy.stats import ttest_ind
from scipy.stats import pearsonr
from scipy.stats import ttest_1samp
from scipy.stats import tstd


arr = []
col_name = ['traffic_share_type', 'publisher_user_id', 'number_of_visits']


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
            SELECT traffic_share_type, a1.publisher_user_id, CASE WHEN SUM(CASE  WHEN   a2.requested_at_in_et >= a1.first_login_date THEN net_conversion_value ELSE 0 END) > 0 THEN 1 ELSE 0 END AS Booker
            FROM 
            
                    (select 
                            publisher_user_id, 
                            min(requested_at_in_et) as first_login_date,                      
                            traffic_share_type
                    from intent_media_log_data_production.ad_calls
                    where ad_unit_type = 'META' 
                            and ip_address_blacklisted = false 
                            and site_type = 'ORBITZ_GLOBAL'
                            AND publisher_user_id not in 
                            
                                    (select  publisher_user_id
                                    from intent_media_log_data_production.ad_calls
                                    where ad_unit_type = 'META' 
                                            and ip_address_blacklisted = false 
                                            and site_type = 'ORBITZ_GLOBAL'
                                    GROUP BY publisher_user_id 
                                    HAVING count(DISTINCT traffic_share_type) > 1)
                                    
                                    and month(requested_at_date_in_et) = 11
                                    and publisher_user_id <>  '' 
                       GROUP BY publisher_user_id, traffic_Share_type) a1
            
            LEFT JOIN 
            
                    (SELECT DISTINCT publisher_user_id, net_conversion_value, requested_at_in_et
                    FROM intent_media_log_data_production.conversions
                    WHERE SITE_TYPE = 'ORBITZ_GLOBAL' AND entity_id = 55 AND ip_address_blacklisted = false
                    AND product_category_type = 'HOTELS'
                    ) a2
                    
            ON a1.publisher_user_id = a2.publisher_user_id
           -- WHERE a2.requested_at_in_et >= a1.first_login_date
             ---AND datediff(day,first_login_date, a2.requested_at_in_et) between 0 and 120
            GROUP BY traffic_share_type, a1.publisher_user_id 
        
            """

print 'Pulling Data'

cur.execute(sql_string)
conn.close()

print 'data fetch process completed..'

raw_data = pd.DataFrame(arr, columns=col_name)

PUBLISHER = raw_data[raw_data['traffic_share_type'] == 'PUBLISHER']['number_of_visits'].astype(int)
INTENT_MEDIA = raw_data[raw_data['traffic_share_type'] == 'INTENT_MEDIA']['number_of_visits'].astype(int)

print raw_data[1:10]
print PUBLISHER[1:10]
print INTENT_MEDIA[1:10]

print 'Pandas Tranformation complete..'

print ttest_ind(PUBLISHER, INTENT_MEDIA, 0, False)
print tstd(INTENT_MEDIA)
print tstd(PUBLISHER)