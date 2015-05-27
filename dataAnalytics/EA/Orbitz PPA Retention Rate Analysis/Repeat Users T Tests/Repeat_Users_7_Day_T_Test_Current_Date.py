import numpy as np
import pandas as pd
import vertica_python as vp
import scipy.stats as scipy_stats

def run_analysis(lookback):

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

        select 
                CAST(a1.traffic_share_type as varchar(20)),
                CAST(a1.publisher_user_id  as varchar(30)),
                --COUNT( datediff(day,first_login_date,requested_at_date_in_et)) > as number_of_visit_days
               MAX(CASE WHEN datediff(day,first_login_date,requested_at_date_in_et) > 7 Then 1 ELSE 0 END) as Greater_then_7       
        from
        (
        select 
                publisher_user_id,
                min(requested_at_date_in_et) as first_login_date,
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
                and requested_at_date_in_et >=   CURRENT_DATE - {0:d} AND requested_at_date_in_et < CURRENT_DATE
                and publisher_user_id <>  ''
        group by publisher_user_id, traffic_share_type
        ) a1
        left join 
        (
        select
                publisher_user_id,
                requested_at_date_in_et,
                traffic_share_type
        from intent_media_log_data_production.ad_calls
        where ad_unit_type = 'META' 
                and ip_address_blacklisted = false 
                and site_type = 'ORBITZ_GLOBAL'
                and requested_at_date_in_et >=   CURRENT_DATE - {0:d} AND requested_at_date_in_et < CURRENT_DATE                
        ) a2
        on a1.publisher_user_id = a2.publisher_user_id AND a1.traffic_share_type = a2.traffic_share_type
        --where datediff(day,first_login_date,requested_at_date_in_et) between 7 and 120
        --AND datediff(day,first_login_date,requested_at_date_in_et) >= 1    ----Include this to limit to revists only
        GROUP BY                 CAST(a1.traffic_share_type as varchar(20)),
                CAST(a1.publisher_user_id  as varchar(30))
                """.format(lookback)

    # print 'Pulling Data'

    cur.execute(sql_string)
    conn.close()

    # print 'data fetch process completed..'

    raw_data = pd.DataFrame(arr, columns=col_name)

    PUBLISHER = raw_data[raw_data['traffic_share_type'] == 'PUBLISHER']['number_of_visits'].astype(int)
    INTENT_MEDIA = raw_data[raw_data['traffic_share_type'] == 'INTENT_MEDIA']['number_of_visits'].astype(int)

    # print raw_data[1:10]
    # print PUBLISHER[1:10]
    # print INTENT_MEDIA[1:10]

    # print 'Pandas Tranformation complete..'


    print "This is for {0:d}".format(lookback)
    print scipy_stats.ttest_ind(PUBLISHER, INTENT_MEDIA, 0, False)

    print scipy_stats.tstd(INTENT_MEDIA)
    print scipy_stats.tstd(PUBLISHER)

    print INTENT_MEDIA.mean()
    print PUBLISHER.mean()

    print len(INTENT_MEDIA)
    print len(PUBLISHER)

run_analysis(15)
run_analysis(30)
run_analysis(45)
run_analysis(60)
