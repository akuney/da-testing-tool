
---- T Test Query

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
                and requested_at_date_in_et >=   CURRENT_DATE - 14 AND requested_at_date_in_et < CURRENT_DATE
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
                 and requested_at_date_in_et >=   CURRENT_DATE - 14 AND requested_at_date_in_et < CURRENT_DATE
        ) a2
        on a1.publisher_user_id = a2.publisher_user_id AND a1.traffic_share_type = a2.traffic_share_type
        --where datediff(day,first_login_date,requested_at_date_in_et) between 7 and 120
        --AND datediff(day,first_login_date,requested_at_date_in_et) >= 1    ----Include this to limit to revists only
        GROUP BY                 CAST(a1.traffic_share_type as varchar(20)),
                CAST(a1.publisher_user_id  as varchar(30))


---- Query to get average number of repeat visits

SELECT traffic_share_type, avg(Greater_then_7) as Pct_visits_after_7_days, COUNT(DISTINCT publisher_user_id) as users
FROM (
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
               -- and month(requested_at_date_in_et) = 11
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
        ) a2
        on a1.publisher_user_id = a2.publisher_user_id AND a1.traffic_share_type = a2.traffic_share_type
        --where datediff(day,first_login_date,requested_at_date_in_et) between 7 and 120
        --AND datediff(day,first_login_date,requested_at_date_in_et) >= 1    ----Include this to limit to revists only
        GROUP BY                 CAST(a1.traffic_share_type as varchar(20)),
                CAST(a1.publisher_user_id  as varchar(30))
                ) main
GROUP BY traffic_share_type