 SELECT traffic_share_type, avg(sessions_per_user) as avg_sessions_per_user, count(Distinct publisher_user_id) as users
 FROM (
       SELECT 
       a1.traffic_share_type,
       a1.publisher_user_id,
       COUNT(DISTINCT a2.session_ID) as sessions_per_user
       FROM
       
       ( select 
                publisher_user_id,
                min(requested_at_date_in_et) as first_login_date,                
                0 as session_id,
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
        group by publisher_user_id, traffic_share_type
        ) a1
        left join 
        (
                select
                        traffic_share_type,
                        publisher_user_id,
                        CONDITIONAL_TRUE_EVENT(DATEDIFF('minute', LAG(requested_at_in_et), requested_at_in_et) >=  30) OVER (PARTITION BY publisher_user_id ORDER BY requested_at_in_et) as session_ID,
                        requested_at_date_in_et,
                        requested_at_in_et
                from intent_media_log_data_production.ad_calls
                where ad_unit_type = 'META' 
                        and ip_address_blacklisted = false 
                        and site_type = 'ORBITZ_GLOBAL'
                        and publisher_user_id <>  ''
        ) a2
        on a1.publisher_user_id = a2.publisher_user_id AND a1.traffic_share_type = a2.traffic_share_type
        where datediff(day,first_login_date,requested_at_date_in_et) between 0 and 120
        
       GROUP BY        a1.traffic_share_type,
       a1.publisher_user_id )   main
GROUP BY traffic_share_type