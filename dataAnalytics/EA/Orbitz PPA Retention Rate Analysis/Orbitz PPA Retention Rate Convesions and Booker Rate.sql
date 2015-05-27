
--Conversion Rate

SELECT traffic_share_type,  count(DISTINCT publisher_user_id) as users, SUM(conversions) as conversions,  SUM(booker) as Bookers, avg(booker) as booker_rate, SUM(conversions)/SUM(Booker) as conversions_per_booker,  
       avg(conversions) as avg_conversions_per_user, 
       SUM(total_conversion_value)/SUM(Booker) as total_order_value_per_booker,
       SUM(total_conversion_value)/SUM(conversions) as average_order_value,
       SUM(total_conversion_value)/ count(DISTINCT publisher_user_id)  as conversions_dollars_per_user
       
FROM (
        SELECT traffic_share_type, a1.publisher_user_id, SUM(CASE  WHEN   a2.requested_at_in_et >= a1.first_login_date AND net_conversion_value > 1 THEN 1 ELSE 0 END) as conversions,
        CASE WHEN SUM(CASE  WHEN   a2.requested_at_in_et >= a1.first_login_date THEN net_conversion_value ELSE 0 END) > 0 THEN 1 ELSE 0 END AS Booker, 
        sum(CASE  WHEN   a2.requested_at_in_et >= a1.first_login_date AND net_conversion_value > 1 THEN net_conversion_value ELSE 0 END) as total_conversion_value
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
                                
                        and month(requested_at_date_in_et) in (1, 2)
                        and publisher_user_id <>  '' 
                   GROUP BY publisher_user_id, traffic_Share_type) a1
        
        LEFT JOIN 
        
                (SELECT DISTINCT publisher_user_id, net_conversion_value, requested_at_in_et
                FROM intent_media_log_data_production.conversions
                WHERE SITE_TYPE = 'ORBITZ_GLOBAL' AND entity_id = 55 AND ip_address_blacklisted = false
                AND product_category_type = 'HOTELS'
                ) a2
                
        ON a1.publisher_user_id = a2.publisher_user_id
        AND a2.requested_at_in_et >= a1.first_login_date
         ---AND datediff(day,first_login_date, a2.requested_at_in_et) between 0 and 120
        GROUP BY traffic_share_type, a1.publisher_user_id ) main
GROUP BY traffic_share_Type





--Booker Rate T Test


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
                                
                                and month(requested_at_date_in_et) in (1, 2)
                                and publisher_user_id <>  '' 
                   GROUP BY publisher_user_id, traffic_Share_type) a1
        
        LEFT JOIN 
        
                (SELECT DISTINCT publisher_user_id, net_conversion_value, requested_at_in_et
                FROM intent_media_log_data_production.conversions
                WHERE SITE_TYPE = 'ORBITZ_GLOBAL' AND entity_id = 55 AND ip_address_blacklisted = false
                AND product_category_type = 'HOTELS'
                ) a2
                
        ON a1.publisher_user_id = a2.publisher_user_id
        AND a2.requested_at_in_et >= a1.first_login_date
         ---AND datediff(day,first_login_date, a2.requested_at_in_et) between 0 and 120
        GROUP BY traffic_share_type, a1.publisher_user_id 
        





--Conversion Per User T Test

            SELECT traffic_share_type, a1.publisher_user_id, SUM(CASE  WHEN   net_conversion_value > 1 THEN 1 ELSE 0 END) as conversions_per_user
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
                                    
                                    -----and month(requested_at_date_in_et) = 11
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




--Conversions Per Booker
        
SELECT traffic_share_type, avg(conversions_per_booker) , COUNT(DISTINCT PUBLISHER_USER_ID) as users
FROM (
 
             SELECT traffic_share_type, publisher_user_id, conversions/Booker as conversions_per_booker

            FROM (
                    SELECT traffic_share_type, a1.publisher_user_id, SUM(CASE  WHEN  net_conversion_value > 1 THEN 1 ELSE 0 END) as conversions,
                    CASE WHEN SUM(CASE  WHEN  net_conversion_value > 1 THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END AS Booker, 
                    ---sum(CASE  WHEN  net_conversion_value > 1 THEN net_conversion_value ELSE 0 END) as total_conversion_value
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
                                            
                                            --and month(requested_at_date_in_et) = 11
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
                    GROUP BY traffic_share_type, a1.publisher_user_id ) main
            WHERE BOoker = 1
        
 ) main
 GROUP BY traffic_share_type
        


--#Conversions BY percentage of users

SELECT traffic_share_type, conversions, COUNT(DISTINCT publisher_user_id)
       
FROM (
        SELECT traffic_share_type, a1.publisher_user_id, SUM(CASE  WHEN   a2.requested_at_in_et >= a1.first_login_date AND net_conversion_value > 1 THEN 1 ELSE 0 END) as conversions
     
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
        GROUP BY traffic_share_type, a1.publisher_user_id ) main
GROUP BY traffic_share_Type, conversions
ORDER BY traffic_share_Type, conversions

        