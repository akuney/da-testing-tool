
---- Query to get Percentage of repeats based on original month
select 
	datediff(day,first_login_date,requested_at_date_in_et),
	count (distinct a1.publisher_user_id),
	traffic_share_type
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
        and month(requested_at_date_in_et) = 11
group by publisher_user_id, traffic_share_type
) a1
left join 
(
select
	publisher_user_id,
	requested_at_date_in_et
from intent_media_log_data_production.ad_calls
where ad_unit_type = 'META' 
        and ip_address_blacklisted = false 
        and site_type = 'ORBITZ_GLOBAL'
) a2
on a1.publisher_user_id = a2.publisher_user_id 
where datediff(day,first_login_date,requested_at_date_in_et) between 0 and 120
group by datediff(day,first_login_date,requested_at_date_in_et), traffic_share_type
ORDER BY datediff(day,first_login_date,requested_at_date_in_et), traffic_share_type




---- Query to get average number of repeat visits
SELECT traffic_share_type, avg(number_of_visit_days) as avg_number_of_vist_days, COUNT(distinct publisher_user_id) as User_IDs
FROM (
        select 
                CAST(a1.traffic_share_type as varchar(20)),
                CAST(a1.publisher_user_id  as varchar(30)),
                cast(COUNT(DISTINCT datediff(day,first_login_date,requested_at_date_in_et)) as float) as number_of_visit_days
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
                and month(requested_at_date_in_et) = 11
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
        where datediff(day,first_login_date,requested_at_date_in_et) between 0 and 120
        ---AND datediff(day,first_login_date,requested_at_date_in_et) >= 1    ----Include this to limit to revists only
        group by a1.traffic_share_type, a1.publisher_user_id
        ) main
GROUP BY traffic_share_type






---- Total number of types
SELECT SUM(case WHEN share_type = 1 THEN 1 ELSE 0 END) as count_of_single_types,
        SUM(case WHEN share_type > 1 THEN 1 ELSE 0 END) as count_of_multiple_types
FROM
        (select  publisher_user_id, count(DISTINCT traffic_share_type) as share_type
        from intent_media_log_data_production.ad_calls
        where ad_unit_type = 'META' 
                and ip_address_blacklisted = false 
                and site_type = 'ORBITZ_GLOBAL'
        GROUP BY publisher_user_id ) tot


---- Query without dirty users
SELECT  traffic_share_type, count(DISTINCT publisher_user_id) as users
FROM  intent_media_log_data_production.ad_calls
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
GROUP BY traffic_share_type


---% of  traffic share over time

SELECT  requested_at_date_in_et, traffic_share_type, count(*)
FROM  intent_media_log_data_production.ad_calls
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
GROUP BY requested_at_date_in_et, traffic_share_type