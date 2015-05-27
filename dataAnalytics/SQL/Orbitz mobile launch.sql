-- Media.

SELECT ac.traffic_share_type,
	count(ac.request_id) AS ad_calls,
	sum(CASE WHEN ac.outcome_type = 'SERVED' THEN 1 ELSE 0 END) AS pages_available,
	sum(CASE WHEN ac.positions_filled >0 THEN 1 ELSE 0 END) AS pages_served,
	sum(ac.positions_filled) AS impressions,
	sum(cl.clicks) AS clicks,
	sum(cl.click_revenue) AS gross_media_revenue,
	sum(cl.click_revenue*.752) AS net_media_revenue
FROM intent_media_log_data_production.ad_calls ac
LEFT JOIN (

SELECT ad_call_request_id,
	count(request_id) AS clicks,
	sum(actual_cpc) AS click_revenue
FROM intent_media_log_data_production.clicks
WHERE requested_at_date_in_et between '2014-02-24' and '2014-04-21'
	AND ip_address_blacklisted = 0
	AND fraudulent = 0
	AND site_type = 'ORBITZ_GLOBAL'
	AND product_category_type = 'HOTELS'
GROUP BY ad_call_request_id

) cl ON (ac.request_id = cl.ad_call_request_id)
WHERE ac.requested_at_date_in_et between '2014-02-24' and '2014-04-21'
	AND ac.ip_address_blacklisted = 0
	AND ac.ad_unit_id = 124
GROUP BY ac.traffic_share_type
ORDER BY ac.traffic_share_type desc
;

-- Bookers.

select
uvs.traffic_share_type,
count(uvs.publisher_user_id) as visitors,
sum(case when conversions.publisher_user_id is not null then 1 else 0 end) as bookers,
sum(conversions.conversions) AS conversions,
sum(conversions.conversions*conversions.conversions) AS conversions_2_u,
sum(conversions.conversion_value) as conversion_value,
sum(conversions.conversion_value*conversions.conversion_value) as conversion_value_2_u
from
	(select 
	publisher_user_id,
	traffic_share_type,
	min(requested_at) as first_ad_call_time
	from 
	intent_media_log_data_production.ad_calls 
	where 
	ad_unit_id = 124 
	and requested_at_date_in_et between '2014-02-24' and '2014-04-21'
	and ip_address_blacklisted = 0
	group by 1,2) as uvs
	left join
	(select
	c.publisher_user_id,
	count(c.order_id) AS conversions,
	sum(c.conversion_value) AS conversion_value
	from
	intent_media_log_data_production.conversions c
	inner join (select entity_id, order_id, min(requested_at) AS min_requested_at from intent_media_log_data_production.conversions where site_type = 'ORBITZ_GLOBAL' and entity_id = 55 and requested_at_date_in_et between '2014-02-24' and '2014-04-21' and product_category_type = 'HOTELS' and ip_address_blacklisted = 0 group by entity_id, order_id) order_min_req_at ON (c.entity_id = order_min_req_at.entity_id and c.order_id = order_min_req_at.order_id and c.requested_at = order_min_req_at.min_requested_at)
	inner join (select publisher_user_id, min(requested_at) as first_ad_call_time from intent_media_log_data_production.ad_calls where ad_unit_id = 124 and requested_at_date_in_et between '2014-02-24' and '2014-04-21' and ip_address_blacklisted = 0 group by 1) ac_min_req_at ON (c.publisher_user_id = ac_min_req_at.publisher_user_id and c.requested_at > ac_min_req_at.first_ad_call_time)
	where
	c.site_type = 'ORBITZ_GLOBAL'
	and c.entity_id = 55
	and c.requested_at_date_in_et between '2014-02-24' and '2014-04-21'
	and c.product_category_type = 'HOTELS'
	and c.ip_address_blacklisted = 0
	group by 1) as
conversions
on uvs.publisher_user_id = conversions.publisher_user_id
group by 1
order by 1 desc;


-- Bookers (not deduped).

select
traffic_share_type,
count(*) as visitors,
sum(is_booker) as bookers,
sum(total_conversions) AS conversions,
sum(total_conversion_value) as gross_profit
from
(select
uvs.publisher_user_id as publisher_user_id,
uvs.traffic_share_type as traffic_share_type,
max(case 
	when conversions.conversion_value > 0 then 1 
	else 0
	end) as is_booker,
count(conversions.conversion_value) AS total_conversions,
sum(conversions.conversion_value) as total_conversion_value	
from
	(select 
	publisher_user_id,
	traffic_share_type,
	min(requested_at) as first_ad_call_time
	from 
	intent_media_log_data_production.ad_calls 
	where 
	ad_unit_id = 124
	and requested_at_date_in_et between '2014-02-24' and '2014-04-21'
	and ip_address_blacklisted = 0
	group by 1,2) as 
uvs left join
	(select
	publisher_user_id,
	conversion_value,
	requested_at
	from
	intent_media_log_data_production.conversions
	where
	site_type = 'ORBITZ_GLOBAL'
	and entity_id = 55
	and requested_at_date_in_et between '2014-02-24' and '2014-04-21'
	and product_category_type = 'HOTELS'
	and ip_address_blacklisted = 0) as
conversions
on uvs.publisher_user_id = conversions.publisher_user_id
and uvs.first_ad_call_time < conversions.requested_at
group by 1,2) as 
pulled_data
group by 1
order by 1 desc;
