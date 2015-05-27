/****
Queries below for MVT expirement
****/

/*Total Ad calls by MVT expirement*/
select 
	split_part(split_part(multivariate_test_attributes,'"HOTEL_MARKET_MATCHING_TYPE":"',2),'"',1) AS "Market Matching Type", 
	count(request_id) AS "Total Ad Calls"     
FROM
	intent_media_log_data_production.ad_calls
WHERE 
	ip_address_blacklisted = 0
	and requested_at_date_in_et between '2015-01-22' and DATE(CURRENT_TIMESTAMP at timezone 'EST')-INTERVAL '1 DAY' 
	and site_type = 'TVLY'
	and product_category_type = 'HOTELS'
	and ad_unit_type = 'CT'
group by 1
order by 1
;

/*Served Ad calls by MVT expirement*/
select 
	split_part(split_part(multivariate_test_attributes,'"HOTEL_MARKET_MATCHING_TYPE":"',2),'"',1) AS "Market Matching Type", 
	count(request_id) AS "Served Ad Calls"  
FROM
	intent_media_log_data_production.ad_calls
WHERE 
	ip_address_blacklisted = 0
	and requested_at_date_in_et between '2015-01-22' AND DATE(CURRENT_TIMESTAMP at timezone 'EST')-INTERVAL '1 DAY' 
	and site_type = 'TVLY'
	and product_category_type = 'HOTELS'
	and outcome_type = 'SERVED'
	and ad_unit_type = 'CT'
group by 1
order by 1
;

/*Long Tail Ad calls by MVT expirement*/
select 
	split_part(split_part(multivariate_test_attributes,'"HOTEL_MARKET_MATCHING_TYPE":"',2),'"',1) AS "Market Matching Type", 
	count(request_id) AS "Long Tail Ad Calls"
FROM
	intent_media_log_data_production.ad_calls
WHERE 
	ip_address_blacklisted = 0
	and requested_at_date_in_et between '2015-01-22' AND DATE(CURRENT_TIMESTAMP at timezone 'EST')-INTERVAL '1 DAY' 
	and site_type = 'TVLY'
	and product_category_type = 'HOTELS'
	and outcome_type = 'SERVED'
	and ad_unit_type = 'CT'
	and hotel_city_id is null
group by 1
order by 1
;

/* Clicks by MVT expirement */
select 
	split_part(split_part(ac.multivariate_test_attributes,'"HOTEL_MARKET_MATCHING_TYPE":"',2),'"',1) AS "Market Matching Type", 
	count(c.request_id) AS "Clicks"
from 
	intent_media_log_data_production.clicks c
left join 
	intent_media_log_data_production.ad_calls ac on c.ad_call_request_id = ac.request_id
where 
	c.requested_at_date_in_et between '2015-01-22' AND DATE(CURRENT_TIMESTAMP at timezone 'EST')-INTERVAL '1 DAY'  
	and ac.requested_at_date_in_et between '2015-01-22' AND DATE(CURRENT_TIMESTAMP at timezone 'EST')-INTERVAL '1 DAY' 
	and c.fraudulent = 0
	and c.ip_address_blacklisted = 0
	AND ac.ip_address_blacklisted = 0
	AND ac.ad_unit_type = 'CT'
	and ac.site_type = 'TVLY'
	and ac.product_category_type = 'HOTELS'
	and ac.outcome_type = 'SERVED'
GROUP BY 1
order by 1
;

/* Revenue and Average CPC by MVT expirement */
select 
	split_part(split_part(ac.multivariate_test_attributes,'"HOTEL_MARKET_MATCHING_TYPE":"',2),'"',1) AS "Market Matching Type", 
	sum(c.actual_cpc) AS "Revenue", 
	avg(c.actual_cpc) AS "Average CPC"
from 
	intent_media_log_data_production.clicks c
	left join intent_media_log_data_production.ad_calls ac on c.ad_call_request_id = ac.request_id
where 
	c.requested_at_date_in_et between '2015-01-22' AND DATE(CURRENT_TIMESTAMP at timezone 'EST')-INTERVAL '1 DAY' 
	and ac.requested_at_date_in_et between '2015-01-22' AND DATE(CURRENT_TIMESTAMP at timezone 'EST')-INTERVAL '1 DAY' 
	and c.fraudulent = 0
	and c.ip_address_blacklisted = 0
	AND ac.ip_address_blacklisted = 0
	AND ac.ad_unit_type = 'CT'
	and ac.site_type = 'TVLY'
	and ac.product_category_type = 'HOTELS'
	and ac.outcome_type = 'SERVED'
GROUP BY 1
order by 1
;

/* Interactions by MVT expirement */
select 
	split_part(split_part(ac.multivariate_test_attributes,'"HOTEL_MARKET_MATCHING_TYPE":"',2),'"',1) AS "Market Matching Type", 
	count(distinct(c.ad_call_request_id)) AS "Interactions"
from 
	intent_media_log_data_production.clicks c
	left join intent_media_log_data_production.ad_calls ac on c.ad_call_request_id = ac.request_id
where 
	c.requested_at_date_in_et between '2015-01-22' AND DATE(CURRENT_TIMESTAMP at timezone 'EST')-INTERVAL '1 DAY' 
	and ac.requested_at_date_in_et between '2015-01-22' AND DATE(CURRENT_TIMESTAMP at timezone 'EST')-INTERVAL '1 DAY' 
	and c.fraudulent = 0
	and c.ip_address_blacklisted = 0
	AND ac.ip_address_blacklisted = 0
	AND ac.ad_unit_type = 'CT'
	and ac.site_type = 'TVLY'
	and ac.product_category_type = 'HOTELS'
	and ac.outcome_type = 'SERVED'
GROUP BY 1
order by 1
;



/****
Additional queries below
****/


select 
	split_part(split_part(multivariate_test_attributes,'"HOTEL_MARKET_MATCHING_TYPE":"',2),'"',1) AS "Market Matching Type", 
	outcome_type, 
	count(request_id)   
FROM
intent_media_log_data_production.ad_calls
WHERE 
	ip_address_blacklisted = 0
	and requested_at_date_in_et between '2015-01-22' AND DATE(CURRENT_TIMESTAMP at timezone 'EST')-INTERVAL '1 DAY' 
	and site_type = 'TVLY'
	and product_category_type = 'HOTELS'
	and ad_unit_type = 'CT'
group by 1,2
order by 1,2
;

/*Distribution of traffic by Intent Media Market*/
select 
	split_part(split_part(ac.multivariate_test_attributes,'"HOTEL_MARKET_MATCHING_TYPE":"',2),'"',1) AS "Market Matching Type", 
	ac.hotel_city_id, 
	hc.name, 
	count(request_id)   
FROM
	intent_media_log_data_production.ad_calls ac
	left join intent_media_production.hotel_cities hc on ac.hotel_city_id = hc.id
WHERE 
	ac.ip_address_blacklisted = 0
	and ac.requested_at_date_in_et between '2015-01-22' AND DATE(CURRENT_TIMESTAMP at timezone 'EST')-INTERVAL '1 DAY' 
	and ac.site_type = 'TVLY'
	and ac.product_category_type = 'HOTELS'
	and ac.outcome_type = 'SERVED'
	and ac.ad_unit_type = 'CT'
group by 1,2,3
order by 4 desc
;

/*Request errors by day*/
select 
	count(request_id),
	requested_at_date_in_et
from 
	intent_media_log_data_production.request_errors
where 
	requested_at_date_in_et between '2015-01-22' AND DATE(CURRENT_TIMESTAMP at timezone 'EST')-INTERVAL '1 DAY'
	and ip_address_blacklisted = 0
	and request_url like '%TVLY%'
	and request_url like '%hotels%'
group by 2
order by 2
;

/* Does have city id, but doesn't have combo of city state country / Add 'CT' for ad unit type*/

select 
	count(request_id)
FROM
	intent_media_log_data_production.ad_calls
WHERE 
	ip_address_blacklisted = 0
	and requested_at_date_in_et between '2015-01-22' AND DATE(CURRENT_TIMESTAMP at timezone 'EST')-INTERVAL '1 DAY' 
	and site_type = 'TVLY'
	and product_category_type = 'HOTELS'
	and outcome_type = 'SUPPRESSED_BY_UNKNOWN_HOTEL_DESTINATION'
	and request_url like '%hotel_city_name%'
	and request_url like '%hotel_state_code%'
	and request_url like '%hotel_country_code=%'
	and ad_unit_type = 'CT'
;

/* No valid layout -> no city id to match*/
select 
	hotel_city_id, 
	split_part(split_part(request_url,'hotel_country_code=',2),'&',1) as hotel_country_code, 
	count(request_id) 
FROM
	intent_media_log_data_production.ad_calls
WHERE 
	ip_address_blacklisted = 0
	and requested_at_date_in_et between '2015-01-22' AND DATE(CURRENT_TIMESTAMP at timezone 'EST')-INTERVAL '1 DAY'
	and product_category_type = 'HOTELS'
	and outcome_type = 'SUPPRESSED_BY_NO_VALID_LAYOUT'
	and ad_unit_type = 'CT'
group by 1,2
order by 3 desc
;

/*For each city state country what ad calls getting -> to improve dynamic algorithm*/
select 
	split_part(split_part(request_url,'&hotel_city_name=',2),'&',1) as hotel_city_name, 
	split_part(split_part(request_url,'&hotel_state_code=',2),'&',1) as hotel_state_code, 
	split_part(split_part(request_url,'&hotel_country_code=',2),'&',1) as hotel_country_code, 
	count(1) 
from 
	intent_media_log_data_production.ad_calls
where 
	requested_at_date_in_et between '2015-01-22' AND DATE(CURRENT_TIMESTAMP at timezone 'EST')-INTERVAL '1 DAY'
	and ip_address_blacklisted = 0
	and site_type = 'TVLY'
	and product_category_type = 'HOTELS'
	and multivariate_test_attributes_variable like '%dynamic_algorithm%'
	and ad_unit_type = 'CT'
	and hotel_city_id is null
group by 1, 2, 3
order by 4 desc
;

select 
	count(request_id) 
from 
	intent_media_log_data_production.ad_calls
where 
	requested_at_date_in_et between '2015-01-22' AND DATE(CURRENT_TIMESTAMP at timezone 'EST')-INTERVAL '1 DAY'
	and ip_address_blacklisted = 0
	and site_type = 'TVLY'
	and product_category_type = 'HOTELS'
	and multivariate_test_attributes_variable like '%dynamic_algorithm%'
	and ad_unit_type = 'CT'
	and hotel_city_id is null
group by 1, 2, 3
order by 4 desc
;




/****
Query below to run in MySQL until city_coordinates table in vertica
****/

/*  list of hotel_cities that 1. Do NOT have a match within the radius in city_coordinates with the same name, state and country 2. DO have at least one match on city state and country that is outside of the radius 3. Ordered by weight desc (showing the biggest markets at the top) and the distance (showing the best match) 
Note: Run in MySQL*/
SELECT 
	hc.search_weight, 
	greatest(0.00000001, ( 3959 * acos( cos( radians(cc.latitude) ) * cos( radians( hc.latitude ) ) * cos( radians( hc.longitude ) - radians(cc.longitude) ) + sin( radians(cc.latitude) ) * sin( radians( hc.latitude ) ) ) )) AS distance,
	hc.name, 
	cc.city_name, 
	hc.state, 
	c.code, 
	hc.longitude, 
	hc.latitude, 
	hc.radius, 
	hc.id
FROM 
	hotel_cities hc
	JOIN countries c ON hc.country_id = c.id
	JOIN city_coordinates cc ON hc.name = cc.city_name
WHERE 
	(cc.state_code IS NULL OR cc.state_code = hc.state) AND cc.country_code = c.code HAVING distance > hc.radius 
	AND hc.id NOT IN
		(SELECT matches.id FROM 
			(SELECT greatest(0.00000001, ( 3959 * acos( cos( radians(cc2.latitude) ) * cos( radians( hc2.latitude ) ) * cos( radians( hc2.longitude ) - radians(cc2.longitude) ) + sin( radians(cc2.latitude) ) * sin( radians( hc2.latitude ) ) ) )) AS distance2, hc2.id, hc2.radius FROM hotel_cities hc2
			JOIN countries c2 ON hc2.country_id = c2.id
			JOIN city_coordinates cc2 ON hc2.name = cc2.city_name
			WHERE 
				(cc2.state_code IS NULL OR cc2.state_code = hc2.state) 
				AND cc2.country_code = c2.code 
				HAVING distance2 < hc2.radius 
			) 
		AS matches)
ORDER BY hc.search_weight DESC, distance ASC
;

