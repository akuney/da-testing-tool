/****
Queries below for MVT expirement - dashboard query
****/
/* All funnel metrics for market mapping split test */
SELECT 
	CASE 
		WHEN ifnull(trim(trailing '"' from regexp_substr(ac.multivariate_test_attributes_variable,'"HOTEL_MARKET_MATCHING_TYPE":"(.*?")[,}]', 1, 1, '', 1)), 'Not Found') = 'dynamic_algorithm' THEN 'Dynamic Algorithm'
        WHEN ifnull(trim(trailing '"' from regexp_substr(ac.multivariate_test_attributes_variable,'"HOTEL_MARKET_MATCHING_TYPE":"(.*?")[,}]', 1, 1, '', 1)), 'Not Found') = 'static_mapping' THEN 'Static Mapping'
        END
	AS "Market Matching Type",
	ac.site_type AS "Publisher Site",
	ac.requested_at_date_in_et AS "Date",
	ac.outcome_type AS "Outcome Type",
	ac.hotel_city_id as "Hotel City ID (IM Market)",
	au.name as "Ad Unit",
	hc.name AS "Hotel City Name (IM Market)",
	uri_percent_decode(regexp_substr(ac.request_url,'hotel_city_name=(.*?)&',1,1,'',1)) as "Hotel City Name (Request URL)",
	count(DISTINCT ac.request_id) as "Total Ad Calls",
	count (DISTINCT CASE WHEN ac.outcome_type = 'SERVED' THEN ac.request_id END) as "Served Ad Calls",
	count (DISTINCT CASE WHEN (ac.outcome_type = 'SERVED' AND ac.hotel_city_id is null) THEN ac.request_id END) as "Long Tail Ad Calls",
	count(c.request_id) AS "Clicks",
	sum(c.actual_cpc) AS "Revenue",
	count(distinct(c.ad_call_request_id)) AS "Interactions"
FROM
	intent_media_log_data_production.ad_calls ac
LEFT join 
	intent_media_production.hotel_cities hc
on 
	ac.hotel_city_id = hc.id
LEFT join
	intent_media_log_data_production.clicks c 
ON 
	c.ad_call_request_id = ac.request_id
	and c.fraudulent = 0
	and c.ip_address_blacklisted = 0
	AND c.requested_at_date_in_et between '2015-01-22' and DATE(CURRENT_TIMESTAMP at timezone 'EST')-INTERVAL '1 DAY' 
LEFT JOIN
	intent_media_production.ad_units au
ON
	ac.ad_unit_id = au.id
	AND au.active = 1
	AND au.ad_type = 'CT'
WHERE 
	ac.ip_address_blacklisted = 0
	and ac.requested_at_date_in_et between '2015-01-22' and DATE(CURRENT_TIMESTAMP at timezone 'EST')-INTERVAL '1 DAY' 
	and ac.product_category_type = 'HOTELS'
	and ac.ad_unit_type = 'CT'
GROUP BY 
	"Market Matching Type",
	"Publisher Site",
	"Date",
	"Outcome Type",
	"Hotel City ID (IM Market)",
	"Ad Unit",
	"Hotel City Name (IM Market)",
	"Hotel City Name (Request URL)"
;
