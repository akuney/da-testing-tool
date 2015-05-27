
/*MySQL tables - Publisher Hotel Cities*/




SELECT
t1.*, t2."Count of Hotel City ID"
FROM
(
SELECT 
	CASE 
	WHEN  ifnull(trim(trailing '"' from regexp_substr(ac.multivariate_test_attributes_variable,'"HOTEL_MARKET_MATCHING_TYPE":"(.*?")[,}]', 1, 1, '', 1)), 'Not Found') = 'dynamic_algorithm' THEN 'Dynamic Algorithm'
        ELSE 'Static Mapping'
        END
	AS "Market Matching Type",
	ac.site_type AS "Publisher Site",
	ac.requested_at_date_in_et AS "Date",
	ac.hotel_city_id as "Hotel City ID (IM Market)",
	hc.name AS "Hotel City Name (IM Market)",
	uri_percent_decode(regexp_substr(ac.request_url,'hotel_city_name=(.*?)&',1,1,'',1)) AS "Hotel City Name (Request URL)",
	regexp_substr(ac.request_url,'hotel_state_code=(.*?)&',1,1,'',1) AS "Hotel State Code (Request URL)",
	regexp_substr(ac.request_url,'hotel_country_code=(.*?)&',1,1,'',1) AS "Hotel Country Code (Request URL)",
	count(DISTINCT ac.request_id) as "Total Ad Calls",
	count (DISTINCT CASE WHEN ac.outcome_type = 'SERVED' THEN ac.request_id END) as "Served Ad Calls",
	count(c.request_id) AS "Clicks"
FROM
	intent_media_log_data_production.ad_calls ac
LEFT JOIN 
	intent_media_production.hotel_cities hc
ON 
	ac.hotel_city_id = hc.id
LEFT join
	intent_media_log_data_production.clicks c 
ON 
	c.ad_call_request_id = ac.request_id
	and c.fraudulent = 0
	and c.ip_address_blacklisted = 0
	AND c.requested_at_date_in_et between '2015-01-22' and DATE(CURRENT_TIMESTAMP at timezone 'EST')-INTERVAL '1 DAY' 
WHERE 
	ac.ip_address_blacklisted = 0
	AND ac.requested_at_date_in_et between '2015-01-22' AND DATE(CURRENT_TIMESTAMP at timezone 'EST')-INTERVAL '1 DAY' 
	AND ac.product_category_type = 'HOTELS'
	AND ac.ad_unit_type = 'CT'
	AND ac.hotel_city_id IS NOT NULL
GROUP BY 
	"Market Matching Type",
	"Publisher Site",
	"Date",
	"Hotel City ID (IM Market)",
	"Hotel City Name (IM Market)",
	"Hotel City Name (Request URL)",
	"Hotel State Code (Request URL)",
	"Hotel Country Code (Request URL)"
) t1
LEFT JOIN
(
SELECT 
	ac.site_type AS "Publisher Site",
	uri_percent_decode(regexp_substr(ac.request_url,'hotel_city_name=(.*?)&',1,1,'',1)) AS "Hotel City Name (Request URL)",
	regexp_substr(request_url,'hotel_state_code=(.*?)&',1,1,'',1) AS "Hotel State Code (Request URL)",
	regexp_substr(request_url,'hotel_country_code=(.*?)&',1,1,'',1) AS "Hotel Country Code (Request URL)",
	count(DISTINCT ac.hotel_city_id) as "Count of Hotel City ID",
	CASE WHEN count(DISTINCT ac.hotel_city_id) > 1 THEN 'True' ELSE 'FALSE' END AS "Multiple Markets"
FROM
	intent_media_log_data_production.ad_calls ac
WHERE 
	ac.ip_address_blacklisted = 0
	AND ac.requested_at_date_in_et between '2015-01-22' AND DATE(CURRENT_TIMESTAMP at timezone 'EST')-INTERVAL '1 DAY' 
	AND ac.product_category_type = 'HOTELS'
	AND ac.ad_unit_type = 'CT'
	AND ac.hotel_city_id IS NOT NULL
GROUP BY 
	"Publisher Site",
	"Hotel City Name (Request URL)",
	"Hotel State Code (Request URL)",
	"Hotel Country Code (Request URL)"
) t2
on 
	t1."Hotel City Name (Request URL)" = t2."Hotel City Name (Request URL)"
	AND t1."Hotel Country Code (Request URL)" = t2."Hotel Country Code (Request URL)"
	AND t1."Hotel State Code (Request URL)" = t2."Hotel State Code (Request URL)"
	AND t1."Publisher Site" = t2."Publisher Site"
;


