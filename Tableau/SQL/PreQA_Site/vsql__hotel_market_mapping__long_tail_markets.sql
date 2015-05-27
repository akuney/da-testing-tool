SELECT 
	ac.requested_at_date_in_et as "Date",
	ac.site_type as "Publisher Site",
	uri_percent_decode(regexp_substr(ac.request_url,'hotel_city_name=(.*?)&',1,1,'',1)) AS "Hotel City Name (Request URL)",
	regexp_substr(ac.request_url,'hotel_state_code=(.*?)&',1,1,'',1) AS "Hotel State Code (Request URL)",
	regexp_substr(ac.request_url,'hotel_country_code=(.*?)&',1,1,'',1) AS "Hotel Country Code (Request URL)",
	au.name as "Ad Unit",
	count(ac.request_id) as "Ad Calls"
FROM 
	intent_media_log_data_production.ad_calls ac
LEFT JOIN
	intent_media_production.ad_units au
ON
	ac.ad_unit_id = au.id
	AND au.active = 1
	AND au.ad_type = 'CT'
WHERE 
	ac.requested_at_date_in_et between '2015-01-22' and DATE(CURRENT_TIMESTAMP at timezone 'EST')-INTERVAL '1 DAY' 
	and ac.ip_address_blacklisted = 0
	and ac.product_category_type = 'HOTELS'
	and ac.ad_unit_type = 'CT'
	and ifnull(trim(trailing '"' from regexp_substr(ac.multivariate_test_attributes_variable,'"HOTEL_MARKET_MATCHING_TYPE":"(.*?")[,}]', 1, 1, '', 1)), 'Not Found') = 'dynamic_algorithm'
	and ac.hotel_city_id is null
GROUP BY "Date", "Publisher Site", "Hotel City Name (Request URL)", "Hotel State Code (Request URL)", "Hotel Country Code (Request URL)", "Ad Unit"
ORDER BY "Ad Calls" desc
;