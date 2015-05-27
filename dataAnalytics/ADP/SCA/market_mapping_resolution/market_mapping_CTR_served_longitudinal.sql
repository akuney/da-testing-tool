/*Daily longitudinal view of CTR and Served Ad Calls by Intent Media hotel market across Publishers*/

select 
	CASE WHEN 
		split_part(split_part(ac.multivariate_test_attributes,'"HOTEL_MARKET_MATCHING_TYPE":"',2),'"',1)='dynamic_algorithm' THEN 'Dynamic Algorithm'
		WHEN split_part(split_part(ac.multivariate_test_attributes,'"HOTEL_MARKET_MATCHING_TYPE":"',2),'"',1)='static_mapping' THEN 'Static Mapping'
		END
	as "Market Matching Type", 
	ac.requested_at_date_in_et as "Date",
	ac.hotel_city_id as "Hotel City ID (IM Market)", 
	hc.name "Hotel City Name (IM Market)", 
	ac.site_type as "Publisher Site",
	count(ac.request_id) as "Ad calls", 
	count(c.request_id) as "Clicks"
FROM
	intent_media_log_data_production.ad_calls ac
	left join intent_media_log_data_production.clicks c on ac.request_id = c.ad_call_request_id
	inner join intent_media_production.hotel_cities hc on ac.hotel_city_id = hc.id
WHERE 
	ac.ip_address_blacklisted = 0
	and ac.requested_at_date_in_et between '2015-01-22' and DATE(CURRENT_TIMESTAMP at timezone 'EST')-INTERVAL '1 DAY'
	and ac.product_category_type = 'HOTELS'
	and ac.outcome_type = 'SERVED'
	and ac.ad_unit_type = 'CT'
group by 1,2,3,4,5
order by 6 desc
;