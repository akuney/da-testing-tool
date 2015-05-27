# Browser eCPM comparison

SELECT 
	ac.requested_at_date_in_et AS "Date",
	ac.browser as "Browser",
	ac.browser_family as "Browser Family",
	ac.site_type AS "Publisher Site",
	ac.product_category_type AS "Product Type",
	lpt.page_type AS "Page Type",
	count(DISTINCT ac.request_id) as "Total Ad Calls",
	count (DISTINCT CASE WHEN ac.outcome_type = 'SERVED' THEN ac.request_id END) as "Served Ad Calls",
	count(c.request_id) AS "Clicks",
	sum(c.actual_cpc) AS "Revenue",
	count(DISTINCT(c.ad_call_request_id)) AS "Interactions"
FROM
	intent_media_log_data_production.ad_calls ac
LEFT JOIN
	intent_media_log_data_production.clicks c 
ON 
	c.ad_call_request_id = ac.request_id
	and c.fraudulent = 0
	and c.ip_address_blacklisted = 0
	AND c.requested_at_date_in_et between '2015-01-15' and DATE(CURRENT_TIMESTAMP at timezone 'EST')-INTERVAL '1 DAY' 
LEFT JOIN
	intent_media_production.ad_units au
ON
	ac.ad_unit_id = au.id
LEFT JOIN
	intent_media_production.legal_page_types lpt
ON
	au.legal_page_type_id = lpt.id
WHERE 
	ac.ip_address_blacklisted = 0
	and ac.requested_at_date_in_et between '2015-01-15' and DATE(CURRENT_TIMESTAMP at timezone 'EST')-INTERVAL '1 DAY' 
	and ac.ad_unit_type = 'CT'
GROUP BY
	ac.requested_at_date_in_et,ac.browser,ac.browser_family,ac.site_type, ac.product_category_type, lpt.page_type
;