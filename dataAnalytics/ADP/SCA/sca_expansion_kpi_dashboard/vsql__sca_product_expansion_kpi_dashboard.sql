-- Query for KPI metrics

SELECT
	SUM(m.click_count) AS "Clicks",
	SUM(m.interaction_count) AS "Interactions",
	SUM(m.impression_count) AS "Impressions",
	SUM(m.gross_revenue_sum) AS "Gross Media Revenue",
	m.aggregation_level_date_in_et AS "Date",	
	(CASE
        WHEN m.aggregation_level_date_in_et >= '2014-08-31' then m.site_country
        else (case when au.name like '%UK%' then 'GB' when au.name like '%.ca%' then 'CA' else 'US' end)
    end) as "Country",
	m.browser AS "Browser",
	lpt.page_type AS "Page Type",
	CASE WHEN lpt.page_type = 'List page (web)' THEN SUM(m.impression_count) END AS "List Page Impressions",
	CASE WHEN lpt.page_type = 'Exit unit' THEN SUM(m.impression_count) END AS "Exit Unit Impressions",
	CASE WHEN lpt.page_type = 'Exit unit' AND SUM(m.impression_count) > 0 THEN 'True' ELSE 'False' END AS "Supports Exit Units",
	CASE WHEN lpt.page_type = 'Exit unit' THEN m.browser END AS "Exit Unit Browser",
	'Flights' AS "Product Category Type"
FROM
	intent_media_production.air_ct_media_performance_aggregations m
LEFT JOIN
	intent_media_production.ad_units au
ON
	m.ad_unit_id = au.id
LEFT JOIN
	intent_media_production.legal_page_types lpt
ON
	au.legal_page_type_id = lpt.id
	AND au.ad_type = 'CT'
WHERE
	m.aggregation_level_date_in_et BETWEEN DATE(CURRENT_TIMESTAMP at timezone 'EST')-INTERVAL '1 YEAR' AND DATE(CURRENT_TIMESTAMP at timezone 'EST')-INTERVAL '1 DAY'
GROUP BY
	"Date",
	"Country",
	"Browser",
	"Page Type"
UNION

SELECT
	SUM(m.click_count) AS "Clicks",
	SUM(m.interaction_count) AS "Interactions",
	SUM(m.impression_count) AS "Impressions",
	SUM(m.gross_revenue_sum) AS "Gross Media Revenue",
	m.aggregation_level_date_in_et AS "Date",	
	(CASE
        WHEN m.aggregation_level_date_in_et >= '2014-08-31' then m.site_country
        else (case when au.name like '%UK%' then 'GB' when au.name like '%.ca%' then 'CA' else 'US' end)
    end) as "Country",
    m.browser AS "Browser",
	lpt.page_type AS "Page Type",
	CASE WHEN lpt.page_type = 'List page (web)' THEN SUM(m.impression_count) END AS "List Page Impressions",
	CASE WHEN lpt.page_type = 'Exit unit' THEN SUM(m.impression_count) END AS "Exit Unit Impressions",
	CASE WHEN lpt.page_type = 'Exit unit' AND SUM(m.impression_count) > 0 THEN 'True' ELSE 'False' END AS "Supports Exit Units",
	CASE WHEN lpt.page_type = 'Exit unit' THEN m.browser END AS "Exit Unit Browser",
	'Hotels' AS "Product Category Type"
FROM
	intent_media_production.hotel_ct_media_performance_aggregations m
LEFT JOIN
	intent_media_production.ad_units au
ON
	m.ad_unit_id = au.id
LEFT JOIN
	intent_media_production.legal_page_types lpt
ON
	au.legal_page_type_id = lpt.id
	AND au.ad_type = 'CT'
WHERE
	m.aggregation_level_date_in_et BETWEEN DATE(CURRENT_TIMESTAMP at timezone 'EST')-INTERVAL '1 YEAR' AND DATE(CURRENT_TIMESTAMP at timezone 'EST')-INTERVAL '1 DAY' 
GROUP BY
	"Date",
	"Country",
	"Browser",
	"Page Type"
;








-- Query for placement type metrics



SELECT
	SUM(p.actual_cpc_sum) AS "Gross Media Revenue",
	p.date_in_et AS "Date",
	p.placement_type AS "Placement Type",
	s.display_name AS "Site",
	'Flights' AS "Product Category Type"
FROM
	intent_media_production.air_ct_placement_type_performance_aggregations p
LEFT JOIN
	intent_media_production.ad_units au
ON
	p.ad_unit_id = au.id
	AND au.ad_type = 'CT'
LEFT JOIN
	intent_media_production.sites s
ON
	au.site_id = s.id
WHERE
	p.date_in_et BETWEEN DATE(CURRENT_TIMESTAMP at timezone 'EST')-INTERVAL '1 YEAR' AND DATE(CURRENT_TIMESTAMP at timezone 'EST')-INTERVAL '1 DAY' 
GROUP BY
	"Date",
	"Placement Type",
	"Site"

UNION

SELECT
	SUM(p.actual_cpc_sum) AS "Gross Media Revenue",
	p.date_in_et AS "Date",
	p.placement_type AS "Placement Type",
	s.display_name AS "Site",
	'Hotels' AS "Product Category Type"
FROM
	intent_media_production.hotel_ct_placement_type_performance_aggregations p
LEFT JOIN
	intent_media_production.ad_units au
ON
	p.ad_unit_id = au.id
	AND au.ad_type = 'CT'
LEFT JOIN
	intent_media_production.sites s
ON
	au.site_id = s.id
WHERE
	p.date_in_et BETWEEN DATE(CURRENT_TIMESTAMP at timezone 'EST')-INTERVAL '1 YEAR' AND DATE(CURRENT_TIMESTAMP at timezone 'EST')-INTERVAL '1 DAY' 
GROUP BY
	"Date",
	"Placement Type",
	"Site"
;


-- Query for exit unit browser metrics

SELECT
	COUNT(CASE WHEN ac.ad_unit_page_type_id = 2 THEN ac.request_id END) AS "Count of Ad Calls (List Page)",
	COUNT(CASE WHEN ac.ad_unit_page_type_id = 5 THEN ac.request_id END) AS "Count of Ad Calls (Exit Unit)",
	ac.requested_at_date_in_et AS "Date",
	CASE 
		WHEN regexp_instr(ac.user_agent,E'.*Chrome\/(4[1-9]).*') > 0 THEN 'CHROME41-49'
		WHEN regexp_instr(ac.user_agent,E'..*iPad.*Version\/8.*') > 0 THEN 'iPad iOS 8'
		WHEN regexp_instr(ac.user_agent,E'..*iPad.*') > 0 AND regexp_instr(ac.user_agent,E'.*Version\/8.*') = 0 THEN 'iPad iOS 7-'
		WHEN regexp_instr(ac.user_agent,E'..*iPhone.*') > 0 THEN 'iPhone'
		WHEN regexp_instr(ac.user_agent,E'..*Macintosh.*Version\/6.[1-9].*') > 0 THEN 'Safari 6.1+'		
		ELSE ac.browser 
	END as "New Browser",
	ac.browser_family as "Browser Family"
FROM intent_media_log_data_production.ad_calls ac
WHERE
	ac.ad_unit_type ='CT'
	AND ac.ip_address_blacklisted = 0
	AND ac.requested_at_date_in_et >= '2015-01-01'
	AND ac.outcome_type = 'SERVED'
GROUP BY 
	"Browser Family",
	"New Browser",
	"Date"
