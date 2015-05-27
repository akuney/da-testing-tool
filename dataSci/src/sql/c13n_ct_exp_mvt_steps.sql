DROP TABLE IF EXISTS intent_media_sandbox_production.exp_clicktab_mvt_u;
CREATE TABLE intent_media_sandbox_production.exp_clicktab_mvt_u AS
SELECT publisher_user_id,
	CASE INSTR(multivariate_test_attributes,'HOTEL_META_INTERCARD_DESIGN',1) WHEN 0 THEN 'Not_found' ELSE SUBSTR(multivariate_test_attributes,INSTR(multivariate_test_attributes,'HOTEL_META_INTERCARD_DESIGN',1)+LENGTH('HOTEL_META_INTERCARD_DESIGN')+3,CASE INSTR(multivariate_test_attributes,'","',INSTR(multivariate_test_attributes,'HOTEL_META_INTERCARD_DESIGN',1)) WHEN 0 THEN LENGTH(multivariate_test_attributes)-INSTR(multivariate_test_attributes,'HOTEL_META_INTERCARD_DESIGN',1)-LENGTH('CLICK_TYPE')-4 ELSE INSTR(multivariate_test_attributes,'","',INSTR(multivariate_test_attributes,'HOTEL_META_INTERCARD_DESIGN',1))-INSTR(multivariate_test_attributes,'HOTEL_META_INTERCARD_DESIGN',1)-LENGTH('HOTEL_META_INTERCARD_DESIGN')-3 END) END AS mvt_value,
	MAX(pure_group_type) AS pure_group_type,
	MIN(requested_at) AS first_ad_call_time
	FROM 
	intent_media_log_data_production.ad_calls 
	WHERE 
	ad_unit_type='CT' 
	AND site_type = 'EXPEDIA'
	AND product_category_type = 'FLIGHTS'
	AND browser_family = 'CHROME'
	AND requested_at_date_in_et BETWEEN '2014-03-19' AND '2014-04-18'
	AND ip_address_blacklisted = FALSE
	AND multivariate_version_id >= 880
	GROUP BY 1,2;

DROP TABLE IF EXISTS intent_media_sandbox_production.exp_clicktab_mvt_conv;
CREATE TABLE intent_media_sandbox_production.exp_clicktab_mvt_conv AS
SELECT
	publisher_user_id,
	net_conversion_value,
	requested_at
	FROM
	intent_media_log_data_production.conversions
	WHERE
	site_type = 'EXPEDIA'
	AND entity_id = 45
	AND requested_at_date_in_et BETWEEN '2014-03-19' AND '2014-04-18'
	AND product_category_type = 'FLIGHTS'
	AND ip_address_blacklisted = FALSE;
	
DROP TABLE IF EXISTS intent_media_sandbox_production.exp_clicktab_mvt_u_conv;
CREATE TABLE intent_media_sandbox_production.exp_clicktab_mvt_u_conv AS
SELECT
uvs.publisher_user_id AS publisher_user_id,
uvs.pure_group_type AS pure_group_type,
uvs.mvt_value AS mvt_value,
MAX(CASE 
	WHEN conversions.net_conversion_value > 0 then 1 
	ELSE 0
	END) AS is_booker,
SUM(conversions.net_conversion_value) AS total_conversion_value		
FROM
intent_media_sandbox_production.exp_clicktab_mvt_u uvs
LEFT JOIN
intent_media_sandbox_production.exp_clicktab_mvt_conv conversions
ON uvs.publisher_user_id = conversions.publisher_user_id
AND uvs.first_ad_call_time < conversions.requested_at
GROUP BY 1,2,3;

SELECT
pure_group_type,
mvt_value,
COUNT(*) AS visitors,
SUM(is_booker) AS bookers,
SUM(total_conversion_value) AS gross_profit
FROM
intent_media_sandbox_production.exp_clicktab_mvt_u_conv
GROUP BY 1,2;
