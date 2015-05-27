
DROP TABLE IF EXISTS intent_media_sandbox_production.SP_MVT_1052_ac
;

CREATE TABLE intent_media_sandbox_production.SP_MVT_1052_ac AS
SELECT request_id,
	requested_at,
	publisher_user_id,
	site_type,
	ad_unit_id,
	multivariate_version_id,
        CASE 
                WHEN multivariate_test_attributes like '%old_model%' THEN 'old_model'
                WHEN multivariate_test_attributes like '%new_model%' THEN 'new_model'
                ELSE 'not_found' END as model_type 
FROM intent_media_log_data_production.ad_calls
WHERE requested_at_date_in_et between '2014-05-30' and '2014-06-24'
	AND ip_address_blacklisted = 0
/*	AND site_id in (2, 3, 4, 6, 8, 9, 12, 13, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
	                       31, 32, 33, 34, 35, 42, 43, 45)*/
	AND site_type in ('AIRFASTTICKETS', 'AIRTKT', 'BOOKIT', 'BUDGETAIR', 'CHEAPAIR', 'CHEAPOAIR', 'CHEAPTICKETS', 
	                       'EBOOKERS', 'EXPEDIA', 'FARESPOTTER', 'FLY_DOT_COM', 'GOGOBOT', 'HIPMUNK', 'HOTWIRE', 
	                       'HOTWIRE_MEDIA_FILL_IN', 'HOTWIRE_UK', 'KAYAK', 'KAYAK_UK', 'LASTMINUTE_DOT_COM', 'LOWCOSTAIRLINES', 
	                       'LOWFARES', 'ONETRAVEL', 'ORBITZ_GLOBAL', 'TRAVELZOO', 'TRIPADVISOR', 'TRIPDOTCOM', 'TVLY', 'VAYAMA', 'WEBJET')
	AND product_category_type = 'FLIGHTS'
	AND ad_unit_type = 'CT'
        AND outcome_type = 'SERVED'
        AND multivariate_version_id >= 1052
;

DROP TABLE IF EXISTS intent_media_sandbox_production.SP_MVT_1052_cl
;

CREATE TABLE intent_media_sandbox_production.SP_MVT_1052_cl AS
SELECT  ad_call_request_id,
	count(*) AS clicks,
	sum(actual_cpc) AS revenue
FROM intent_media_log_data_production.clicks 
WHERE requested_at_date_in_et between '2014-05-30' and '2014-06-25'
	AND ip_address_blacklisted = 0
	AND fraudulent = 0
	AND product_category_type = 'FLIGHTS'
GROUP BY ad_call_request_id
;

DROP TABLE IF EXISTS intent_media_sandbox_production.SP_MVT_1052_c_ac
;
CREATE TABLE intent_media_sandbox_production.SP_MVT_1052_c_ac AS
SELECT  
	ac.model_type,
	cl.clicks,
	cl.revenue
FROM intent_media_sandbox_production.SP_MVT_1052_ac ac
LEFT JOIN intent_media_sandbox_production.SP_MVT_1052_cl cl ON (ac.request_id = cl.ad_call_request_id)
;

SELECT
        model_type,
        count(*) AS ad_calls,
        sum(clicks) AS total_clicks,
        sum(revenue) AS total_click_revenue
FROM intent_media_sandbox_production.SP_MVT_1052_c_ac
GROUP BY model_type
;

