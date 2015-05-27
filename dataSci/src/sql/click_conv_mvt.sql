
DROP TABLE IF EXISTS intent_media_sandbox_production.SP_MVT_1052_ac
;

CREATE TABLE intent_media_sandbox_production.SP_MVT_1052_ac AS
SELECT request_id,
	requested_at,
	publisher_user_id,
	site_type,
	ad_unit_id,
	multivariate_version_id,
	CASE INSTR(multivariate_test_attributes,'QUALITY_SCORE_MODEL',1) WHEN 0 THEN 'Not_found' ELSE SUBSTR(multivariate_test_attributes,INSTR(multivariate_test_attributes,'QUALITY_SCORE_MODEL',1)+LENGTH('QUALITY_SCORE_MODEL')+3,CASE INSTR(multivariate_test_attributes,'","',INSTR(multivariate_test_attributes,'QUALITY_SCORE_MODEL',1)) WHEN 0 THEN LENGTH(multivariate_test_attributes)-INSTR(multivariate_test_attributes,'QUALITY_SCORE_MODEL',1)-LENGTH('QUALITY_SCORE_MODEL')-4 ELSE INSTR(multivariate_test_attributes,'","',INSTR(multivariate_test_attributes,'QUALITY_SCORE_MODEL',1))-INSTR(multivariate_test_attributes,'QUALITY_SCORE_MODEL',1)-LENGTH('QUALITY_SCORE_MODEL')-3 END) END AS mvt_value_1
FROM intent_media_log_data_production.ad_calls
WHERE requested_at_date_in_et between '2014-05-30' and '2014-06-21'
	AND ip_address_blacklisted = 0
	AND site_id in (2, 3, 4, 6, 8, 9, 12, 13, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
	                       31, 32, 33, 34, 35, 42, 43, 45)
	AND product_category_type = 'FLIGHTS'
	AND ad_unit_type = 'CT'
        AND outcome_type = 'SERVED'
        AND multivariate_version_id >= 1052
;

DROP TABLE IF EXISTS intent_media_sandbox_production.SP_MVT_1052_c_ac
;

CREATE TABLE intent_media_sandbox_production.SP_MVT_1052_c_ac AS
SELECT ad_call_request_id,
	count(request_id) AS clicks,
	sum(actual_cpc) AS revenue,
	sum(actual_cpc*actual_cpc) AS revenue_2_c
FROM intent_media_log_data_production.clicks
WHERE requested_at_date_in_et between '2014-05-30' and '2014-06-22'
	AND ip_address_blacklisted = 0
	AND fraudulent = 0
	AND product_category_type = 'FLIGHTS'
GROUP BY ad_call_request_id
;

DROP TABLE IF EXISTS intent_media_sandbox_production.SP_MVT_1052_c_raw
;

CREATE TABLE intent_media_sandbox_production.SP_MVT_1052_c_raw AS
SELECT request_id,
	requested_at,
	ad_call_request_id,
	external_impression_id,
	publisher_user_id,
	webuser_id,
	advertisement_id,
	actual_cpc
FROM intent_media_log_data_production.clicks
WHERE requested_at_date_in_et between '2014-05-30' and '2014-06-30'
	AND ip_address_blacklisted = 0
	AND fraudulent = 0
	AND product_category_type = 'FLIGHTS'
;

DROP TABLE IF EXISTS intent_media_sandbox_production.SP_MVT_1052_attribution_i
;

CREATE TABLE intent_media_sandbox_production.SP_MVT_1052_attribution_i AS
SELECT external_id,
	min(advertiser_id) AS advertiser_id
FROM intent_media_log_data_production.impressions
        -- Looking at impressions from day 0
WHERE requested_at_date_in_et between '2014-05-30' and '2014-06-30'
	AND ip_address_blacklisted = 0
	AND ad_unit_id in (10,12,13,15,30,31,40,59,62,70,71,76,77,78,80,87,88,95,96,97,98,99,100,103,115,119,122)
GROUP BY external_id
;

DROP TABLE IF EXISTS intent_media_sandbox_production.SP_MVT_1052_c_complete
;

CREATE TABLE intent_media_sandbox_production.SP_MVT_1052_c_complete AS
SELECT cl.request_id,
	cl.requested_at,
	cl.ad_call_request_id,
	cl.publisher_user_id,
	cl.webuser_id,
	i.advertiser_id,
	cl.actual_cpc
FROM intent_media_sandbox_production.SP_MVT_1052_c_raw cl
INNER JOIN intent_media_sandbox_production.SP_MVT_1052_attribution_i i ON (cl.external_impression_id = i.external_id)
;

DROP TABLE IF EXISTS intent_media_sandbox_production.SP_MVT_1052_con
;

CREATE TABLE intent_media_sandbox_production.SP_MVT_1052_con AS
SELECT con.request_id,
	con.requested_at,
	con.order_id,
	con.webuser_id,
	con.publisher_user_id,
	con.entity_id,
	con.product_category_type,
	con.site_type,
	con.conversion_value,
	con.net_conversion_value
FROM intent_media_log_data_production.conversions con
     INNER JOIN (

SELECT entity_id,
	order_id,
	min(requested_at) AS min_requested_at
FROM intent_media_log_data_production.conversions
        -- Conversion data from day 0 to day 31 (or what we have);
        -- Conversions can be attributed for upto 30 days
WHERE requested_at_date_in_et between '2014-05-30' and '2014-06-30'
	AND ip_address_blacklisted = 0
	-- TODO: Are these the only advertisers we are getting conversion data from?
	AND entity_id in (59528, 59777, 61224, 62118, 66539, 70994, 87697, 106574, 152665)
	AND product_category_type = 'FLIGHTS'
	AND order_id is not null
GROUP BY entity_id,
	order_id

) e_o_min ON (con.entity_id = e_o_min.entity_id AND con.order_id = e_o_min.order_id AND con.requested_at = e_o_min.min_requested_at)

UNION

SELECT con.request_id,
	con.requested_at,
	con.order_id,
	con.webuser_id,
	con.publisher_user_id,
	con.entity_id,
	con.product_category_type,
	con.site_type,
	con.conversion_value,
	con.net_conversion_value
FROM intent_media_log_data_production.conversions con
WHERE con.requested_at_date_in_et between '2014-05-30' and '2014-06-02'
	AND con.ip_address_blacklisted = 0
	AND con.entity_id in (59528, 59777, 61224, 62118, 66539, 70994, 87697, 106574, 152665)
	AND con.product_category_type = 'FLIGHTS'
	AND con.order_id is null
;

DROP TABLE IF EXISTS intent_media_sandbox_production.SP_MVT_1052_con_cl
;

CREATE TABLE intent_media_sandbox_production.SP_MVT_1052_con_cl AS
SELECT con.request_id AS con_request_id,
	cl.request_id AS cl_request_id,
	con.requested_at AS con_requested_at,
	cl.requested_at AS cl_requested_at,
	DATEDIFF('ss', cl.requested_at, con.requested_at) as con_cl_time_diff
FROM intent_media_sandbox_production.SP_MVT_1052_con con
INNER JOIN intent_media_sandbox_production.SP_MVT_1052_c_complete cl ON (con.webuser_id = cl.webuser_id AND con.entity_id = cl.advertiser_id)
;

DROP TABLE IF EXISTS intent_media_sandbox_production.SP_MVT_1052_cl_con
;

CREATE TABLE intent_media_sandbox_production.SP_MVT_1052_cl_con AS
SELECT cl.request_id,
	cl.requested_at,
	cl.ad_call_request_id,
	cl.actual_cpc,
	CASE WHEN att_con.conversions is null THEN 0 ELSE att_con.conversions END AS clicked_conversions
FROM intent_media_sandbox_production.SP_MVT_1052_c_complete cl
LEFT JOIN (

SELECT con_cl.cl_request_id,
	count(con_cl.con_request_id) AS conversions
FROM intent_media_sandbox_production.SP_MVT_1052_con_cl con_cl
INNER JOIN (

SELECT con_request_id,
	min(con_cl_time_diff) AS min_con_cl_time_diff
FROM intent_media_sandbox_production.SP_MVT_1052_con_cl
WHERE con_cl_time_diff between 1 and 2592000
GROUP BY con_request_id

) min_con_cl_time_diff ON (con_cl.con_request_id = min_con_cl_time_diff.con_request_id AND con_cl.con_cl_time_diff = min_con_cl_time_diff.min_con_cl_time_diff)
GROUP BY con_cl.cl_request_id

) att_con ON (cl.request_id = att_con.cl_request_id)
;

DROP TABLE IF EXISTS intent_media_sandbox_production.SP_MVT_1052_cl
;

CREATE TABLE intent_media_sandbox_production.SP_MVT_1052_cl AS
SELECT ad_call_request_id,
	count(request_id) AS clicks,
	sum(actual_cpc) AS revenue,
	sum(actual_cpc*actual_cpc) AS revenue_2_c,
	sum(clicked_conversions) AS clicked_conversions,
	sum(clicked_conversions*clicked_conversions) AS clicked_conversions_2_c
FROM intent_media_sandbox_production.SP_MVT_1052_cl_con
GROUP BY ad_call_request_id
;

DROP TABLE IF EXISTS intent_media_sandbox_production.SP_MVT_1052_final
;

CREATE TABLE intent_media_sandbox_production.SP_MVT_1052_final AS
SELECT mvt_value_1,
	count(publisher_user_id) AS users,
	sum(ad_calls) AS ad_calls,
	sum(ad_calls*ad_calls) AS ad_calls_2,
	sum(interactions) AS interactions,
	sum(interactions*interactions) AS interactions_2_u,
	sum(clicks) AS clicks,
	sum(clicks_2_ac) AS clicks_2_ac,
	sum(clicks*clicks) AS clicks_2_u,
	sum(revenue) AS revenue,
	sum(revenue_2_c) AS revenue_2_c,
	sum(revenue_2_ac) AS revenue_2_ac,
	sum(revenue*revenue) AS revenue_2_u,
	sum(clicked_conversions) AS clicked_conversions,
	sum(clicked_conversions_2_c) AS clicked_conversions_2_c,
	sum(clicked_conversions*clicked_conversions) AS clicked_conversions_2_u
FROM (

SELECT mvt_value_1,
	publisher_user_id,
	count(request_id) AS ad_calls,
	count(clicks) AS interactions,
	sum(clicks) AS clicks,
	sum(clicks*clicks) AS clicks_2_ac,
	sum(revenue) AS revenue,
	sum(revenue_2_c) AS revenue_2_c,
	sum(revenue*revenue) AS revenue_2_ac,
	sum(clicked_conversions) AS clicked_conversions,
	sum(clicked_conversions_2_c) AS clicked_conversions_2_c
FROM (

SELECT ac.request_id,
	ac.requested_at,
	ac.publisher_user_id,
	ac.site_type,
	ac.ad_unit_id,
	ac.multivariate_version_id,
	ac.mvt_value_1,
	c.clicks,
	c.revenue,
	c.revenue_2_c,
	c.clicked_conversions,
	c.clicked_conversions_2_c
FROM intent_media_sandbox_production.SP_MVT_1052_ac ac
LEFT JOIN intent_media_sandbox_production.SP_MVT_1052_cl c ON (ac.request_id = c.ad_call_request_id)

) ac_c
GROUP BY mvt_value_1,
	publisher_user_id

) user_agg
GROUP BY mvt_value_1
;

/*Analysis-y stuff.*/

-- Just the overall experiment.

SELECT * FROM intent_media_sandbox_production.SP_MVT_1052_final
ORDER BY mvt_value_1 desc
;