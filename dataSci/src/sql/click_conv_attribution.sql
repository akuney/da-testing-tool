--1
DROP TABLE IF EXISTS intent_media_sandbox_production.SPDP_55_ac
;
--2
CREATE TABLE intent_media_sandbox_production.SPDP_55_ac AS
SELECT request_id,
	requested_at,
	publisher_user_id,
	site_type,
	ad_unit_id
	FROM intent_media_log_data_production.ad_calls
WHERE requested_at_date_in_et between '2014-05-01' and '2014-05-07'
	AND ip_address_blacklisted = 0
	AND site_id in (2, 3, 12, 21, 13, 20, 22, 29, 27, 30)
	AND product_category_type = 'FLIGHTS'
	AND ad_unit_type = 'CT'
        AND outcome_type = 'SERVED'
;
--3
DROP TABLE IF EXISTS intent_media_sandbox_production.SPDP_55_c_ac
;
--4
CREATE TABLE intent_media_sandbox_production.SPDP_55_c_ac AS
SELECT ad_call_request_id,
	count(request_id) AS clicks,
	sum(actual_cpc) AS revenue
FROM intent_media_log_data_production.clicks
WHERE requested_at_date_in_et between '2014-05-01' and '2014-06-08'
	AND ip_address_blacklisted = 0
	AND fraudulent = 0
	AND product_category_type = 'FLIGHTS'
GROUP BY ad_call_request_id
;
--5
DROP TABLE IF EXISTS intent_media_sandbox_production.SPDP_55_c_raw
;
--6
CREATE TABLE intent_media_sandbox_production.SPDP_55_c_raw AS
SELECT request_id,
	requested_at,
	ad_call_request_id,
	external_impression_id,
	publisher_user_id,
	webuser_id,
	advertisement_id,
	actual_cpc
FROM intent_media_log_data_production.clicks
WHERE requested_at_date_in_et between '2014-05-01' and '2014-06-08'
	AND ip_address_blacklisted = 0
	AND fraudulent = 0
	AND product_category_type = 'FLIGHTS'
;
--7
DROP TABLE IF EXISTS intent_media_sandbox_production.SPDP_55_attribution_i
;
--8
CREATE TABLE intent_media_sandbox_production.SPDP_55_attribution_i AS
SELECT external_id,
	min(advertiser_id) AS advertiser_id
FROM intent_media_log_data_production.impressions
        -- Looking at impressions from day 0
WHERE requested_at_date_in_et between '2014-05-01' and '2014-06-08'
	AND ip_address_blacklisted = 0
	-- TODO: Is this the most updated list for the given list of sites?
	AND ad_unit_id in (10,12,13,15,30,31,40,59,62,70,71,76,77,78,80,87,88,95,96,97,98,99,100,103,115,119,122)
GROUP BY external_id
;
--9
DROP TABLE IF EXISTS intent_media_sandbox_production.SPDP_55_c_complete
;
--10
CREATE TABLE intent_media_sandbox_production.SPDP_55_c_complete AS
SELECT cl.request_id,
	cl.requested_at,
	cl.ad_call_request_id,
	cl.publisher_user_id,
	cl.webuser_id,
	i.advertiser_id,
	cl.actual_cpc
FROM intent_media_sandbox_production.SPDP_55_c_raw cl
INNER JOIN intent_media_sandbox_production.SPDP_55_attribution_i i ON (cl.external_impression_id = i.external_id)
;
--11
DROP TABLE IF EXISTS intent_media_sandbox_production.SPDP_55_con
;
--12
CREATE TABLE intent_media_sandbox_production.SPDP_55_con AS
SELECT  
      con.request_id,
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
     INNER JOIN 
     (
        SELECT entity_id,
	       order_id,
	       min(requested_at) AS min_requested_at
        FROM intent_media_log_data_production.conversions
        -- Conversion data from day 0 to day 31 (or what we have)
        -- Conversions can be attributed for upto 30 days
        WHERE requested_at_date_in_et between '2014-05-01' and '2014-06-08'
	       AND ip_address_blacklisted = 0
	       -- TODO: Are these the only advertisers we are getting conversion data from?
	       AND entity_id in (59528, 59777, 61224, 62118, 66539, 70994, 87697, 106574, 152665)
	       AND product_category_type = 'FLIGHTS'
	       AND order_id is not null
        GROUP BY 
               entity_id,
	       order_id
     ) e_o_min 
     ON (con.entity_id = e_o_min.entity_id AND con.order_id = e_o_min.order_id AND con.requested_at = e_o_min.min_requested_at)
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
        WHERE con.requested_at_date_in_et between '2014-05-01' and '2014-06-08'
	       AND con.ip_address_blacklisted = 0
	       AND con.entity_id in (59528, 59777, 61224, 62118, 66539, 70994, 87697, 106574, 152665)
	       AND con.product_category_type = 'FLIGHTS'
	       AND con.order_id is null
;
--13
DROP TABLE IF EXISTS intent_media_sandbox_production.SPDP_55_con_cl
;
--14
CREATE TABLE intent_media_sandbox_production.SPDP_55_con_cl AS
SELECT con.request_id AS con_request_id,
	cl.request_id AS cl_request_id,
	con.requested_at AS con_requested_at,
	cl.requested_at AS cl_requested_at,
	DATEDIFF('ss', cl.requested_at, con.requested_at) as con_cl_time_diff
FROM intent_media_sandbox_production.SPDP_55_con con
INNER JOIN intent_media_sandbox_production.SPDP_55_c_complete cl 
ON (con.webuser_id = cl.webuser_id AND con.entity_id = cl.advertiser_id)
;
--15
DROP TABLE IF EXISTS intent_media_sandbox_production.SPDP_55_cl_con
;
--16
CREATE TABLE intent_media_sandbox_production.SPDP_55_cl_con AS
SELECT  cl.request_id,
	cl.requested_at,
	cl.ad_call_request_id,
	cl.actual_cpc,
	CASE WHEN att_con.conversions is null THEN 0 ELSE att_con.conversions END AS clicked_conversions
FROM    intent_media_sandbox_production.SPDP_55_c_complete cl
        LEFT JOIN 
        (
                SELECT con_cl.cl_request_id,
	        count(con_cl.con_request_id) AS conversions
                FROM 
                        intent_media_sandbox_production.SPDP_55_con_cl con_cl
                        INNER JOIN 
                        (

                                SELECT con_request_id,
	                        min(con_cl_time_diff) AS min_con_cl_time_diff
                                FROM    intent_media_sandbox_production.SPDP_55_con_cl
                                        WHERE con_cl_time_diff between 1 and 2592000
                                GROUP BY con_request_id
                        ) min_con_cl_time_diff 
                        ON (con_cl.con_request_id = min_con_cl_time_diff.con_request_id AND con_cl.con_cl_time_diff = min_con_cl_time_diff.min_con_cl_time_diff)
                GROUP BY con_cl.cl_request_id
        ) att_con 
        ON (cl.request_id = att_con.cl_request_id)
;
--17
DROP TABLE IF EXISTS intent_media_sandbox_production.SPDP_55_cl
;
--18
CREATE TABLE intent_media_sandbox_production.SPDP_55_cl AS
SELECT ad_call_request_id,
	count(request_id) AS clicks,
	sum(actual_cpc) AS revenue,
	sum(clicked_conversions) AS clicked_conversions
FROM intent_media_sandbox_production.SPDP_55_cl_con
GROUP BY ad_call_request_id
;
--19
DROP TABLE IF EXISTS intent_media_sandbox_production.SPDP_55_final
;
--20
CREATE TABLE intent_media_sandbox_production.SPDP_55_final AS
SELECT 
	count(publisher_user_id) AS users,
	sum(ad_calls) AS ad_calls,
	sum(interactions) AS interactions,
	sum(clicks) AS clicks,
	sum(revenue) AS revenue,
	sum(clicked_conversions) AS clicked_conversions
FROM    (
        SELECT 
	       publisher_user_id,
	       count(request_id) AS ad_calls,
	       count(clicks) AS interactions,
	       sum(clicks) AS clicks,
	       sum(revenue) AS revenue,
	       sum(clicked_conversions) AS clicked_conversions
        FROM   (
                SELECT ac.request_id,
	               ac.requested_at,
	               ac.publisher_user_id,
	               ac.site_type,
	               ac.ad_unit_id,
	               c.clicks,
	               c.revenue,
	               c.clicked_conversions
                FROM intent_media_sandbox_production.SPDP_55_ac ac
                LEFT JOIN intent_media_sandbox_production.SPDP_55_cl c 
                ON (ac.request_id = c.ad_call_request_id)
                ) ac_c
        GROUP BY publisher_user_id
        ) user_agg
;

/*Analysis-y stuff.*/

-- Just the overall experiment.

SELECT * FROM intent_media_sandbox_production.SPDP_55_final
;