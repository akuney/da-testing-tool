/*I always start by pulling the data that I need down from intent_media_log_data_production. In this case that's ad calls, clicks, and conversions, so I'll start with ad calls. Everything generally ends up being attributed back onto ad calls, so you need to get your filters right. If you have extraneous clicks, that's okay, because they won't end up getting attributed. But extraneous or irrelevant ad calls is a problem. So I always have five filters in every ad call extract:

-Date
-IP address blacklisted = 0
-Product
-Ad type
-Site

And then MOST of the time I'll have an outcome_type filter. Not having outcome_type = 'SERVED' can really mess up your data if you only want to look at served ad calls. So include those five above, and always think about whether you need to filter by outcome_type too.*/

DROP TABLE IF EXISTS intent_media_sandbox_production.DP_cc_attribution_ac
;

CREATE TABLE intent_media_sandbox_production.DP_cc_attribution_ac AS
SELECT request_id,
	requested_at,
	product_category_type,
	site_type,
	ad_unit_id,
	publisher_user_id
FROM intent_media_log_data_production.ad_calls
WHERE requested_at_date_in_et between '2014-03-01' and '2014-03-01'
	AND ip_address_blacklisted = 0
	AND product_category_type = 'FLIGHTS'
	AND ad_unit_type = 'CT'
	AND site_id in (2, 3)
    AND outcome_type = 'SERVED'
;

/*Now clicks. A quick chat about the dates: a click can be attributed up to one day after an ad call. An advertiser conversion can be attributed up to 30 days after a click. So that means that even if we're only looking at ad calls on day 1, we need conversions up to 31 days after day 1. Any conversions beyond that date couldn't possibly be attributed to one of my ad calls. So ad calls for day 1 means clicks for days 1 and 2, and conversions for days 1 through 32.

BUT, since I'm attributing conversions to clicks, I have to look at clicks from days 1 through 32 as well. Reason being, we're doing last-click attribution, so I need to make sure I have that last click in my data set, even if it won't end up being attributed to one of my ad calls.

Back to the data above: day 1 is 2014-03-01. If was just attributing clicks, my click date range would be 2014-03-01 - 2014-03-02. But since I'm attributing advertiser conversions to clicks, I need a date range of 2014-03-01 - 2014-04-01 for both clicks and conversions.*/

DROP TABLE IF EXISTS intent_media_sandbox_production.DP_cc_attribution_cl_raw
;

CREATE TABLE intent_media_sandbox_production.DP_cc_attribution_cl_raw AS
SELECT request_id,
	requested_at,
	external_impression_id,
	ad_call_request_id,
	publisher_user_id,
	webuser_id,
	actual_cpc
FROM intent_media_log_data_production.clicks
WHERE requested_at_date_in_et between '2014-03-01' and '2014-04-01'
	AND ip_address_blacklisted = 0
	AND fraudulent = 0
	AND site_type in ('ORBITZ_GLOBAL', 'CHEAPTICKETS')
;

/*Here's this annoying thing where I have to pull impressions to match with clicks so that I can get the advertiser_id for clicks. Ugh. I wrote a story about adding an advertiser_id field to the clicks table ... dare to dream.*/

DROP TABLE IF EXISTS intent_media_sandbox_production.DP_cc_attribution_i_cl
;

CREATE TABLE intent_media_sandbox_production.DP_cc_attribution_i_cl AS
SELECT external_id,
	min(advertiser_id) AS advertiser_id
FROM intent_media_log_data_production.impressions
WHERE requested_at_date_in_et between '2014-03-01' and '2014-04-01'
	AND ip_address_blacklisted = 0
	AND ad_unit_id in (10, 12, 13, 15, 30, 31)
GROUP BY external_id
;

/*Putting clicks together with that impressions extract so that I have clicks WITH an advertiser_id.*/

DROP TABLE IF EXISTS intent_media_sandbox_production.DP_cc_attribution_cl_raw_complete
;

CREATE TABLE intent_media_sandbox_production.DP_cc_attribution_cl_raw_complete AS
SELECT cl.request_id,
	cl.requested_at,
	cl.ad_call_request_id,
	cl.publisher_user_id,
	cl.webuser_id,
	i.advertiser_id,
	cl.actual_cpc
FROM intent_media_sandbox_production.DP_cc_attribution_cl_raw cl
INNER JOIN intent_media_sandbox_production.DP_cc_attribution_i_cl i ON (cl.external_impression_id = i.external_id)
;

/*Onto conversions. There are two important things happening here: deduping, and filtering on entity. The deduping is just a data cleanliness thing, and the filtering on entity is necessary because some advertisers send us conversion records that aren't actually conversions. This list isn't documented anywhere, you just have to know who's sending us good data. Work with Greg on updating this list every now and then.*/

DROP TABLE IF EXISTS intent_media_sandbox_production.DP_cc_attribution_con
;

CREATE TABLE intent_media_sandbox_production.DP_cc_attribution_con AS
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
WHERE requested_at_date_in_et between '2014-03-01' and '2014-04-01'
	AND ip_address_blacklisted = 0
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
WHERE con.requested_at_date_in_et between '2014-03-01' and '2014-04-01'
	AND con.ip_address_blacklisted = 0
	AND con.entity_id in (59528, 59777, 61224, 62118, 66539, 70994, 87697, 106574, 152665)
	AND con.product_category_type = 'FLIGHTS'
	AND con.order_id is null
;

/*Now we join conversions to clicks, by webuser and advertiser. There will be multiple clicks that match multiple conversions, so we need to drill down once we've joined. First, find the last click before a conversion, so each conversion is uniquely tied to one click. Then group by the click request_id so that we have one unique record per click.*/

DROP TABLE IF EXISTS intent_media_sandbox_production.DP_cc_attribution_con_cl
;

CREATE TABLE intent_media_sandbox_production.DP_cc_attribution_con_cl AS
SELECT con.request_id AS con_request_id,
	cl.request_id AS cl_request_id,
	con.requested_at AS con_requested_at,
	cl.requested_at AS cl_requested_at,
	DATEDIFF('ss', cl.requested_at, con.requested_at) as con_cl_time_diff
FROM intent_media_sandbox_production.DP_cc_attribution_con con
INNER JOIN intent_media_sandbox_production.DP_cc_attribution_cl_raw_complete cl ON (con.webuser_id = cl.webuser_id AND con.entity_id = cl.advertiser_id)
;

DROP TABLE IF EXISTS intent_media_sandbox_production.DP_cc_attribution_cl_con
;

CREATE TABLE intent_media_sandbox_production.DP_cc_attribution_cl_con AS
SELECT cl.request_id,
	cl.requested_at,
	cl.ad_call_request_id,
	cl.actual_cpc,
	CASE WHEN att_con.conversions is null THEN 0 ELSE att_con.conversions END AS clicked_conversions
FROM intent_media_sandbox_production.DP_cc_attribution_cl_raw_complete cl
LEFT JOIN (

SELECT con_cl.cl_request_id,
	count(con_cl.con_request_id) AS conversions
FROM intent_media_sandbox_production.DP_cc_attribution_con_cl con_cl
INNER JOIN (

SELECT con_request_id,
	min(con_cl_time_diff) AS min_con_cl_time_diff
FROM intent_media_sandbox_production.DP_cc_attribution_con_cl
WHERE con_cl_time_diff between 1 and 2592000
GROUP BY con_request_id

) min_con_cl_time_diff ON (con_cl.con_request_id = min_con_cl_time_diff.con_request_id AND con_cl.con_cl_time_diff = min_con_cl_time_diff.min_con_cl_time_diff)
GROUP BY con_cl.cl_request_id

) att_con ON (cl.request_id = att_con.cl_request_id)
;

/*Now we need to summarize click data by ad_call_request_id. This is necessary because if you just join ad calls to clicks by ad_calls.request_id = clicks.ad_call_request_id, and you have an ad call that garnered multiple clicks, you'll duplicate the ad call record. That is, the ad calls-to-clicks relationship is one-to-many. So summarize click data by ad_call_request_id first. THEN join it onto ad calls.*/

DROP TABLE IF EXISTS intent_media_sandbox_production.DP_cc_attribution_cl
;

CREATE TABLE intent_media_sandbox_production.DP_cc_attribution_cl AS
SELECT ad_call_request_id,
	count(request_id) AS clicks,
	sum(actual_cpc) AS revenue,
	sum(clicked_conversions) AS clicked_conversions
FROM intent_media_sandbox_production.DP_cc_attribution_cl_con
GROUP BY ad_call_request_id
;

DROP TABLE IF EXISTS intent_media_sandbox_production.DP_cc_attribution_ac_c
;

CREATE TABLE intent_media_sandbox_production.DP_cc_attribution_ac_c AS
SELECT ac.request_id,
	ac.requested_at,
	ac.product_category_type,
	ac.site_type,
	ac.ad_unit_id,
	ac.publisher_user_id,
	cl.ad_call_request_id,
	cl.clicks,
	cl.revenue,
	cl.clicked_conversions
FROM intent_media_sandbox_production.DP_cc_attribution_ac ac
LEFT JOIN intent_media_sandbox_production.DP_cc_attribution_cl cl ON (ac.request_id = cl.ad_call_request_id)
;

/*Summarize the data by whatever, and you're done!*/

DROP TABLE IF EXISTS intent_media_sandbox_production.DP_cc_attribution_final
;

CREATE TABLE intent_media_sandbox_production.DP_cc_attribution_final AS
SELECT site_type,
	count(distinct publisher_user_id) AS users,
	count(request_id) AS ad_calls,
	count(clicks) AS interactions,
	sum(clicks) AS clicks,
	sum(revenue) AS revenue,
	sum(clicked_conversions) AS clicked_conversions
FROM intent_media_sandbox_production.DP_cc_attribution_ac_c
GROUP BY site_type
;

SELECT * FROM intent_media_sandbox_production.DP_cc_attribution_final
ORDER BY site_type asc
;
