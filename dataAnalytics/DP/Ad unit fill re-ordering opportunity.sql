/*We'll start by pulling all impressions with their matching clicks.*/

DROP TABLE IF EXISTS intent_media_sandbox_production.DP_all_i_cl
;

CREATE TABLE intent_media_sandbox_production.DP_all_i_cl AS
SELECT i.ad_unit_id,
	i.request_id,
	i.external_id,
	i.auction_position,
	i.quality_score,
	i.actual_cpc,
	cl.external_impression_id,
	cl.clicks
FROM intent_media_log_data_production.impressions i
LEFT JOIN (

SELECT external_impression_id,
	count(request_id) AS clicks,
	sum(actual_cpc) AS actual_cpc
FROM intent_media_log_data_production.clicks
WHERE requested_at_date_in_et between '2014-04-01' and '2014-04-08'
	AND ip_address_blacklisted = 0
	AND fraudulent = 0
GROUP BY external_impression_id

) cl ON (i.external_id = cl.external_impression_id)
WHERE i.requested_at_date_in_et between '2014-04-01' and '2014-04-07'
	AND i.ip_address_blacklisted = 0
;

/*Next we create a table with APP values by ad unit and auction position. This is our measure of the natural click gradient.*/

DROP TABLE IF EXISTS intent_media_sandbox_production.DP_all_APP
;

CREATE TABLE intent_media_sandbox_production.DP_all_APP AS
SELECT ad_unit_id,
	auction_position,
	sum(clicks)/sum(quality_score) AS APP,
	RANK() OVER (PARTITION BY ad_unit_id ORDER BY sum(clicks)/sum(quality_score) desc) AS APP_rank
FROM intent_media_sandbox_production.DP_all_i_cl
GROUP BY ad_unit_id,
	auction_position
ORDER BY ad_unit_id,
	auction_position
;

/*Now it's just a matter of aggregating results, first by ad unit, then by product and ad type. The difference between old_erev and new_erev is the opportunity for re-ordering, at least within the sample.*/

SELECT i.ad_unit_id,
	count(distinct i.request_id) AS ad_calls,
	count(i.external_id) AS impressions,
	sum(i.quality_score) AS qs_sum,
	sum(i.quality_score * APP_old.APP) AS old_eclicks,
	sum(i.quality_score * APP_new.APP) AS new_eclicks,
	sum(i.quality_score * APP_old.APP * i.actual_cpc) AS old_erev,
	sum(i.quality_score * APP_new.APP * i.actual_cpc) AS new_erev
FROM intent_media_sandbox_production.DP_all_i_cl i
INNER JOIN intent_media_sandbox_production.DP_all_APP APP_old ON (i.ad_unit_id = APP_old.ad_unit_id AND i.auction_position = APP_old.auction_position)
INNER JOIN intent_media_sandbox_production.DP_all_APP APP_new ON (i.ad_unit_id = APP_new.ad_unit_id AND i.auction_position = APP_new.APP_rank)
INNER JOIN intent_media_production.ad_units au ON (i.ad_unit_id = au.id)
GROUP BY i.ad_unit_id
ORDER BY i.ad_unit_id
;

SELECT au.product_category_type,
	au.ad_type,
	count(distinct i.request_id) AS ad_calls,
	count(i.external_id) AS impressions,
	sum(i.quality_score) AS qs_sum,
	sum(i.quality_score * APP_old.APP) AS old_eclicks,
	sum(i.quality_score * APP_new.APP) AS new_eclicks,
	sum(i.quality_score * APP_old.APP * i.actual_cpc) AS old_erev,
	sum(i.quality_score * APP_new.APP * i.actual_cpc) AS new_erev
FROM intent_media_sandbox_production.DP_all_i_cl i
INNER JOIN intent_media_sandbox_production.DP_all_APP APP_old ON (i.ad_unit_id = APP_old.ad_unit_id AND i.auction_position = APP_old.auction_position)
INNER JOIN intent_media_sandbox_production.DP_all_APP APP_new ON (i.ad_unit_id = APP_new.ad_unit_id AND i.auction_position = APP_new.APP_rank)
INNER JOIN intent_media_production.ad_units au ON (i.ad_unit_id = au.id)
GROUP BY au.product_category_type,
	au.ad_type
ORDER BY au.product_category_type,
	au.ad_type
;