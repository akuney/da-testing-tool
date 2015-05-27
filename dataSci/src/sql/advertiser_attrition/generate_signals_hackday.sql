-- features from entities table for each
-- hotel advertiser, not based on time/month
DROP TABLE IF EXISTS intent_media_sandbox_production.sp_adv_attrition_advertiser_entities;
CREATE TABLE intent_media_sandbox_production.sp_adv_attrition_advertiser_entities AS
SELECT
  id,
  active,
  exceeded_credit_threshold,
  workflow_state,
  payment_method_id,
  last_auction_participation,
  first_auction_participation,
  hotel_ssr_advertiser_live_time,
  spending_cap_enabled,
  allow_ads_on_package_path,
  suspended_for_non_payment
FROM intent_media_production.entities
WHERE
  entity_type = 'HotelSsrAdvertiser'
;

-- list of (month, year)
DROP TABLE IF EXISTS intent_media_sandbox_production.sp_adv_attrition_months;
CREATE TABLE intent_media_sandbox_production.sp_adv_attrition_months AS
select * from
    (select 2011 as year union
     select 2012 as year union
     select 2013 as year union
     select 2014 as year union
     select 2015 as year
    ) AS years,
    (select 1 as month union
     select 2 as month union
     select 3 as month union
     select 4 as month union
     select 5 as month union
     select 6 as month union
     select 7 as month union
     select 8 as month union
     select 9 as month union
     select 10 as month union
     select 11 as month union
     select 12 as month
     ) AS months
WHERE year between 2011 AND 2014 OR (year = 2015 AND month < 2)
ORDER by year, month;

-- splitting the entities and entity-based
-- signals over the months
DROP TABLE IF EXISTS intent_media_sandbox_production.sp_adv_attrition_advertiser_entities_months;
CREATE TABLE intent_media_sandbox_production.sp_adv_attrition_advertiser_entities_months AS
SELECT
  *
FROM
  intent_media_sandbox_production.sp_adv_attrition_advertiser_entities e
  JOIN intent_media_sandbox_production.sp_adv_attrition_months m  ON TRUE;

-- list of each time a campaign was paused
DROP TABLE IF EXISTS intent_media_sandbox_production.sp_adv_attrition_pause_time;
CREATE TABLE intent_media_sandbox_production.sp_adv_attrition_pause_time AS
SELECT
  advertiser_id,
  created_at as campaign_paused_time
FROM intent_media_production.hotel_ssr_advertiser_changes
WHERE
  change_type = 'Campaign Status'
  AND old_settings = 'Active'
  AND new_settings = 'Paused'
;

-- cumulative count of prior pauses by advertiser
-- for each time a campaign was paused
DROP TABLE IF EXISTS intent_media_sandbox_production.sp_adv_attrition_cum_pause_count;
CREATE TABLE intent_media_sandbox_production.sp_adv_attrition_cum_pause_count AS
SELECT
t1.advertiser_id,
t1.campaign_paused_time,
sum(case WHEN t2.advertiser_id IS NULL THEN 0 ELSE 1 END) as cumulative_pause_count
FROM
intent_media_sandbox_production.sp_adv_attrition_pause_time t1
LEFT JOIN
intent_media_sandbox_production.sp_adv_attrition_pause_time t2
ON t1.advertiser_id = t2.advertiser_id and t1.campaign_paused_time > t2.campaign_paused_time
GROUP BY 1,2
ORDER BY 1,2 ASC
;

DROP TABLE IF EXISTS intent_media_sandbox_production.sp_adv_attrition_advertiser_entities_months_1;
CREATE TABLE intent_media_sandbox_production.sp_adv_attrition_advertiser_entities_months_1 AS
SELECT
  em1.*,
  ifnull(em_cpc.max_cum_pause_count, 0) AS max_cum_pause_count
FROM
intent_media_sandbox_production.sp_adv_attrition_advertiser_entities_months em1
INNER JOIN
(SELECT
  em.id,
  em.month,
  em.year,
  max(cpc.cumulative_pause_count) max_cum_pause_count
FROM intent_media_sandbox_production.sp_adv_attrition_advertiser_entities_months em
LEFT JOIN  intent_media_sandbox_production.sp_adv_attrition_cum_pause_count cpc
ON em.id = cpc.advertiser_id AND em.month = MONTH(cpc.campaign_paused_time) and em.year = YEAR(cpc.campaign_paused_time)
group by 1,2,3) em_cpc
ON em1.id=em_cpc.id AND em1.month=em_cpc.month AND em1.year=em_cpc.year;


SELECT
  user_id,
  count(DISTINCT entity_id)
FROM intent_media_production.memberships;

SELECT
  em.month,
  em.exceeded_credit_threshold,
  em.payment_method_id,
  em.spending_cap_enabled,
  em.allow_ads_on_package_path,
  em.suspended_for_non_payment,
  em.max_cum_pause_count,
  imhpm.intent_media_market_id,
	ifnull(imm.report_segment, 'Other') as segment_name,
  CASE WHEN p.advertiser_id IS NULL THEN 0 ELSE 1 END AS y_value
FROM intent_media_sandbox_production.sp_adv_attrition_advertiser_entities_months_1 em
  LEFT JOIN intent_media_production.hotel_property_advertisers hpa ON em.id = hpa.hotel_ssr_advertiser_id
  LEFT JOIN intent_media_production.intent_media_hotel_properties_markets imhpm ON hpa.hotel_property_id = imhpm.hotel_property_id
  LEFT JOIN intent_media_production.intent_media_markets imm ON imm.id = imhpm.intent_media_market_id
  LEFT JOIN intent_media_sandbox_production.sp_adv_attrition_pause_time p

    ON em.id = p.advertiser_id AND em.month = MONTH(p.campaign_paused_time) AND em.year=YEAR(p.campaign_paused_time);


