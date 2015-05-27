/* Example Query 1 - CSV import to sandbox schema */
DROP TABLE IF EXISTS intent_media_sandbox_production.YB_markets;
CREATE TABLE intent_media_sandbox_production.YB_markets
(
      id int,
      created_at datetime,
      updated_at datetime,
      name varchar,
      publisher_id int,
      publisher_market_id varchar,
      minimum_auction_price_override decimal,
      active boolean,
      country varchar,
      market_group_id int,
      is_in_feed boolean,
      airport_distince_override int
);

COPY intent_media_sandbox_production.YB_markets
(
      id,
      created_at,
      updated_at,
      name,
      publisher_id,
      publisher_market_id,
      minimum_auction_price_override,
      active,
      country,
      market_group_id,
      is_in_feed,
      airport_distince_override
)
FROM LOCAL '/Users/yoojong.bang/Desktop/markets.tsv'
WITH DELIMITER E'\t'
NULL as 'NULL';

/* Example Query 2 - window partitioning */
select
  site_type,
  request_id,
  count(request_id) over(partition by site_type) as served_ad_calls_per_site
from intent_media_log_data_production.ad_calls
where requested_at_date_in_et = '2015-03-01'
  and ip_address_blacklisted = 0
  and outcome_type = 'SERVED'
  and ad_unit_type = 'CT'
  and product_category_type = 'FLIGHTS'
limit 100;

/* Example Query 3 - conditional_true_event & lag */
select
  count(ad_call_request_id) as served_ad_calls,
  count(distinct(case when cond1 - ifnull(lag1,0) = 1 then interaction_request_id end)) as interactions,
  count(case when cond1 - ifnull(lag1,0) = 1 then click_request_id end) as clicks,
  count(case when cond2 - ifnull(lag2,0) = 1 then click_request_id end) as out_session_clicks,
  sum(case when cond1 - ifnull(lag1,0) = 1 then actual_cpc end) as gmr,
  sum(case when cond2 - ifnull(lag2,0) = 1 then actual_cpc end) as out_session_gmr
from
(
  select
    ad_call_request_id,
    interaction_request_id,
    click_request_id,
    ad_call_timestamp,
    click_timestamp,
    actual_cpc, 
    cond1,
    cond2,
    lag(cond1) over (partition by ad_call_request_id order by cond1) as lag1,
    lag(cond2) over (partition by ad_call_request_id order by cond2) as lag2
  from
  (
    select
      ac.request_id as ad_call_request_id,
      c.ad_call_request_id as interaction_request_id,
      c.request_id as click_request_id,
      ac.requested_at as ad_call_timestamp,
      c.requested_at as click_timestamp,    
      c.actual_cpc,
      conditional_true_event(ac.requested_at < c.requested_at and ac.requested_at + interval '24 hours' >= c.requested_at) over (partition by ac.request_id order by ac.requested_at, c.requested_at) as cond1,
      conditional_true_event(ac.requested_at + interval '24 hours' >= c.requested_at and ac.requested_at + interval '30 minutes' < c.requested_at) over (partition by ac.request_id order by ac.requested_at, c.requested_at) as cond2
    from intent_media_log_data_production.ad_calls ac
    left join intent_media_log_data_production.clicks c
    on ac.request_id = c.ad_call_request_id
    and c.requested_at_date_in_et between '2015-03-05' and '2015-03-10'
    and c.ip_address_blacklisted = 0
    and c.fraudulent = 0
    and c.site_type = 'EXPEDIA'
    and c.product_category_type = 'FLIGHTS'
    where ac.requested_at_date_in_et between '2015-03-05' and '2015-03-09'
      and ac.ip_address_blacklisted = 0
      and ac.outcome_type = 'SERVED'
      and ac.site_type = 'EXPEDIA'
      and ac.ad_unit_type = 'CT'
      and ac.product_category_type = 'FLIGHTS'
  ) base
) final;

/* Example Query 4 - Timeseries as dimension */
select
  e.name as advertiser,
  cam.name as campaign,
  dim.ts_lag as ts,
  count(c.request_id) as clicks,
  sum(c.actual_cpc) as gmr
from
(
  select ts, lag(ts) over (order by ts) ts_lag
  from
  (
    select ts from (
    select '2015-02-01 00:00'::timestamp t
    union
    select '2015-02-01 24:00'::timestamp t
    ) t1
    timeseries ts as '1 hour' over (order by t)
  ) t2
) dim
left join intent_media_log_data_production.clicks c
on c.requested_at_in_et between ts_lag and ts
and c.requested_at_date_in_et = '2015-02-01'
and c.ip_address_blacklisted = 0
and c.fraudulent = 0
and c.product_category_type = 'FLIGHTS'
inner join intent_media_log_data_production.impressions i
on c.external_impression_id = i.external_id
and i.requested_at_date_in_et between '2015-01-31' and '2015-02-01'
and i.ip_address_blacklisted = 0
and i.advertiser_id in (59777, 61224, 87697)
inner join intent_media_production.entities e on i.advertiser_id = e.id
inner join intent_media_production.campaigns cam on i.campaign_id = cam.id
where dim.ts_lag is not null
group by 1,2,3
order by 1,2,3;