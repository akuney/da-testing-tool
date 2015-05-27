/* Query 1 for only served ad calls */
select
  mvt_key,
  mvt_value as site_reporting_value_bucket,
  count(users) as users,
  sum(served_ad_calls) as served_ad_calls,
  sum(interactions) as interactions,
  sum(clicks) as clicks,
  sum(revenue) as revenue
from
(
  select
    'site_reporting_value_01' as mvt_key,
    site_reporting_value_01 as mvt_value,
    publisher_user_id as users,
    count(request_id) as served_ad_calls,
    sum(interactions) as interactions,
    sum(clicks) as clicks,
    sum(revenue) as revenue
  from intent_media_log_data_production.ad_calls ac
  left join
  (
    select
      ad_call_request_id,
      case when count(request_id) > 0 then 1 else 0 end as interactions,
      count(request_id) as clicks,
      sum(actual_cpc) as revenue
    from intent_media_log_data_production.clicks
    where ip_address_blacklisted = 0
      and fraudulent = 0
      and site_type = 'EXPEDIA'
      and product_category_type = 'HOTELS'
      and date(ad_call_requested_at) >= '2014-10-08'
    group by
      ad_call_request_id
  ) c
  on ac.request_id = c.ad_call_request_id
  where ac.ip_address_blacklisted = 0
    and ac.outcome_type = 'SERVED'
    and ac.site_type = 'EXPEDIA'
    and ac.ad_unit_type = 'CT'
    and ac.product_category_type = 'HOTELS'
    and ac.requested_at_date_in_et >= '2014-10-08'
    and ac.ad_unit_id = 196
  group by
    mvt_value,
    users
) per_user
group by
  mvt_key,
  mvt_value
order by 
  mvt_value;

/* Query 2 for extended dimensions */
select
  show_ads,
  outcome_type,
  mvt_key,
  mvt_value as site_reporting_value_bucket,
  count(users) as users,
  sum(served_ad_calls) as served_ad_calls,
  sum(interactions) as interactions,
  sum(clicks) as clicks,
  sum(revenue) as revenue
from
(
  select
    show_ads,
    outcome_type,
    'site_reporting_value_01' as mvt_key,
    site_reporting_value_01 as mvt_value,
    publisher_user_id as users,
    count(request_id) as served_ad_calls,
    sum(interactions) as interactions,
    sum(clicks) as clicks,
    sum(revenue) as revenue
  from intent_media_log_data_production.ad_calls ac
  left join
  (
    select
      ad_call_request_id,
      case when count(request_id) > 0 then 1 else 0 end as interactions,
      count(request_id) as clicks,
      sum(actual_cpc) as revenue
    from intent_media_log_data_production.clicks
    where ip_address_blacklisted = 0
      and fraudulent = 0
      and site_type = 'EXPEDIA'
      and product_category_type = 'HOTELS'
      and date(ad_call_requested_at) >= '2014-10-08'
    group by
      ad_call_request_id
  ) c
  on ac.request_id = c.ad_call_request_id
  where ac.ip_address_blacklisted = 0
    and ac.site_type = 'EXPEDIA'
    and ac.ad_unit_type = 'CT'
    and ac.product_category_type = 'HOTELS'
    and ac.requested_at_date_in_et >= '2014-10-08'
    and ac.ad_unit_id = 196
  group by
    show_ads,
    outcome_type,
    mvt_value,
    users
) per_user
group by
  show_ads,
  outcome_type,
  mvt_key,
  mvt_value
order by
  mvt_value;