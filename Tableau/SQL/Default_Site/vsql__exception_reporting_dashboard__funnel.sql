select
  dimensions.requested_at_date_in_et as Date,
  dimensions.ad_unit_id as "Ad Unit ID",
  dimensions.browser_family as "Browser Family",
  dimensions.browser as Browser,
  sites.id as "Site ID",
  sites.display_name as "Site",
  ad_units.name as "Ad Unit",
  ad_units.ad_type as "Ad Type",
  ad_units.product_category_type as "Product Category Type",
  sum(numbers.ad_calls) as "Ad Calls",
  sum(numbers.blacklisted_ad_calls) as "Blacklisted Ad Calls",
  sum(numbers.served_ad_calls) as "Served Ad Calls",
  sum(numbers.interactions) as Interactions,
  sum(numbers.clicks) as Clicks,
  sum(numbers.fraudulent_clicks) as "Fraudulent Clicks", 
  sum(numbers.sum_actual_cpc) as "Gross Media Revenue"
from
(
  select *
  from
    (select
        distinct(requested_at_date_in_et) as requested_at_date_in_et
    from intent_media_log_data_production.ad_calls
    where requested_at_date_in_et >= (date(current_timestamp at timezone 'America/New_York') - interval '38 days')
        and requested_at_date_in_et < date(current_timestamp at timezone 'America/New_York')) dates,
    (select
        distinct(ad_unit_id) as ad_unit_id
    from intent_media_log_data_production.ad_calls
    where requested_at_date_in_et >= (date(current_timestamp at timezone 'America/New_York') - interval '38 days')
        and requested_at_date_in_et < date(current_timestamp at timezone 'America/New_York')) ad_units,
    (select
        ifnull(browser_family,'NULL') as browser_family,
        ifnull(browser,'NULL') as browser
    from intent_media_log_data_production.ad_calls
    where requested_at_date_in_et >= (date(current_timestamp at timezone 'America/New_York') - interval '38 days')
        and requested_at_date_in_et < date(current_timestamp at timezone 'America/New_York')
    group by
        browser_family,
        browser) browser
) dimensions
left join
(
  select
    requested_at_date_in_et,
    site_id,
    product_category_type,
    ad_unit_id,
    browser_family,
    browser,
    count(case when ip_address_blacklisted = 0 then request_id end) as ad_calls,
    count(case when ip_address_blacklisted = 1 then request_id end) as blacklisted_ad_calls,
    count(case when ip_address_blacklisted = 0 and outcome_type = 'SERVED' then request_id end) as served_ad_calls,
    sum(interactions) as interactions,
    sum(clicks) as clicks,
    sum(fraudulent_clicks) as fraudulent_clicks,
    sum(sum_actual_cpc) as sum_actual_cpc
  from
  (
    select
      min(ac.requested_at_date_in_et) as requested_at_date_in_et,
      min(ac.site_id) as site_id,
      min(ac.product_category_type) as product_category_type,
      min(ac.ad_unit_id) as ad_unit_id,
      min(ac.browser_family) as browser_family,
      min(ac.browser) as browser,
      min(ac.outcome_type) as outcome_type,
      ac.ip_address_blacklisted,
      ac.request_id,
      case when count(case when c.fraudulent = 0 then c.request_id end) > 0 then 1 else 0 end as interactions,
      count(case when c.fraudulent = 0 then c.request_id end) as clicks,
      count(case when c.fraudulent = 1 then c.request_id end) as fraudulent_clicks,
      sum(case when c.fraudulent = 0 then c.actual_cpc end) as sum_actual_cpc
    from intent_media_log_data_production.ad_calls ac
    left join intent_media_log_data_production.clicks c
    on ac.request_id = c.ad_call_request_id
    and c.requested_at_date_in_et >= (date(current_timestamp at timezone 'America/New_York') - interval '38 days')
    and c.requested_at_date_in_et < date(current_timestamp at timezone 'America/New_York')
    and c.ip_address_blacklisted = 0
    where ac.requested_at_date_in_et >= (date(current_timestamp at timezone 'America/New_York') - interval '38 days')
      and ac.requested_at_date_in_et < date(current_timestamp at timezone 'America/New_York')
    group by
      ac.ip_address_blacklisted,
      ac.request_id
  ) media
  group by
    requested_at_date_in_et,
    site_id,
    product_category_type,
    ad_unit_id,
    browser_family,
    browser
) numbers
on dimensions.requested_at_date_in_et = numbers.requested_at_date_in_et
and dimensions.ad_unit_id = numbers.ad_unit_id
and dimensions.browser_family = numbers.browser_family
and dimensions.browser = numbers.browser
left join intent_media_production.ad_units on ad_units.id = dimensions.ad_unit_id
left join intent_media_production.sites on sites.id = ad_units.site_id
where ad_units.active = 1
group by
  dimensions.requested_at_date_in_et,
  dimensions.ad_unit_id,
  dimensions.browser_family,
  dimensions.browser,
  sites.id,
  sites.display_name,
  ad_units.name,
  ad_units.ad_type,
  ad_units.product_category_type