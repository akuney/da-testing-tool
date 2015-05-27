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
    sum(data.ad_calls) as "Ad Calls",
    sum(data.blacklisted_ad_calls) as "Blacklisted Ad Calls",
    sum(data.served_ad_calls) as "Served Ad Calls",
    sum(data.clicks) as Clicks,
    sum(data.fraudulent_clicks) as "Fraudulent Clicks",
    sum(data.interactions) as Interactions,
    sum(data.sum_actual_cpc) as "Gross Media Revenue"
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
          distinct(site_type) as site_type
      from intent_media_log_data_production.ad_calls
      where requested_at_date_in_et >= (date(current_timestamp at timezone 'America/New_York') - interval '38 days')
          and requested_at_date_in_et < date(current_timestamp at timezone 'America/New_York')) site_type,
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
      ac.requested_at_date_in_et,
      ac.site_type,
      ac.product_category_type,
      ac.ad_unit_id,
      ac.browser_family,
      ac.browser,
      sum(ac.ad_calls) as ad_calls,
      sum(ac.blacklisted_ad_calls) as blacklisted_ad_calls,
      sum(ac.served_ad_calls) as served_ad_calls,
      sum(c.clicks) as clicks,
      sum(c.fraudulent_clicks) as fraudulent_clicks,
      sum(c.interactions) as interactions,
      sum(c.sum_actual_cpc) as sum_actual_cpc
    from
      (
        select
          requested_at_date_in_et,
          site_type,
          product_category_type,
          ad_unit_id,
          browser_family,
          browser,
          request_id,
          count(case when ip_address_blacklisted = 0 then request_id end) as ad_calls,
          count(case when ip_address_blacklisted = 1 then request_id end) as blacklisted_ad_calls,
          count(case when ip_address_blacklisted = 0 and outcome_type = 'SERVED' then request_id end) as served_ad_calls
        from intent_media_log_data_production.ad_calls
        where requested_at_date_in_et >= (date(current_timestamp at timezone 'America/New_York') - interval '38 days')
          and requested_at_date_in_et < date(current_timestamp at timezone 'America/New_York')
        group by
          requested_at_date_in_et,
          site_type,
          product_category_type,
          ad_unit_id,
          browser_family,
          browser,
          request_id
      ) ac
    left join
      (
        select
          c.requested_at_date_in_et,
          c.site_type,
          c.product_category_type,
          au.id as ad_unit_id,
          c.browser_family,
          c.browser,
          c.ad_call_request_id,
          count(case when fraudulent = 0 then request_id end) as clicks,
          count(case when fraudulent = 1 then request_id end) as fraudulent_clicks,
          case when count(case when fraudulent = 0 then request_id end) > 0 then 1 else 0 end as interactions,
          sum(case when fraudulent = 0 then actual_cpc end) as sum_actual_cpc
        from intent_media_log_data_production.clicks c
        left join intent_media_production.sites s on c.site_type = s.name
        left join intent_media_production.ad_units au on s.id = au.site_id
        where requested_at_date_in_et >= (date(current_timestamp at timezone 'America/New_York') - interval '38 days')
          and requested_at_date_in_et < date(current_timestamp at timezone 'America/New_York')
          and ip_address_blacklisted = 0
        group by
          c.requested_at_date_in_et,
          c.site_type,
          c.product_category_type,
          au.id,
          c.browser_family,
          c.browser,
          c.ad_call_request_id
      ) c
    on ac.requested_at_date_in_et = c.requested_at_date_in_et
      and ac.site_type = c.site_type
      and ac.product_category_type = c.product_category_type
      and ac.ad_unit_id = c.ad_unit_id
      and ac.browser_family = c.browser_family
      and ac.browser = c.browser
      and ac.request_id = c.ad_call_request_id
    group by
      ac.requested_at_date_in_et,
      ac.site_type,
      ac.product_category_type,
      ac.ad_unit_id,
      ac.browser_family,
      ac.browser
  ) data
on dimensions.requested_at_date_in_et = data.requested_at_date_in_et
  and dimensions.site_type = data.site_type
  and dimensions.ad_unit_id = data.ad_unit_id
  and dimensions.browser_family = data.browser_family
  and dimensions.browser = data.browser
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