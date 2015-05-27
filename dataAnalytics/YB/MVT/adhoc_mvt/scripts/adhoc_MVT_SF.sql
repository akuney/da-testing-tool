select
  sfe.multivariate_version_id,
  sfe.mvt_value,
  sfe.browser,
  sfe.click_type,
  acc.placement_type,
  count(sfe.publisher_user_id) as users,
  sum(sfe.page_load_count) as page_load_count,
  sum(sfe.page_load_count_2_u) as page_load_count_2_u,
  sum(acc.served_ad_calls) as served_ad_calls,
  sum(acc.interactions) as interactions,
  sum(acc.interactions * acc.interactions) as interactions_2_u,
  sum(acc.clicks) as clicks,
  sum(acc.clicks_2_ac) as clicks_2_ac,
  sum(acc.clicks_2_u) as clicks_2_u,
  sum(acc.revenue) as revenue,
  sum(acc.revenue_2_ac) as revenue_2_ac,
  sum(acc.revenue_2_u) as revenue_2_u
from
(
  select
    multivariate_version_id,
    ifnull(regexp_substr(multivariate_test_attributes_variable, '"FLIGHTS_ADVERTISEMENT_HEADLINE":"(.*?)"', 1, 1, '', 1), 'Not Found') as mvt_value,
    case browser_family when 'IE' then 'IE' when 'CHROME' then 'Chrome' when 'FIREFOX' then 'Firefox' when 'SAFARI' then 'Safari' else 'Other' end as browser,
    case browser_family when 'CHROME' then 'Single' when 'IE' then 'Single' else 'Multi' end as click_type,
    publisher_user_id,
    count(request_id) as page_load_count,
    count(request_id) * count(request_id) as page_load_count_2_u
  from intent_media_log_data_production.search_compare_form_events
  where requested_at_date_in_et >= date(current_timestamp at timezone 'America/New_York') - interval '30 days'
    and ip_address_blacklisted = 0
    and site_type = 'TRAVELZOO'
    and multivariate_test_attributes_variable like '%FLIGHTS_ADVERTISEMENT_HEADLINE%'
    and publisher_user_id is not null
    and publisher_user_id not in
      (
        select publisher_user_id
        from intent_media_log_data_production.search_compare_form_events
        where requested_at_date_in_et >= date(current_timestamp at timezone 'America/New_York') - interval '30 days'
          and ip_address_blacklisted = 0
          and site_type = 'TRAVELZOO'
          and multivariate_test_attributes_variable like '%FLIGHTS_ADVERTISEMENT_HEADLINE%'
        and publisher_user_id is not null
        group by publisher_user_id
        having count(distinct browser_family) > 1
      )
  group by
    multivariate_version_id,
    ifnull(regexp_substr(multivariate_test_attributes_variable, '"FLIGHTS_ADVERTISEMENT_HEADLINE":"(.*?)"', 1, 1, '', 1), 'Not Found'),
    case browser_family when 'IE' then 'IE' when 'CHROME' then 'Chrome' when 'FIREFOX' then 'Firefox' when 'SAFARI' then 'Safari' else 'Other' end,
    case browser_family when 'CHROME' then 'Single' when 'IE' then 'Single' else 'Multi' end,
    publisher_user_id
) sfe
left join
(
  select
    ac.publisher_user_id,
    case when count(distinct c.placement_type) > 1 then 'Mixed' else min(c.placement_type) end as placement_type,
    count(ac.request_id) as served_ad_calls,
    sum(c.interactions) as interactions,
    sum(c.interactions * c.interactions) as interactions_2_u,
    sum(c.clicks) as clicks,
    sum(c.clicks_2_ac) as clicks_2_ac,
    sum(c.clicks * c.clicks) as clicks_2_u,
    sum(c.revenue) as revenue,
    sum(c.revenue_2_ac) as revenue_2_ac,
    sum(c.revenue * c.revenue) as revenue_2_u
  from intent_media_log_data_production.ad_calls ac
  left join
  (
    select
      ad_call_request_id,
      case when count(distinct placement_type) > 1 then 'Mixed' else min(placement_type) end as placement_type,
      case when count(request_id) > 0 then 1 else 0 end as interactions,
      count(request_id) as clicks,
      (count(request_id) * count(request_id)) as clicks_2_ac,
      sum(actual_cpc) as revenue,
      (sum(actual_cpc) * sum(actual_cpc)) as revenue_2_ac
    from intent_media_log_data_production.clicks
    where date(ad_call_requested_at at timezone 'UTC' at timezone 'America/New_York') >= date(current_timestamp at timezone 'America/New_York') - interval '30 days'
      and publisher_user_id is not null
      and ip_address_blacklisted = 0
      and fraudulent = 0
      and site_type = 'TRAVELZOO'
    group by
      ad_call_request_id
  ) c
  on ac.request_id = c.ad_call_request_id
  where ac.requested_at_date_in_et >= date(current_timestamp at timezone 'America/New_York') - interval '30 days'
    and ac.ip_address_blacklisted = 0
    and ac.outcome_type = 'SERVED'
    and ac.site_type = 'TRAVELZOO'
    and ac.multivariate_test_attributes_variable like '%FLIGHTS_ADVERTISEMENT_HEADLINE%'
    and ac.publisher_user_id is not null
    and ac.publisher_user_id not in
      (
        select publisher_user_id
        from intent_media_log_data_production.ad_calls
        where requested_at_date_in_et >= date(current_timestamp at timezone 'America/New_York') - interval '30 days'
          and ip_address_blacklisted = 0
          and outcome_type = 'SERVED'
          and site_type = 'TRAVELZOO'
          and multivariate_test_attributes_variable like '%FLIGHTS_ADVERTISEMENT_HEADLINE%'
        and publisher_user_id is not null
        group by publisher_user_id
        having count(distinct browser_family) > 1
      )
  group by
    ac.publisher_user_id
) acc
on sfe.publisher_user_id = acc.publisher_user_id
group by
  sfe.multivariate_version_id,
  sfe.mvt_value,
  sfe.browser,
  sfe.click_type,
  acc.placement_type