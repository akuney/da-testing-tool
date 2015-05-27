-- test test --

/* SCA Flights */
select
	s.display_name as site,
	'SCA Flights' as product,
	data.ad_unit_id,
	data.requested_at_date_in_et,
	data.outcome_type,
	data.os,
	data.browser_family,
	data.browser,
	au.name as ad_unit_name,
	au.active as ad_unit_active,
	data.unique_visitors,
	data.page_views,
	data.served_page_views,
	data.interactions,
	data.clicks,
	data.click_revenue
from
(
  select
    ad_unit_id,
    requested_at_date_in_et,
    outcome_type,
    os,
    browser_family,
    browser,
    count(distinct publisher_user_id) as unique_visitors,
    count(request_id) as page_views,
    count(case when outcome_type = 'SERVED' then request_id end) as served_page_views,
    sum(interactions) as interactions,
    sum(clicks) as clicks,
    sum(revenue) as click_revenue
  from
  /* Join Ad Calls with Clicks */
  (
    select
      ac.request_id,
      min(ac.requested_at_date_in_et) as requested_at_date_in_et,
      case when min(ac.publisher_user_id) is null then min(ac.webuser_id) else min(ac.publisher_user_id) end as publisher_user_id,
      min(ac.ad_unit_id) as ad_unit_id,
      min(ac.ad_unit_type) as ad_unit_type,
      min(ac.outcome_type) as outcome_type,
      min(ac.positions_filled) as positions_filled,
      min(ac.browser_family) as browser_family,
      min(ac.browser) as browser,
      min(ac.os_family) as os_family,
      min(ac.os) as os,
      case when count(c.request_id) > 0 then 1 else 0 end as interactions,
      count(c.request_id) as clicks,
      sum(c.actual_cpc) as revenue
    from
    /* Load Ad Calls */
    (
      select
        request_id,
        request_correlation_id,
        requested_at,
        requested_at_date_in_et,
        publisher_user_id,
        webuser_id,
        browser_family,
        browser,
        os_family,
        os,
        ad_unit_id,
        ad_unit_type,
        outcome_type,
        positions_filled
      from intent_media_log_data_production.ad_calls
      where requested_at_date_in_et between '2014-06-15' and '2014-06-21'
        and ip_address_blacklisted = 0
        and ad_unit_type = 'CT'
        and product_category_type = 'FLIGHTS'
    ) ac
    left join
    /* Load Clicks and CPC per Ad Calls */
    (
      select
        ad_call_request_id,
        requested_at,
        request_id,
        actual_cpc
      from intent_media_log_data_production.clicks
      where requested_at_date_in_et between '2014-06-15' and '2014-06-22'
        and ip_address_blacklisted = 0
        and fraudulent = 0
        and product_category_type = 'FLIGHTS'
    ) c
    on ac.request_id = c.ad_call_request_id
    group by
      ac.request_id
  ) ac_c
  group by
    ad_unit_id,
    requested_at_date_in_et,
    outcome_type,
    os,
    browser_family,
    browser
  ) data
left join intent_media_production.ad_units au on data.ad_unit_id = au.id
left join intent_media_production.sites s on au.site_id = s.id

union

select
	s.display_name as site,
	'SCA Hotels' as product,
	data.ad_unit_id,
	data.requested_at_date_in_et,
	data.outcome_type,
	data.os,
	data.browser_family,
	data.browser,
	au.name as ad_unit_name,
	au.active as ad_unit_active,
	data.unique_visitors,
	data.page_views,
	data.served_page_views,
	data.interactions,
	data.clicks,
	data.click_revenue
from
(
  select
    ad_unit_id,
    requested_at_date_in_et,
    outcome_type,
    os,
    browser_family,
    browser,
    count(distinct publisher_user_id) as unique_visitors,
    count(request_id) as page_views,
    count(case when outcome_type = 'SERVED' then request_id end) as served_page_views,
    sum(interactions) as interactions,
    sum(clicks) as clicks,
    sum(revenue) as click_revenue
  from
  /* Join Ad Calls with Clicks */
  (
    select
      ac.request_id,
      min(ac.requested_at_date_in_et) as requested_at_date_in_et,
      case when min(ac.publisher_user_id) is null then min(ac.webuser_id) else min(ac.publisher_user_id) end as publisher_user_id,
      min(ac.ad_unit_id) as ad_unit_id,
      min(ac.ad_unit_type) as ad_unit_type,
      min(ac.outcome_type) as outcome_type,
      min(ac.positions_filled) as positions_filled,
      min(ac.browser_family) as browser_family,
      min(ac.browser) as browser,
      min(ac.os_family) as os_family,
      min(ac.os) as os,
      case when count(c.request_id) > 0 then 1 else 0 end as interactions,
      count(c.request_id) as clicks,
      sum(c.actual_cpc) as revenue
    from
    /* Load Ad Calls */
    (
      select
        request_id,
        request_correlation_id,
        requested_at,
        requested_at_date_in_et,
        publisher_user_id,
        webuser_id,
        browser_family,
        browser,
        os_family,
        os,
        ad_unit_id,
        ad_unit_type,
        outcome_type,
        positions_filled
      from intent_media_log_data_production.ad_calls
      where requested_at_date_in_et between '2014-06-15' and '2014-06-21'
        and ip_address_blacklisted = 0
        and ad_unit_type = 'CT'
        and product_category_type = 'HOTELS'
    ) ac
    left join
    /* Load Clicks and CPC per Ad Calls */
    (
      select
        ad_call_request_id,
        requested_at,
        request_id,
        actual_cpc
      from intent_media_log_data_production.clicks
      where requested_at_date_in_et between '2014-06-15' and '2014-06-22'
        and ip_address_blacklisted = 0
        and fraudulent = 0
        and product_category_type = 'HOTELS'
    ) c
    on ac.request_id = c.ad_call_request_id
    group by
      ac.request_id
  ) ac_c
  group by
    ad_unit_id,
    requested_at_date_in_et,
    outcome_type,
    os,
    browser_family,
    browser
  ) data
left join intent_media_production.ad_units au on data.ad_unit_id = au.id
left join intent_media_production.sites s on au.site_id = s.id

union

select
	s.display_name as site,
	'SSN Hotels' as product,
	data.ad_unit_id,
	data.requested_at_date_in_et,
	data.outcome_type,
	data.os,
	data.browser_family,
	data.browser,
	au.name as ad_unit_name,
	au.active as ad_unit_active,
	data.unique_visitors,
	data.page_views,
	data.served_page_views,
	data.interactions,
	data.clicks,
	data.click_revenue
from
(
  select
    ad_unit_id,
    requested_at_date_in_et,
    outcome_type,
    os,
    browser_family,
    browser,
    count(distinct publisher_user_id) as unique_visitors,
    count(request_id) as page_views,
    count(case when outcome_type = 'SERVED' and positions_filled > 0 then request_id end) as served_page_views,
    sum(interactions) as interactions,
    sum(clicks) as clicks,
    sum(revenue) as click_revenue
  from
  /* Join Ad Calls with Clicks */
  (
    select
      ac.request_id,
      min(ac.requested_at_date_in_et) as requested_at_date_in_et,
      case when min(ac.publisher_user_id) is null then min(ac.webuser_id) else min(ac.publisher_user_id) end as publisher_user_id,
      min(ac.ad_unit_id) as ad_unit_id,
      min(ac.ad_unit_type) as ad_unit_type,
      min(ac.outcome_type) as outcome_type,
      min(ac.positions_filled) as positions_filled,
      min(ac.browser_family) as browser_family,
      min(ac.browser) as browser,
      min(ac.os_family) as os_family,
      min(ac.os) as os,
      case when count(c.request_id) > 0 then 1 else 0 end as interactions,
      count(c.request_id) as clicks,
      sum(c.actual_cpc) as revenue
    from
    /* Load Ad Calls */
    (
      select
        request_id,
        request_correlation_id,
        requested_at,
        requested_at_date_in_et,
        publisher_user_id,
        webuser_id,
        browser_family,
        browser,
        os_family,
        os,
        ad_unit_id,
        ad_unit_type,
        outcome_type,
        positions_filled
      from intent_media_log_data_production.ad_calls
      where requested_at_date_in_et between '2014-06-15' and '2014-06-21'
        and ip_address_blacklisted = 0
        and ad_unit_type = 'SSR'
        and product_category_type = 'HOTELS'
    ) ac
    left join
    /* Load Clicks and CPC per Ad Calls */
    (
      select
        ad_call_request_id,
        requested_at,
        request_id,
        actual_cpc
      from intent_media_log_data_production.clicks
      where requested_at_date_in_et between '2014-06-15' and '2014-06-22'
        and ip_address_blacklisted = 0
        and fraudulent = 0
        and product_category_type = 'HOTELS'
    ) c
    on ac.request_id = c.ad_call_request_id
    group by
      ac.request_id
  ) ac_c
  group by
    ad_unit_id,
    requested_at_date_in_et,
    outcome_type,
    os,
    browser_family,
    browser
  ) data
left join intent_media_production.ad_units au on data.ad_unit_id = au.id
left join intent_media_production.sites s on au.site_id = s.id

union

select
	s.display_name as site,
	'PPA Hotels' as product,
	data.ad_unit_id,
	data.requested_at_date_in_et,
	data.outcome_type,
	data.os,
	data.browser_family,
	data.browser,
	au.name as ad_unit_name,
	au.active as ad_unit_active,
	data.unique_visitors,
	data.page_views,
	data.served_page_views,
	data.interactions,
	data.clicks,
	data.click_revenue
from
(
  select
    ad_unit_id,
    requested_at_date_in_et,
    outcome_type,
    os,
    browser_family,
    browser,
    count(distinct publisher_user_id) as unique_visitors,
    count(request_id) as page_views,
    count(case when i_request_id is not null then request_id end) as served_page_views,
    sum(interactions) as interactions,
    sum(clicks) as clicks,
    sum(revenue) as click_revenue
  from
  /* Join Ad Calls with Clicks */
  (
    select
      ac.request_correlation_id as request_id,
      min(i.request_id) as i_request_id,
      min(ac.requested_at_date_in_et) as requested_at_date_in_et,
      case when min(ac.publisher_user_id) is null then min(ac.webuser_id) else min(ac.publisher_user_id) end as publisher_user_id,
      min(ac.ad_unit_id) as ad_unit_id,
      min(ac.ad_unit_type) as ad_unit_type,
      min(ac.outcome_type) as outcome_type,
      min(ac.positions_filled) as positions_filled,
      min(ac.browser_family) as browser_family,
      min(ac.browser) as browser,
      min(ac.os_family) as os_family,
      min(ac.os) as os,
      case when count(c.request_id) > 0 then 1 else 0 end as interactions,
      count(c.request_id) as clicks,
      sum(c.actual_cpc) as revenue
    from
    /* Load Ad Calls */
    (
      select
        request_id,
        request_correlation_id,
        requested_at,
        requested_at_date_in_et,
        publisher_user_id,
        webuser_id,
        browser_family,
        browser,
        os_family,
        os,
        ad_unit_id,
        ad_unit_type,
        outcome_type,
        positions_filled
      from intent_media_log_data_production.ad_calls
      where requested_at_date_in_et between '2014-06-15' and '2014-06-21'
        and ip_address_blacklisted = 0
    ) ac
    left join
    /* Load Clicks and CPC per Ad Calls */
    (
      select
        ad_call_request_id,
        requested_at,
        request_id,
        actual_cpc
      from intent_media_log_data_production.clicks
      where requested_at_date_in_et between '2014-06-15' and '2014-06-22'
        and ip_address_blacklisted = 0
        and fraudulent = 0
    ) c
    on ac.request_id = c.ad_call_request_id
    left join
    (
      select
        distinct request_id
      from intent_media_log_data_production.impressions
      where requested_at_date_in_et between '2014-06-15' and '2014-06-21'
        and ip_address_blacklisted = 0
        and ad_unit_id = 129
    ) i
    on ac.request_id = i.request_id
    group by
      ac.request_correlation_id
  ) ac_c
  group by
    ad_unit_id,
    requested_at_date_in_et,
    outcome_type,
    os,
    browser_family,
    browser
  ) data
left join intent_media_production.ad_units au on data.ad_unit_id = au.id
left join intent_media_production.sites s on au.site_id = s.id
