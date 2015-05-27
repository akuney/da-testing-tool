insert into :verticaSearchPath.mvt_c13n_media_experiment_results (
  experiment_id,
  site_type,
  ad_unit_type,
  product_category_type,
  mvt_key,
  mvt_value,
  browser,
  click_type,
  placement_type,
  sfe_users,
  ac_users,
  page_load_count,
  served_ad_calls,
  served_ad_calls_2_u_sfe,
  interactions,
  interactions_2_u_ac,
  interactions_2_u_sfe,
  clicks,
  clicks_2_ac,
  clicks_2_u_ac,
  clicks_2_u_sfe,
  revenue,
  revenue_2_ac,
  revenue_2_u_ac,
  revenue_2_u_sfe
)
select
  :experimentId as experiment_id,
  :siteType as site_type,
  :adUnitType as ad_unit_type,
  :productCategoryType as product_category_type,
  mvt_key,
  mvt_value,
  browser_family,
  placement_type,
  count(sfe_users) as sfe_users,
  count(ac_users) as ac_users,
  sum(page_load_count) as page_load_count,
  sum(served_ad_calls) as served_ad_calls,
  sum(served_ad_calls_2_u_sfe) as served_ad_calls_2_u_sfe,
  sum(interactions) as interactions,
  sum(interactions_2_u_ac) as interactions_2_u_ac,
  sum(interactions_2_u_sfe) as interactions_2_u_sfe,
  sum(clicks) as clicks,
  sum(clicks_2_ac) as clicks_2_ac,
  sum(clicks_2_u_ac) as clicks_2_u_ac,
  sum(clicks_2_u_sfe) as clicks_2_u_sfe,
  sum(revenue) as revenue,
  sum(revenue_2_ac) as revenue_2_ac,
  sum(revenue_2_u_ac) as revenue_2_u_ac,
  sum(revenue_2_u_sfe) as revenue_2_u_sfe
from
(
  select
    ifnull(sfe.mvt_key,acc.mvt_key) as mvt_key,
    ifnull(sfe.mvt_value,acc.mvt_value) as mvt_value,
    ifnull(sfe.browser_family,acc.browser_family) as browser_family,
    acc.placement_type,
    sfe.sfe_users,
    acc.ac_users,
    sfe.page_load_count,
    acc.served_ad_calls,
    acc.served_ad_calls * acc.served_ad_calls as served_ad_calls_2_u_sfe,
    acc.interactions,
    acc.interactions_2_u_ac,
    acc.interactions * acc.interactions as interactions_2_u_sfe,
    acc.clicks,
    acc.clicks_2_ac,
    acc.clicks_2_u_ac,
    acc.clicks * acc.clicks as clicks_2_u_sfe,
    acc.revenue,
    acc.revenue_2_ac,
    acc.revenue_2_u_ac,
    acc.revenue * acc.revenue as revenue_2_u_sfe
  from
  /* Search Compare Form Events */
  (
    select
      browser_family,
      ':mvtAttributeName' as mvt_key,
      case when :pubSettingsActive then NULL else trim(regexp_substr(multivariate_test_attributes_variable, '":mvtAttributeName":"(.*?)"', 1, 1, '', 1)) end as mvt_value,
      publisher_user_id as sfe_users,
      count(request_id) as page_load_count
    from :logDataSearchPath.search_compare_form_events
    where ip_address_blacklisted = 0
      and site_id = :siteId
      and product_category_type = :productCategoryType
      -- we are an intent defined experiment: filter by multivariate versions
      and (case when :pubSettingsActive then true else multivariate_version_id >= :mvtStartVersionId end)
      and (case when :pubSettingsActive then true when cast(:mvtEndVersionId as numeric) is null then true else multivariate_version_id <= :mvtEndVersionId end)
      -- we are a publisher defined experiment: filter by requested date
      and (case when not(:pubSettingsActive) then true else requested_at_date_in_et >= :requestStartDate end)
      and (case when not(:pubSettingsActive) then true when cast(:requestEndDate as date) is null then true else requested_at_date_in_et <= :requestEndDate end)
    group by
      browser_family,
      mvt_value,
      sfe_users
  ) sfe
  full outer join
  /* Ad Calls and Clicks */
  (
    select
      browser_family,
      ':mvtAttributeName' as mvt_key,
      case when :pubSettingsActive then :placeholderAttributeName else trim(regexp_substr(multivariate_test_attributes_variable, '":mvtAttributeName":"(.*?)"', 1, 1, '', 1)) end as mvt_value,
      publisher_user_id as ac_users,
      case when count(distinct c.placement_type) > 1 then 'Mixed' else min(c.placement_type) end as placement_type,
      count(request_id) as served_ad_calls,
      sum(interactions) as interactions,
      sum(interactions) * sum(interactions) as interactions_2_u_ac,
      sum(clicks) as clicks,
      sum(clicks_2_ac) as clicks_2_ac,
      sum(clicks) * sum(clicks) as clicks_2_u_ac,
      sum(revenue) as revenue,
      sum(revenue_2_ac) as revenue_2_ac,
      sum(revenue) * sum(revenue) as revenue_2_u_ac
    from :logDataSearchPath.ad_calls ac
    left join
    (
      select
        ad_call_request_id,
        case when count(distinct placement_type) > 1 then 'Mixed' else min(placement_type) end as placement_type,
        case when count(request_id) > 0 then 1 else 0 end as interactions,
        count(request_id) as clicks,
        count(request_id) * count(request_id) as clicks_2_ac,
        sum(actual_cpc) as revenue,
        sum(actual_cpc) * sum(actual_cpc) as revenue_2_ac
      from :logDataSearchPath.clicks
      where ip_address_blacklisted = 0
        and fraudulent = 0
        and site_type = :siteType
        and product_category_type = :productCategoryType
        -- we are an intent defined experiment: filter by multivariate versions
        and (case when :pubSettingsActive then true else multivariate_version_id >= :mvtStartVersionId end)
        and (case when :pubSettingsActive then true when cast(:mvtEndVersionId as numeric) is null then true else multivariate_version_id <= :mvtEndVersionId end)
        -- we are a publisher defined experiment: filter by requested date
        and (case when not(:pubSettingsActive) then true else date(ad_call_requested_at) >= :requestStartDate end)
        and (case when not(:pubSettingsActive) then true when cast(:requestEndDate as date) is null then true else date(ad_call_requested_at) <= :requestEndDate end)
      group by
        ad_call_request_id
    ) c
    on ac.request_id = c.ad_call_request_id
    where ac.ip_address_blacklisted = 0
      and ac.outcome_type = 'SERVED'
      and ac.site_id = :siteId
      and ac.ad_unit_type = :adUnitType
      and ac.product_category_type = :productCategoryType
      -- we are an intent defined experiment: filter by multivariate versions
      and (case when :pubSettingsActive then true else ac.multivariate_version_id >= :mvtStartVersionId end)
      and (case when :pubSettingsActive then true when cast(:mvtEndVersionId as numeric) is null then true else ac.multivariate_version_id <= :mvtEndVersionId end)
      -- we are a publisher defined experiment: filter by requested date
      and (case when not(:pubSettingsActive) then true else ac.requested_at_date_in_et >= :requestStartDate end)
      and (case when not(:pubSettingsActive) then true when cast(:requestEndDate as date) is null then true else ac.requested_at_date_in_et <= :requestEndDate end)
    group by
      browser_family,
      mvt_value,
      ac_users
  ) acc
  on sfe.browser_family = acc.browser_family
  and sfe.mvt_value = acc.mvt_value
  and sfe.sfe_users = acc.ac_users
) per_user
group by
  mvt_key,
  mvt_value,
  browser_family,
  placement_type;