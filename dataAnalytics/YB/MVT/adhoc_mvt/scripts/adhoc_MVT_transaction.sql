select
  page_type,
  traffic_share_type,
  pure_group_type,
  mvt_key,
  mvt_value,
  count(*) as visitors,
  sum(case when is_booker is null then 0 else is_booker end) as bookers,
  sum(case when total_conversion_value is null then 0 else total_conversion_value end) as gross_profit,
  ad_unit_type,
  path_product_category_type,
  conversion_product_category_type,
  site_id
from
(
  select
    uvs.publisher_user_id as publisher_user_id,
    uvs.traffic_share_type as traffic_share_type,
    uvs.mvt_key,
    uvs.mvt_value,
    max(case when conversions.net_conversion_value > 0 then 1 else 0 end) as is_booker,
    sum(conversions.net_conversion_value) as total_conversion_value,
    uvs.page_type,
    uvs.ad_unit_type,
    uvs.path_product_category_type,
    conversions.product_category_type as conversion_product_category_type,
    uvs.site_id,
    uvs.pure_group_type
  from
  (
    select
      ac.publisher_user_id,
      'MAP_UI_INTERCARD_LAYOUT_VARIATION' as mvt_key,
      case when false then site_reporting_value_01 else ifnull(trim(trailing '"' from regexp_substr(ac.multivariate_test_attributes_variable, '"MAP_UI_INTERCARD_LAYOUT_VARIATION":"(.*?")[,}]', 1, 1, '', 1)), 'Not Found') end as mvt_value,
      ac.product_category_type as path_product_category_type,
      max(ac.traffic_share_type) as traffic_share_type,
      min(ac.requested_at_in_et) as first_ad_call_time,
      ac.ad_unit_type,
      ac.site_id,
      ac.pure_group_type,
      lpt.page_type
    from intent_media_log_data_production.ad_calls ac
    left join intent_media_production.ad_units au on ac.ad_unit_id = au.id
    left join intent_media_production.legal_page_types lpt on au.legal_page_type_id = lpt.id
    where ac.ad_unit_type = 'CT'
      and ac.site_id = 12
      and ac.ip_address_blacklisted = 0
      and (case when cast('HOTELS' as varchar) is null then true else ac.product_category_type = 'HOTELS' end)
      -- we are an intent defined experiment: filter by multivariate versions
      and (case when not(false) then ac.multivariate_version_id >= 2050 else true end)
      and (case when not(false) and cast(2086 as numeric) is not null then ac.multivariate_version_id <= 2086 else true end)
      -- we are a publisher defined experiment: filter by requested date
      and (case when false then date(ac.requested_at_date_in_et) >= '2015-01-01' else true end)
      and (case when false and date('2015-01-01') is not null then date(ac.requested_at_date_in_et) <= '2015-01-01' else true end)
    group by
      ac.publisher_user_id,
      mvt_value,
      path_product_category_type,
      ac.ad_unit_type,
      ac.site_id,
      ac.pure_group_type,
      lpt.page_type
  ) uvs
  left join
  (
    select
      publisher_user_id,
      order_id,
      round(net_conversion_value, 2) as groupable_net_conversion_value,
      min(requested_at_in_et) as requested_at_in_et,
      min(net_conversion_value) as net_conversion_value,
      min(product_category_type) as product_category_type,
      min(net_insurance_value) as net_insurance_value
    from intent_media_log_data_production.conversions
    where ip_address_blacklisted = 0
      and site_id = 12
      and entity_id = 85
      and publisher_user_id is not null
    group by
      publisher_user_id,
      order_id,
      round(net_conversion_value, 2)
  ) conversions
  on uvs.publisher_user_id = conversions.publisher_user_id
  and uvs.first_ad_call_time < conversions.requested_at_in_et
  group by
    uvs.publisher_user_id,
    uvs.traffic_share_type,
    uvs.mvt_key,
    uvs.mvt_value,
    uvs.page_type,
    uvs.ad_unit_type,
    uvs.path_product_category_type,
    conversions.product_category_type,
    uvs.site_id,
    uvs.pure_group_type
) pulled_data
group by
  traffic_share_type,
  pure_group_type,
  mvt_key,
  mvt_value,
  page_type,
  ad_unit_type,
  path_product_category_type,
  conversion_product_category_type,
  site_id
