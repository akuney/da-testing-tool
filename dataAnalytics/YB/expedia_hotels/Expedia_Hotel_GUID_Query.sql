select
  lpt.page_type,
  ac.site_reporting_value_01,
  ac.publisher_user_id as GUID,
  count(ac.request_id) as served_ad_calls,
  sum(c.clicks) as clicks
from intent_media_log_data_production.ad_calls ac
left join
(
  select
    ad_call_request_id,
    count(request_id) as clicks
  from intent_media_log_data_production.clicks
  where ip_address_blacklisted = 0
    and fraudulent = 0
    and site_type = 'EXPEDIA'
    and product_category_type = 'HOTELS'
    and date(ad_call_requested_at) >= '2014-11-11'
  group by
    ad_call_request_id
) c
on ac.request_id = c.ad_call_request_id
inner join intent_media_production.ad_units au on ac.ad_unit_id = au.id
inner join intent_media_production.legal_page_types lpt on au.legal_page_type_id = lpt.id
where ac.ip_address_blacklisted = 0
  and ac.outcome_type = 'SERVED'
  and ac.site_type = 'EXPEDIA'
  and ac.ad_unit_type = 'CT'
  and ac.product_category_type = 'HOTELS'
  and ac.requested_at_date_in_et >= '2014-11-11'
group by
  lpt.page_type,
  ac.site_reporting_value_01, 
  ac.publisher_user_id