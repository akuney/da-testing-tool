select  
  ac.requested_at_date_in_et as date,
  au.name as ad_unit_name,
  au.id as ad_unit_id,
  ac.site_reporting_value_01,
  sum(c.actual_cpc) as gross_revenue,
  count(distinct ac.request_id) as ad_calls,
  count(distinct case when ac.outcome_type = 'SERVED' then ac.request_id end) as served,
  count(distinct case when ac.outcome_type = 'SUPPRESSED_BY_NO_VALID_LAYOUT' then ac.request_id end) as no_valid_layout,
  count(distinct c.request_id) as clicks
from intent_media_log_data_production.ad_calls ac
inner join intent_media_production.ad_units au on ac.ad_unit_id = au.id
left join intent_media_log_data_production.clicks c
on ac.request_id = c.ad_call_request_id
and c.ip_address_blacklisted = 0
and c.fraudulent = 0
and c.publisher_id = 103
and c.requested_at_date_in_et >= '2014-08-01'
and c.requested_at_in_et <= ac.requested_at_in_et + interval '24 hour'
where au.name like '%Hipmunk%'
  and au.active = 1
  and ac.ip_address_blacklisted = 0
  and ac.requested_at_date_in_et >= '2014-08-01'
group by
  ac.requested_at_date_in_et,
  au.name,
  au.id,
  ac.site_reporting_value_01
order by
  ac.requested_at_date_in_et desc,
  au.id,
  au.name,
  ac.site_reporting_value_01,
  sum(c.actual_cpc)
