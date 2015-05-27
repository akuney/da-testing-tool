select
      ac.hotel_property_id, 
      count(case when cl.request_id is not null then 1 else 0 end) as click_count
  from intent_media_log_data_production.clicks cl
  inner join intent_media_log_data_production.ad_calls ac
      on cl.ad_call_request_id = ac.request_id
  where cl.requested_at_date_in_et between '2014-10-01' and (date('2014-10-31') + interval '1 day')
      and ac.requested_at_date_in_et between '2014-10-01' and '2014-10-31'
      and ac.ip_address_blacklisted = 0
      and cl.ip_address_blacklisted = 0
      and cl.fraudulent = 0
      and cl.placement_type = 'IN_CARD'
      and ac.outcome_type = 'SERVED'
      and ac.ad_unit_type = 'META'
      and ac.product_category_type = 'HOTELS'
      group by ac.hotel_property_id;
