select 
    hotel_property_id,
    count(*) as num_ad_calls
    from intent_media_log_data_production.ad_calls
    where requested_at_date_in_et between '2014-10-01' and '2014-10-31'
        and ip_address_blacklisted = false
        and ad_unit_type = 'META'
        and outcome_type = 'SERVED'
  group by hotel_property_id
  order by num_ad_calls desc;