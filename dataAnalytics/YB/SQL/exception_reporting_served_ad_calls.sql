/* Site */
select s.display_name as site_name, case when (served_ad_calls - ma_served_ad_calls)/sqrt(var_served_ad_calls) <= -3 then 'drop exception' end as exception_flag
from
(
  select site_id, requested_at_date_in_et, served_ad_calls,
    avg(served_ad_calls) over(partition by site_id order by requested_at_date_in_et range between '28 day' preceding and current row) as ma_served_ad_calls,
    variance(served_ad_calls) over(partition by site_id order by requested_at_date_in_et) as var_served_ad_calls
  from
  (
    select site_id, requested_at_date_in_et, count(request_id) as served_ad_calls
    from intent_media_log_data_production.ad_calls
    where requested_at_date_in_et >= current_timestamp at timezone 'America/New_York' - interval '30 days'
      and ip_address_blacklisted = 0
      and outcome_type = 'SERVED'
      and ad_unit_type = 'CT'
    group by 1,2
    order by 1,2
  ) base
  where served_ad_calls > 1000
) final
inner join intent_media_production.sites s on final.site_id = s.id
where requested_at_date_in_et = date(current_timestamp at timezone 'America/New_York') - interval '1 day'
order by 1;

/* Site, Ad Unit */
select s.display_name as site_name, au.name as ad_unit_name,
  case when (served_ad_calls - ma_served_ad_calls)/sqrt(var_served_ad_calls) <= -3 then 'drop exception' end as exception_flag
from
(
  select site_id, ad_unit_id, requested_at_date_in_et, served_ad_calls,
    avg(served_ad_calls) over(partition by site_id, ad_unit_id order by requested_at_date_in_et range between '28 day' preceding and current row) as ma_served_ad_calls,
    variance(served_ad_calls) over(partition by site_id, ad_unit_id order by requested_at_date_in_et) as var_served_ad_calls
  from
  (
    select site_id, ad_unit_id, requested_at_date_in_et, count(request_id) as served_ad_calls
    from intent_media_log_data_production.ad_calls
    where requested_at_date_in_et >= current_timestamp at timezone 'America/New_York' - interval '30 days'
      and ip_address_blacklisted = 0
      and outcome_type = 'SERVED'
      and ad_unit_type = 'CT'
    group by 1,2,3
    order by 1,2,3
  ) base
  where served_ad_calls > 1000
) final
inner join intent_media_production.sites s on final.site_id = s.id
inner join intent_media_production.ad_units au on final.ad_unit_id = au.id
where requested_at_date_in_et = date(current_timestamp at timezone 'America/New_York') - interval '1 day'
order by 1,2;

/* Site, Browser */
select s.display_name as site_name, browser_family, 
  case when (served_ad_calls - ma_served_ad_calls)/sqrt(var_served_ad_calls) <= -3 then 'drop exception' end as exception_flag
from
(
  select site_id, browser_family, requested_at_date_in_et, served_ad_calls,
    avg(served_ad_calls) over(partition by site_id, browser_family order by requested_at_date_in_et range between '28 day' preceding and current row) as ma_served_ad_calls,
    variance(served_ad_calls) over(partition by site_id, browser_family order by requested_at_date_in_et) as var_served_ad_calls
  from
  (
    select site_id, browser_family, requested_at_date_in_et, count(request_id) as served_ad_calls
    from intent_media_log_data_production.ad_calls
    where requested_at_date_in_et >= current_timestamp at timezone 'America/New_York' - interval '30 days'
      and ip_address_blacklisted = 0
      and outcome_type = 'SERVED'
      and ad_unit_type = 'CT'
    group by 1,2,3
    order by 1,2,3
  ) base
  where served_ad_calls > 1000
) final
inner join intent_media_production.sites s on final.site_id = s.id
where requested_at_date_in_et = date(current_timestamp at timezone 'America/New_York') - interval '1 day'
order by 1,2;

/* Site, Ad Unit, Browser */
select s.display_name as site_name, au.name as ad_unit_name, browser_family, 
  case when (served_ad_calls - ma_served_ad_calls)/sqrt(var_served_ad_calls) <= -3 then 'drop exception' end as exception_flag
from
(
  select site_id, ad_unit_id, browser_family, requested_at_date_in_et, served_ad_calls,
    avg(served_ad_calls) over(partition by site_id, ad_unit_id, browser_family order by requested_at_date_in_et range between '28 day' preceding and current row) as ma_served_ad_calls,
    variance(served_ad_calls) over(partition by site_id, ad_unit_id, browser_family order by requested_at_date_in_et) as var_served_ad_calls
  from
  (
    select site_id, ad_unit_id, browser_family, requested_at_date_in_et, count(request_id) as served_ad_calls
    from intent_media_log_data_production.ad_calls
    where requested_at_date_in_et >= current_timestamp at timezone 'America/New_York' - interval '30 days'
      and ip_address_blacklisted = 0
      and outcome_type = 'SERVED'
      and ad_unit_type = 'CT'
    group by 1,2,3,4
    order by 1,2,3,4
  ) base
  where served_ad_calls > 1000
) final
inner join intent_media_production.sites s on final.site_id = s.id
inner join intent_media_production.ad_units au on final.ad_unit_id = au.id
where requested_at_date_in_et = date(current_timestamp at timezone 'America/New_York') - interval '1 day'
order by 1,2,3;