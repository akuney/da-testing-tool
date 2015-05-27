/* Create a table from ad calls */
drop table if exists intent_media_sandbox_production.YB_orbitz_hotel_list_page_ac;
create table intent_media_sandbox_production.YB_orbitz_hotel_list_page_ac as 
select
  trim(regexp_substr(multivariate_test_attributes_variable, '"DAYS_TO_ARRIVAL_ABORT":"(.*?)"', 1, 1, '', 1)) as mvt_value,
  ad_unit_id, 
  outcome_type, 
  request_id, 
  requested_at_in_et as ad_call_timestamp, 
  positions_filled,
  publisher_user_id,
  webuser_id
from intent_media_log_data_production.ad_calls
where requested_at_date_in_et between '2014-09-03' and '2014-10-28'
  and ip_address_blacklisted = 0  
  and ad_unit_id = 124;

/* Create a table which joins the above ad calls table to impressions */
drop table if exists intent_media_sandbox_production.YB_orbitz_hotel_list_page_i;
create table intent_media_sandbox_production.YB_orbitz_hotel_list_page_i as 
select 
  ac.*,
  i.external_id,
  i.advertiser_id,
  i.auction_position
from intent_media_sandbox_production.YB_orbitz_hotel_list_page_ac ac
left join intent_media_log_data_production.impressions i 
on ac.request_id = i.request_id
and i.requested_at_date_in_et between '2014-09-03' and '2014-10-28'
and i.ip_address_blacklisted = 0
and i.ad_unit_id = 124;

/* Create a table which joins the above ad calls and impressions to clicks */
drop table if exists intent_media_sandbox_production.YB_orbitz_hotel_list_page_c;
create table intent_media_sandbox_production.YB_orbitz_hotel_list_page_c as 
select 
  i.*,
  c.request_id as click_request_id, 
  c.actual_cpc as click_actual_cpc,  
  c.advertisement_id as click_advertisement_id, 
  c.requested_at_in_et as click_timestamp
from intent_media_sandbox_production.YB_orbitz_hotel_list_page_i i
left join intent_media_log_data_production.clicks c 
on i.external_id = c.external_impression_id
and c.ip_address_blacklisted = 0
and c.fraudulent = 0;

/* Create a table from conversions */
drop table if exists intent_media_sandbox_production.YB_orbitz_hotel_list_page_bookings;
create table intent_media_sandbox_production.YB_orbitz_hotel_list_page_bookings as 
select
  site_type, 
  request_id as booking_request_id, 
  order_id,
  publisher_user_id, 
  webuser_id,
  requested_at_date_in_et as booking_date, 
  requested_at_in_et as booking_timestamp, 
  entity_id, 
  conversion_value
from intent_media_log_data_production.conversions
where requested_at_date_in_et between '2014-09-03' and '2014-10-28'
  and ip_address_blacklisted = 0
  and site_type = 'ORBITZ_GLOBAL'
  and product_category_type = 'HOTELS';

/* Create a table which joins the above ad calls, impressions, clicks, and conversions */  
drop table if exists intent_media_sandbox_production.YB_orbitz_hotel_list_page_cb;
create table intent_media_sandbox_production.YB_orbitz_hotel_list_page_cb as
select 
  c.*,
  b.booking_request_id,
  b.order_id, 
  b.entity_id, 
  b.conversion_value, 
  b.booking_timestamp 
from intent_media_sandbox_production.YB_orbitz_hotel_list_page_c c 
left join intent_media_sandbox_production.YB_orbitz_hotel_list_page_bookings b
on b.entity_id = c.advertiser_id 
and b.publisher_user_id = c.publisher_user_id
and b.booking_timestamp >= c.click_timestamp;

/* Media numbers */
select
  mvt_value,
  count(distinct publisher_user_id) as visitors,
  count(distinct request_id) as ad_calls,
  count(distinct(case when outcome_type = 'SERVED' then request_id end)) as pages_available,
  count(distinct(case when outcome_type = 'SERVED' and positions_filled > 0 then request_id end)) as pages_served,
  count(1) as impressions,
  count(click_request_id) as clicks,
  sum(c lick_actual_cpc) as gross_media_revenue,
  sum(click_actual_cpc * 0.752) as net_media_revenue
from intent_media_sandbox_production.YB_orbitz_hotel_list_page_cb  
group by
  mvt_value;

/* Attributed bookers (deduped) */
select
  mvt_value,
  count(publisher_user_id) as bookers,
  sum(conversions) as conversions,
  sum(conversions_2_u) as conversions_2_u,
  sum(conversion_value) as conversion_value,
  sum(conversion_value_2_u) as conversion_value_2_u
from
(
  select
    min(mvt_value) as mvt_value,
    publisher_user_id,
    count(booking_request_id) as conversions,
    count(booking_request_id) * count(booking_request_id) as conversions_2_u,
    sum(conversion_value) as conversion_value,
    sum(conversion_value) * sum(conversion_value) as conversion_value_2_u
  from
  (
    select 
      mvt_value,
      publisher_user_id,
      conversion_value,
      booking_request_id,
      rank() over (partition by order_id, entity_id order by booking_timestamp) as rank
    from intent_media_sandbox_production.YB_orbitz_hotel_list_page_cb
    where click_request_id is not null
      and ad_call_timestamp < booking_timestamp
  ) deduped
  where rank = 1
  group by
    publisher_user_id
) per_booker
group by
  mvt_value;

/* Attributed bookers (not deduped) */
select
  mvt_value,
  count(publisher_user_id) as bookers,
  sum(is_booker) as bookers_2,
  sum(conversions) as conversions,
  sum(conversion_value) as gross_profit
from
(
  select
    min(mvt_value) as mvt_value,
    publisher_user_id,
    max(case when conversion_value > 0 then 1 else 0 end) as is_booker,
    count(booking_request_id) as conversions,
    count(booking_request_id) * count(booking_request_id) as conversions_2_u,
    sum(conversion_value) as conversion_value,
    sum(conversion_value) * sum(conversion_value) as conversion_value_2_u
  from intent_media_sandbox_production.YB_orbitz_hotel_list_page_cb
  where click_request_id is not null
    and ad_call_timestamp < booking_timestamp
  group by publisher_user_id
) per_booker
group by
  mvt_value;