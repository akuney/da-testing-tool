/* Valid clicks defined by "all clicks happened within 24 hours after the impression"
   Only include advertisers that have conversion pixel + not "Meta" advertiser
*/
drop table if exists intent_media_sandbox_production.YB_valid_clicks;
create table intent_media_sandbox_production.YB_valid_clicks as 
select
  i.request_id as impression_request_id,
  i.advertiser_id,
  i.auction_position,
  c.request_id as click_request_id,
  c.requested_at_in_et as click_requested_at_in_et,
  c.webuser_id,
  c.actual_cpc
from intent_media_log_data_production.impressions i
left join
(
  select
    external_impression_id,
    webuser_id,
    request_id,
    requested_at_in_et,
    actual_cpc
  from intent_media_log_data_production.clicks
  where requested_at_date_in_et between '2014-08-21' and '2014-08-28'
    and ip_address_blacklisted = 0
    and fraudulent = 0
    and external_impression_id is not null
) c
on i.external_id = c.external_impression_id
and i.requested_at_in_et + interval '24 hours' >= c.requested_at_in_et
where i.requested_at_date_in_et between '2014-08-21' and '2014-08-27'
  and i.ip_address_blacklisted = 0
  and i.advertiser_id in (59777, 148684, 61224, 148708, 70994, 157259, 60462, 155752, 106574, 66356, 152665, 180018, 137882, 169739, 62118, 87697, 122112, 59528);


/* Conversion De-Duplication for Non-null Order IDs */
drop table if exists intent_media_sandbox_production.YB_deduped_conversions;
create table intent_media_sandbox_production.YB_deduped_conversions as
select
  con.*
from
(
  select
    con.entity_id,
    con.order_id,
    min(con.conversion_requested_at_in_et) as min_requested_at_in_et
  from
  (
    select
      entity_id,
      order_id,
      requested_at_in_et as conversion_requested_at_in_et
    from intent_media_log_data_production.conversions
    where requested_at_date_in_et between '2014-08-21' and '2014-09-26'
      and ip_address_blacklisted = 0
      and order_id is not null
  ) con
  group by
    con.entity_id,
    con.order_id
) distinct_con
left join
(
  select
    site_type,
    entity_id,
    order_id,
    request_id as conversion_request_id,
    requested_at_in_et as conversion_requested_at_in_et,
    product_category_type,
    webuser_id,
    conversion_value,
    net_conversion_value
  from intent_media_log_data_production.conversions
  where requested_at_date_in_et between '2014-08-21' and '2014-09-26'
    and ip_address_blacklisted = 0
    and order_id is not null
) con
on con.entity_id = distinct_con.entity_id
and con.order_id = distinct_con.order_id
and con.conversion_requested_at_in_et = distinct_con.min_requested_at_in_et

union

select
  site_type,
  entity_id,
  order_id,
  request_id as conversion_request_id,
  requested_at_in_et as conversion_requested_at_in_et,
  product_category_type,
  webuser_id,
  conversion_value,
  net_conversion_value
from intent_media_log_data_production.conversions
where requested_at_date_in_et between '2014-08-21' and '2014-09-26'
  and ip_address_blacklisted = 0
  and order_id is null;

/* Join Valid Clicks & De-duplicated Conversions */
drop table if exists intent_media_sandbox_production.YB_valid_deduped_click_conversions;
create table intent_media_sandbox_production.YB_valid_deduped_click_conversions as
select
  vc.*,
  dc.entity_id,
  dc.order_id,
  dc.conversion_request_id,
  dc.conversion_requested_at_in_et,
  dc.product_category_type as conversion_product_category_type,
  dc.conversion_value,
  dc.net_conversion_value,
  rank() over (partition by dc.conversion_request_id order by vc.click_requested_at_in_et desc) as click_rank
from intent_media_sandbox_production.YB_valid_clicks vc
left join intent_media_sandbox_production.YB_deduped_conversions dc
on vc.webuser_id = dc.webuser_id
and vc.click_requested_at_in_et < dc.conversion_requested_at_in_et
and vc.click_requested_at_in_et + interval '30 days' >= dc.conversion_requested_at_in_et;

/* Join back to valid clicks with conversion metrics */
drop table if exists intent_media_sandbox_production.YB_click_conversion;
create table intent_media_sandbox_production.YB_click_conversion as
select
  vc.*,
  metrics.conversion_count_flights,
  metrics.conversion_count_hotels,
  metrics.conversion_count_packages,
  metrics.conversion_count_cars,
  metrics.conversion_count_total,
  metrics.conversion_value_sum_flights,
  metrics.conversion_value_sum_hotels,
  metrics.conversion_value_sum_packages,
  metrics.conversion_value_sum_cars,
  metrics.conversion_value_sum_total,
  metrics.net_conversion_value_sum_flights,
  metrics.net_conversion_value_sum_hotels,
  metrics.net_conversion_value_sum_packages,
  metrics.net_conversion_value_sum_cars,
  metrics.net_conversion_value_sum_total
from intent_media_sandbox_production.YB_valid_clicks vc
left join
(
  select
    click_request_id,
    count(case when conversion_product_category_type = 'FLIGHTS' then conversion_request_id end) as conversion_count_flights,
    count(case when conversion_product_category_type = 'HOTELS' then conversion_request_id end) as conversion_count_hotels,
    count(case when conversion_product_category_type = 'PACKAGES' then conversion_request_id end) as conversion_count_packages,
    count(case when conversion_product_category_type = 'CARS' then conversion_request_id end) as conversion_count_cars,
    count(conversion_request_id) as conversion_count_total,
    sum(case when conversion_product_category_type = 'FLIGHTS' then conversion_value end) as conversion_value_sum_flights,
    sum(case when conversion_product_category_type = 'HOTELS' then conversion_value end) as conversion_value_sum_hotels,
    sum(case when conversion_product_category_type = 'PACKAGES' then conversion_value end) as conversion_value_sum_packages,
    sum(case when conversion_product_category_type = 'CARS' then conversion_value end) as conversion_value_sum_cars,
    sum(conversion_value) as conversion_value_sum_total,
    sum(case when conversion_product_category_type = 'FLIGHTS' then net_conversion_value end) as net_conversion_value_sum_flights,
    sum(case when conversion_product_category_type = 'HOTELS' then net_conversion_value end) as net_conversion_value_sum_hotels,
    sum(case when conversion_product_category_type = 'PACKAGES' then net_conversion_value end) as net_conversion_value_sum_packages,
    sum(case when conversion_product_category_type = 'CARS' then net_conversion_value end) as net_conversion_value_sum_cars,
    sum(net_conversion_value) as net_conversion_value_sum_total
  from intent_media_sandbox_production.YB_valid_deduped_click_conversions
  where click_rank = 1
  group by
    click_request_id
) metrics
on vc.click_request_id = metrics.click_request_id;

/* Join back to ad_calls table to get other dimensions */
drop table if exists intent_media_sandbox_production.YB_ad_call_click_conversion;
create table intent_media_sandbox_production.YB_ad_call_click_conversion as
select
  cc.*,
  ac.*
from intent_media_sandbox_production.YB_click_conversion cc
inner join
(
  select
    request_id,
    publisher_id,
    site_id,
    ad_unit_id,
    ad_unit_type,
    product_category_type as ad_call_product_category_type,
    device_family,
    browser_family,
    os_family,
    segmentation_score 
  from intent_media_log_data_production.ad_calls
  where requested_at_in_et between '2014-08-21' and '2014-08-27'
    and ip_address_blacklisted = 0
) ac
on cc.impression_request_id = ac.request_id;