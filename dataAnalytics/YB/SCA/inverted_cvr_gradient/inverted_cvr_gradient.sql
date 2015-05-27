/* --------------------------------------------------------------------------------------------------------------------------------------------------------------------- */
/* --------------------------------------------------------------------------------------------------------------------------------------------------------------------- */
/* --------------------------------------------------------------------------------------------------------------------------------------------------------------------- */
/* conversion attribution XXXX-XX-XX to YYYY-YY-YY */

----------------------------------------------------------------------------------
----------------------------- Base Tables ----------------------------------------

/* Load Ad Calls */
drop table if exists intent_media_sandbox_production.YB_Inverted_CVR_ad_calls;
create table intent_media_sandbox_production.YB_Inverted_CVR_ad_calls as
select
	publisher_user_id,
	webuser_id,
	request_id as ad_call_request_id,
	requested_at_in_et as ad_call_timestamp,
	requested_at_date_in_et as ad_call_date,
	site_id,
	ad_unit_id,
	trip_type,
	browser_family,
	case when browser_family = 'IE' then 'single' else 'multi' end as click_type,	 
	os_family,
	device_family,
	origination_code || '-' || destination_code as od_pair,
	predict_mode_type,
	segmentation_model_type,
	logged_in_user,
	page_view_type
from intent_media_log_data_production.ad_calls ac
where requested_at_date_in_et between '2014-09-01' and '2014-09-30'
	and ip_address_blacklisted = 0
	and outcome_type = 'SERVED'
	and ad_unit_type = 'CT'
	and product_category_type = 'FLIGHTS'
	and browser_family in ('SAFARI', 'FIREFOX', 'IE', 'CHROME');

/* Load Impressions */
drop table if exists intent_media_sandbox_production.YB_Inverted_CVR_impressions;
create table intent_media_sandbox_production.YB_Inverted_CVR_impressions as
select
	i.request_id as impression_request_id,
	i.external_id,
	i.advertiser_id,
	i.auction_position,
	i.jittered_quality_score,
	i.quality_score,
	i.has_compare_button,
	lpt.page_type as legal_page_type
from intent_media_log_data_production.impressions i
inner join intent_media_production.ad_units au on i.ad_unit_id = au.id
inner join intent_media_production.legal_page_types lpt on au.legal_page_type_id = lpt.id
where i.requested_at_date_in_et between '2014-09-01' and '2014-09-30'
	and i.ip_address_blacklisted = 0
	and i.prechecked = 0
	and i.advertiser_id in (59777, 61224, 106574)
	and au.active = 1;

/* Join Ad Calls and Impressions */
drop table if exists intent_media_sandbox_production.YB_Inverted_CVR_ai;
create table intent_media_sandbox_production.YB_Inverted_CVR_ai as
select ac.*, i.*
from intent_media_sandbox_production.YB_Inverted_CVR_ad_calls ac
inner join intent_media_sandbox_production.YB_Inverted_CVR_impressions i
on ac.ad_call_request_id = i.impression_request_id;

/* Valid Clicks */
drop table if exists intent_media_sandbox_production.YB_Inverted_CVR_clicks;
create table intent_media_sandbox_production.YB_Inverted_CVR_clicks as
select
  placement_type,
  external_impression_id,
	request_id as click_request_id,
	requested_at_in_et as click_timestamp,
	requested_at_date_in_et as click_date,
	webuser_id as click_webuser_id,
	actual_cpc,
	rank() over(partition by external_impression_id order by requested_at_in_et) as single_click_rank
from intent_media_log_data_production.clicks
where requested_at_date_in_et between '2014-09-01' and (date('2014-09-30') + interval '24 hours')
  and ip_address_blacklisted = 0
  and fraudulent = 0;

/* Base table - Each Click with all dimensions from impressions and ad_calls */

drop table if exists intent_media_sandbox_production.YB_Inverted_CVR_aic;
create table intent_media_sandbox_production.YB_Inverted_CVR_aic as
select ai.*, c.*
from intent_media_sandbox_production.YB_Inverted_CVR_ai ai 
inner join intent_media_sandbox_production.YB_Inverted_CVR_clicks c
on ai.external_id = c.external_impression_id
and ai.ad_call_timestamp < c.click_timestamp
and c.click_timestamp <= (ai.ad_call_timestamp + interval '24 hours');

/* Load Conversions */
drop table if exists intent_media_sandbox_production.YB_Inverted_CVR_conversions;
create table intent_media_sandbox_production.YB_Inverted_CVR_conversions as
select
	entity_id,
	order_id,
	request_id,
	requested_at_in_et,
	requested_at_date_in_et,
	product_category_type as conversion_product_category_type,
	webuser_id as conversion_webuser_id,
	conversion_value,
	net_conversion_value,
	site_type
from intent_media_log_data_production.conversions
where requested_at_date_in_et between '2014-09-01' and (date('2014-09-30') + interval '31 days')
	and ip_address_blacklisted = 0;

----------------------------------------------------------------------------------
----------------------------------------------------------------------------------

/* De-duplicate conversions */
drop table if exists intent_media_sandbox_production.YB_Inverted_CVR_deduped_conversions;
create table intent_media_sandbox_production.YB_Inverted_CVR_deduped_conversions as
select
  con.*
from 
(
  select
    entity_id,
    order_id,
    min(requested_at_in_et) as min_requested_at_in_et
  from intent_media_sandbox_production.YB_Inverted_CVR_conversions
  where order_id is not null
  group by
    entity_id,
    order_id
) distinct_con
left join intent_media_sandbox_production.YB_Inverted_CVR_conversions con
on con.entity_id = distinct_con.entity_id
and con.order_id = distinct_con.order_id
and con.requested_at_in_et = distinct_con.min_requested_at_in_et
union
select *
from intent_media_sandbox_production.YB_Inverted_CVR_conversions
where order_id is null;

/* Conversion Attribution */
drop table if exists intent_media_sandbox_production.YB_Inverted_CVR_deduped_conversions_per_click_ordered;
create table intent_media_sandbox_production.YB_Inverted_CVR_deduped_conversions_per_click_ordered as
select
  dc.*,
  aic.click_request_id,
  rank() over (partition by dc.request_id order by aic.click_timestamp desc) as click_rank
from intent_media_sandbox_production.YB_Inverted_CVR_deduped_conversions dc
cross join intent_media_sandbox_production.YB_Inverted_CVR_aic aic
where aic.click_webuser_id = dc.conversion_webuser_id
  and aic.advertiser_id = dc.entity_id
  and (aic.click_timestamp + interval '30 days') >= dc.requested_at_in_et
  and aic.click_timestamp < dc.requested_at_in_et;

/* Get Last Click Only */
drop table if exists intent_media_sandbox_production.YB_Inverted_CVR_deduped_conversions_per_click;
create table intent_media_sandbox_production.YB_Inverted_CVR_deduped_conversions_per_click as
select *
from intent_media_sandbox_production.YB_Inverted_CVR_deduped_conversions_per_click_ordered
where click_rank = 1;
		
/* Aggregate Conversions and Clicks */		
drop table if exists intent_media_sandbox_production.YB_Inverted_CVR_deduped_conversions_by_click;
create table intent_media_sandbox_production.YB_Inverted_CVR_deduped_conversions_by_click as
select
  click_request_id,
  count(case when conversion_product_category_type = 'FLIGHTS' then request_id end) as conversion_count_flights,
  count(case when conversion_product_category_type = 'HOTELS' then request_id end) as conversion_count_hotels,
  count(case when conversion_product_category_type = 'PACKAGES' then request_id end) as conversion_count_packages,
  count(case when conversion_product_category_type = 'CARS' then request_id end) as conversion_count_cars,
  count(request_id) as conversion_count_total,
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
from intent_media_sandbox_production.YB_Inverted_CVR_deduped_conversions_per_click
group by 
  click_request_id;
		
/* Left Join Back to Valid Clicks */	
drop table if exists intent_media_sandbox_production.YB_Inverted_CVR_clicks_with_conversions;
create table intent_media_sandbox_production.YB_Inverted_CVR_clicks_with_conversions as
select
  aic.*,
  d.conversion_count_flights,
  d.conversion_count_hotels,
  d.conversion_count_packages,
  d.conversion_count_cars,
  d.conversion_count_total,
  d.conversion_value_sum_flights,
  d.conversion_value_sum_hotels,
  d.conversion_value_sum_packages,
  d.conversion_value_sum_cars,
  d.conversion_value_sum_total,
  d.net_conversion_value_sum_flights,
  d.net_conversion_value_sum_hotels,
  d.net_conversion_value_sum_packages,
  d.net_conversion_value_sum_cars,
  d.net_conversion_value_sum_total
from intent_media_sandbox_production.YB_Inverted_CVR_aic aic
left join intent_media_sandbox_production.YB_Inverted_CVR_deduped_conversions_by_click d
on aic.click_request_id = d.click_request_id;
	
/* Aggregate the result */	
-- drop table if exists intent_media_sandbox_production.YB_Inverted_CVR_final;
-- create table intent_media_sandbox_production.YB_Inverted_CVR_final as
-- select
--   f.click_requested_at_date_in_et as date_in_et,
--   s.display_name as site_name,
--   au.name as ad_unit_name,
--   f.od_pair,
--   f.trip_type,
--   f.browser_family,
--   f.os_family,
--   f.device_family,
--   f.predict_mode_type,
--   f.segmentation_model_type,
--   f.logged_in_user,
--   f.page_view_type,
--   f.advertiser_id,
--   f.auction_position,
--   f.has_compare_button,
--   f.legal_page_type,
--   f.placement_type as click_placement_type,
--   count(distinct f.publisher_user_id) as uv_count,
--   count(f.click_request_id) as click_count,
--   sum(f.actual_cpc) as actual_cpc_sum,
--   sum(f.conversion_count_flights) as conversion_count_flights,
--   sum(f.conversion_count_hotels) as conversion_count_hotels,
--   sum(f.conversion_count_packages) as conversion_count_packages,
--   sum(f.conversion_count_cars) as conversion_count_cars,
--   sum(f.conversion_count_total) as conversion_count_total,
--   case when sum(f.conversion_count_total) > 0 then 1 else 0 end as conversion_count_boolean,
--   sum(f.conversion_value_sum_flights) as conversion_value_sum_flights,
--   sum(f.conversion_value_sum_hotels) as conversion_value_sum_hotels,
--   sum(f.conversion_value_sum_packages) as conversion_value_sum_packages,
--   sum(f.conversion_value_sum_cars) as conversion_value_sum_cars,
--   sum(f.conversion_value_sum_total) as conversion_value_sum_total,
--   sum(f.net_conversion_value_sum_flights) as net_conversion_value_sum_flights,
--   sum(f.net_conversion_value_sum_hotels) as net_conversion_value_sum_hotels,
--   sum(f.net_conversion_value_sum_packages) as net_conversion_value_sum_packages,
--   sum(f.net_conversion_value_sum_cars) as net_conversion_value_sum_cars,
--   sum(f.net_conversion_value_sum_total) as net_conversion_value_sum_total,
--   random() as random
-- from intent_media_sandbox_production.YB_Inverted_CVR_clicks_with_conversions f
-- inner join intent_media_production.sites s on f.site_id = s.id
-- inner join intent_media_production.ad_units au on f.ad_unit_id = au.id
-- where f.product_category_type = 'FLIGHTS'
-- group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17;

grant all on intent_media_sandbox_production.YB_Inverted_CVR_final to tableau;



	
