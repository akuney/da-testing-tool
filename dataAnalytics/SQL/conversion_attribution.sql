------------------
-- instructions --
------------------
-- replace '2014-02-02' with desired start date
-- replace '2014-02-03' with desired end date
-- add any additional desired fields:
--     - under the "--optional" comment in the initial load of ad_calls, impressions, clicks, and conversions
--     - in the final aggregation
-- ???
-- profit

-----------------------
-- load initial data --
-----------------------

-- load ad_calls
drop table if exists intent_media_sandbox_production.SJ_ad_calls;
create table intent_media_sandbox_production.SJ_ad_calls as
select
	-- required
    request_id,
    requested_at,
    requested_at_date_in_et,
    product_category_type,
    -- optional
    ad_unit_id,
    ad_unit_type,
    site_type,
    trip_type
from intent_media_log_data_production.ad_calls
where requested_at_date_in_et between '2014-02-02' and '2014-02-03'
    and ip_address_blacklisted = 0
    and outcome_type = 'SERVED'
    and ad_unit_type = 'CT';

-- load impressions
drop table if exists intent_media_sandbox_production.SJ_impressions;
create table intent_media_sandbox_production.SJ_impressions as
select
	-- required
    request_id,
    requested_at,
    external_id,
    advertiser_id,
    -- optional
    auction_position    
from intent_media_log_data_production.impressions
where requested_at_date_in_et between '2014-02-02' and '2014-02-03'
    and ip_address_blacklisted = 0;
    
-- load clicks
drop table if exists intent_media_sandbox_production.SJ_clicks;
create table intent_media_sandbox_production.SJ_clicks as
select
	-- required
    clicks.request_id as click_request_id,
    clicks.requested_at as click_requested_at,
    clicks.webuser_id,
    clicks.external_impression_id,
    SJ_impressions.advertiser_id,
    clicks.actual_cpc
from intent_media_log_data_production.clicks
left join intent_media_sandbox_production.SJ_impressions
    on SJ_impressions.external_id = clicks.external_impression_id
where clicks.requested_at_date_in_et between '2014-02-02' and (date('2014-02-03') + interval '31 days')
    and clicks.ip_address_blacklisted = 0
    and clicks.fraudulent = 0;
    
-- load conversions
drop table if exists intent_media_sandbox_production.SJ_conversions;
create table intent_media_sandbox_production.SJ_conversions as
select
    -- required
    entity_id,
    order_id,
    request_id as conversion_request_id,
    requested_at as conversion_requested_at,
    product_category_type,
    webuser_id,
    conversion_value,
    net_conversion_value,
    -- optional
    site_type
from intent_media_log_data_production.conversions
where requested_at_date_in_et between '2014-02-02' and (date('2014-02-03') + interval '31 days')
    and ip_address_blacklisted = 0;



-----------------------------------------------
-- filter out invalid clicks and conversions --
-----------------------------------------------
    
 -- only select valid clicks (made 24 hours or less after an impression)
drop table if exists intent_media_sandbox_production.SJ_valid_clicks;
create table intent_media_sandbox_production.SJ_valid_clicks as
select
    SJ_clicks.*
from intent_media_sandbox_production.SJ_clicks
left join intent_media_sandbox_production.SJ_impressions
    on SJ_impressions.external_id = SJ_clicks.external_impression_id
where SJ_impressions.requested_at + interval '24 hours' >= SJ_clicks.click_requested_at;   


-- dedup conversions
drop table if exists intent_media_sandbox_production.SJ_deduped_conversions;
create table intent_media_sandbox_production.SJ_deduped_conversions as
select
    SJ_conversions.*
from 
    (select
        entity_id,
        order_id,
        min(conversion_requested_at) as min_requested_at
    from intent_media_sandbox_production.SJ_conversions c
    where order_id is not null
    group by entity_id, order_id) distinct_conversions
left join intent_media_sandbox_production.SJ_conversions 
    on SJ_conversions.entity_id = distinct_conversions.entity_id 
    and SJ_conversions.order_id = distinct_conversions.order_id 
    and SJ_conversions.conversion_requested_at = distinct_conversions.min_requested_at
union
select
    *
from intent_media_sandbox_production.SJ_conversions
where order_id is null;   



-------------------------------
-- do conversion attribution --
-------------------------------

-- find all clicks previous to a conversion by same webuser_id
drop table if exists intent_media_sandbox_production.SJ_deduped_conversions_with_clicks_and_rank;
create table intent_media_sandbox_production.SJ_deduped_conversions_with_clicks_and_rank as
select
    SJ_deduped_conversions.*,
    SJ_valid_clicks.click_request_id,
	rank() over (partition by conversion_request_id order by click_requested_at desc) as click_rank    
from intent_media_sandbox_production.SJ_deduped_conversions
cross join intent_media_sandbox_production.SJ_valid_clicks
where SJ_valid_clicks.webuser_id = SJ_deduped_conversions.webuser_id
    and SJ_valid_clicks.advertiser_id = SJ_deduped_conversions.entity_id
    and (SJ_valid_clicks.click_requested_at + interval '30 days') >= SJ_deduped_conversions.conversion_requested_at
    and SJ_valid_clicks.click_requested_at < SJ_deduped_conversions.conversion_requested_at;
    
-- select only the last click before the conversion
drop table if exists intent_media_sandbox_production.SJ_deduped_conversions_with_click;
create table intent_media_sandbox_production.SJ_deduped_conversions_with_click as
select
    *
from intent_media_sandbox_production.SJ_deduped_conversions_with_clicks_and_rank
where click_rank = 1;



----------------------------------------
-- aggregate conversions, then clicks --
----------------------------------------

-- aggregate conversions by click_request_id
drop table if exists intent_media_sandbox_production.SJ_deduped_conversions_by_click;
create table intent_media_sandbox_production.SJ_deduped_conversions_by_click as
select
	click_request_id,
    count(case when product_category_type = 'FLIGHTS' then conversion_request_id end) as conversion_count_flights,
    count(case when product_category_type = 'HOTELS' then conversion_request_id end) as conversion_count_hotels,
    count(case when product_category_type = 'PACKAGES' then conversion_request_id end) as conversion_count_packages,
    count(case when product_category_type = 'CARS' then conversion_request_id end) as conversion_count_cars,
    count(conversion_request_id) as conversion_count_total,
    sum(case when product_category_type = 'FLIGHTS' then conversion_value end) as conversion_value_sum_flights,
    sum(case when product_category_type = 'HOTELS' then conversion_value end) as conversion_value_sum_hotels,
    sum(case when product_category_type = 'PACKAGES' then conversion_value end) as conversion_value_sum_packages,
    sum(case when product_category_type = 'CARS' then conversion_value end) as conversion_value_sum_cars,
    sum(conversion_value) as conversion_value_sum_total,
    sum(case when product_category_type = 'FLIGHTS' then net_conversion_value end) as net_conversion_value_sum_flights,
    sum(case when product_category_type = 'HOTELS' then net_conversion_value end) as net_conversion_value_sum_hotels,
    sum(case when product_category_type = 'PACKAGES' then net_conversion_value end) as net_conversion_value_sum_packages,
    sum(case when product_category_type = 'CARS' then net_conversion_value end) as net_conversion_value_sum_cars,
    sum(net_conversion_value) as net_conversion_value_sum_total
from intent_media_sandbox_production.SJ_deduped_conversions_with_click
group by
	click_request_id;
	
-- left join back onto clicks	
drop table if exists intent_media_sandbox_production.SJ_clicks_with_conversions;
create table intent_media_sandbox_production.SJ_clicks_with_conversions	as
select
	SJ_valid_clicks.*,
    conversion_count_flights,
    conversion_count_hotels,
    conversion_count_packages,
    conversion_count_cars,
    conversion_count_total,
    conversion_value_sum_flights,
    conversion_value_sum_hotels,
    conversion_value_sum_packages,
    conversion_value_sum_cars,
    conversion_value_sum_total,
    net_conversion_value_sum_flights,
    net_conversion_value_sum_hotels,
    net_conversion_value_sum_packages,
    net_conversion_value_sum_cars,
    net_conversion_value_sum_total	
from intent_media_sandbox_production.SJ_valid_clicks
left join intent_media_sandbox_production.SJ_deduped_conversions_by_click
	on SJ_valid_clicks.click_request_id = SJ_deduped_conversions_by_click.click_request_id;
	
-- aggregated clicks and conversions by ad_call/impression request_id	
drop table if exists intent_media_sandbox_production.SJ_impressions_with_clicks_conversions;	
create table intent_media_sandbox_production.SJ_impressions_with_clicks_conversions as
select
	-- required
	SJ_ad_calls.request_id,
    SJ_ad_calls.product_category_type,
    SJ_impressions.external_id,
    SJ_impressions.advertiser_id,
    -- optional
    SJ_ad_calls.requested_at,
    SJ_ad_calls.requested_at_date_in_et,
    SJ_ad_calls.ad_unit_id,
    SJ_ad_calls.ad_unit_type,
    SJ_ad_calls.site_type,
    SJ_ad_calls.trip_type,
    SJ_impressions.auction_position,
	count(SJ_clicks_with_conversions.click_request_id) as click_count,
	sum(SJ_clicks_with_conversions.actual_cpc) as actual_cpc_sum,
    sum(conversion_count_flights) as conversion_count_flights,
    sum(conversion_count_hotels) as conversion_count_hotels,
    sum(conversion_count_packages) as conversion_count_packages,
    sum(conversion_count_cars) as conversion_count_cars,
    sum(conversion_count_total) as conversion_count_total,
    sum(conversion_value_sum_flights) as conversion_value_sum_flights,
    sum(conversion_value_sum_hotels) as conversion_value_sum_hotels,
    sum(conversion_value_sum_packages) as conversion_value_sum_packages,
    sum(conversion_value_sum_cars) as conversion_value_sum_cars,
    sum(conversion_value_sum_total) as conversion_value_sum_total,
    sum(net_conversion_value_sum_flights) as net_conversion_value_sum_flights,
    sum(net_conversion_value_sum_hotels) as net_conversion_value_sum_hotels,
    sum(net_conversion_value_sum_packages) as net_conversion_value_sum_packages,
    sum(net_conversion_value_sum_cars) as net_conversion_value_sum_cars,
    sum(net_conversion_value_sum_total) as net_conversion_value_sum_total
from intent_media_sandbox_production.SJ_impressions
left join intent_media_sandbox_production.SJ_ad_calls
	on SJ_ad_calls.request_id = SJ_impressions.request_id
left join intent_media_sandbox_production.SJ_clicks_with_conversions
	on SJ_clicks_with_conversions.external_impression_id = SJ_impressions.external_id
group by
	-- required
	SJ_ad_calls.request_id,
    SJ_ad_calls.product_category_type,
    SJ_impressions.external_id,
    SJ_impressions.advertiser_id,
    -- optional
    SJ_ad_calls.requested_at,
    SJ_ad_calls.requested_at_date_in_et,
    SJ_ad_calls.ad_unit_id,
    SJ_ad_calls.ad_unit_type,
    SJ_ad_calls.site_type,
    SJ_ad_calls.trip_type,
    SJ_impressions.auction_position;



----------------------------
-- query for desired data --
----------------------------

-- sample query	
select
	requested_at_date_in_et,
    product_category_type,
	count(request_id) as impressions,
	sum(click_count) as clicks,
	sum(actual_cpc_sum) as spend,
	sum(conversion_count_total) as conversions,
	sum(conversion_value_sum_total) as conversion_value_sum
from intent_media_sandbox_production.SJ_impressions_with_clicks_conversions
where advertiser_id = 61224
group by 
    requested_at_date_in_et,
    product_category_type	
	
	
	
	
	
	