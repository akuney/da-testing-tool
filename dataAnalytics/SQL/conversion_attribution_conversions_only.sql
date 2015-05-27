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
    -- optional
    ad_unit_id,
    ad_unit_type,
    site_type,
    trip_type
from intent_media_log_data_production.ad_calls
where requested_at_date_in_et between '2014-02-02' and '2014-02-03'
    and ip_address_blacklisted = 0
    and outcome_type = 'SERVED';

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
where (SJ_impressions.requested_at + interval '24 hours') <= SJ_clicks.click_requested_at;   


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
    SJ_clicks.click_request_id,
	rank() over (partition by conversion_request_id order by click_requested_at desc) as click_rank    
from intent_media_sandbox_production.SJ_deduped_conversions
cross join intent_media_sandbox_production.SJ_clicks
where SJ_clicks.webuser_id = SJ_deduped_conversions.webuser_id
    and SJ_clicks.advertiser_id = SJ_deduped_conversions.entity_id
    and (SJ_clicks.click_requested_at + interval '30 days') >= SJ_deduped_conversions.conversion_requested_at
    and SJ_clicks.click_requested_at < SJ_deduped_conversions.conversion_requested_at;
    
-- select only the last click before the conversion
drop table if exists intent_media_sandbox_production.SJ_deduped_conversions_with_click;
create table intent_media_sandbox_production.SJ_deduped_conversions_with_click as
select
    *
from intent_media_sandbox_production.SJ_deduped_conversions_with_clicks_and_rank
where click_rank = 1;



-------------------------------------
-- left join back onto conversions --
-------------------------------------

drop table if exists intent_media_sandbox_production.SJ_deduped_conversions_to_ad_calls;
create table intent_media_sandbox_production.SJ_deduped_conversions_to_ad_calls as
select
	conversions.conversion_request_id,
	conversions.conversion_requested_at,
	conversions.product_category_type as conversion_product_category_type,
	conversions.conversion_value as conversion_conversion_value,
	clicks.click_request_id,
	clicks.click_requested_at,
	clicks.actual_cpc as click_actual_cpc,
	impressions.advertiser_id as impression_advertiser_id,
	impressions.auction_position as impression_auction_position,
	ad_calls.request_id as ad_call_request_id,
	ad_calls.requested_at as ad_call_requested_at,
	ad_calls.requested_at_date_in_et as ad_call_requested_at_date_in_et,
	ad_calls.ad_unit_id as ad_call_ad_unit_id,
	ad_calls.trip_type as ad_call_trip_type
from intent_media_sandbox_production.SJ_deduped_conversions_with_clicks_and_rank conversions
left join intent_media_sandbox_production.SJ_valid_clicks clicks
	on clicks.click_request_id = conversions.click_request_id
left join intent_media_sandbox_production.SJ_impressions impressions
	on impressions.external_id = clicks.external_impression_id
left join intent_media_sandbox_production.SJ_ad_calls ad_calls
	on ad_calls.request_id = impressions.request_id;



