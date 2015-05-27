-- advertiser_account_report_aggregations
select
	'advertiser_account_report_aggregations' as table_name,
	'SSN' as product,
	aggregation_level_date_in_et as date_in_et,
	ad_unit_id,
	advertiser_id,
	cast(null as int) as ad_call_count,
	sum(impression_count) as impressions,
	sum(actual_cpc_sum) as gross_media_revenue
from intent_media_production.advertiser_account_report_aggregations
where aggregation_level_date_in_et between (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '31 days') and (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '1 day')
group by aggregation_level_date_in_et, ad_unit_id, advertiser_id

union

-- advertiser_ad_report_aggregations
select
	'advertiser_ad_report_aggregations' as table_name,
	'SSN' as product,
	date(aggregation_level at timezone 'UTC' at timezone 'America/New_York') as date_in_et,
	ad_unit_id, 
	advertiser_id,	
	cast(null as int) as ad_call_count,
	sum(impression_count) as impressions,
	sum(actual_cpc_sum) as gross_media_revenue
from intent_media_production.advertiser_ad_report_aggregations
where date(aggregation_level at timezone 'UTC' at timezone 'America/New_York') between (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '31 days') and (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '1 day')
group by date(aggregation_level at timezone 'UTC' at timezone 'America/New_York'), ad_unit_id, advertiser_id

union

-- advertiser_revenue_aggregations
select
	'advertiser_revenue_aggregations' as table_name,
	(case
		when ad_type = 'CT' and product_category_type = 'FLIGHTS' then 'AfT Flights'
		when ad_type = 'CT' and product_category_type = 'HOTELS' then 'Hotel AfT'
		when ad_type = 'SSR' and product_category_type = 'HOTELS' then 'SSN'
		when ad_type = 'META' then 'Hotel Meta'
		else 'Unknown'
	end) as product,
	date(aggregation_level at timezone 'UTC' at timezone 'America/New_York') as date_in_et,
	ad_unit_id, 
	advertiser_id,
	cast(null as int) as ad_call_count,
	cast(null as int) as impressions,
	sum(amount) as gross_media_revenue
from intent_media_production.advertiser_revenue_aggregations
left join intent_media_production.ad_units on ad_units.id = advertiser_revenue_aggregations.ad_unit_id
where date(aggregation_level at timezone 'UTC' at timezone 'America/New_York') between (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '31 days') and (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '1 day')
group by date(aggregation_level at timezone 'UTC' at timezone 'America/New_York'), ad_unit_id, ad_type, product_category_type, advertiser_id

union

-- advertiser_slot_performance_report_aggregations
select
	'advertiser_slot_performance_report_aggregations' as table_name,
	'SSN' as product,
	date(aggregation_level at timezone 'UTC' at timezone 'America/New_York') as date_in_et,
	ad_unit_id,
	advertiser_id,
	cast(null as int) as ad_call_count,
	sum(impression_count) as impressions,
	sum(actual_cpc_sum) as gross_media_revenue
from intent_media_production.advertiser_slot_performance_report_aggregations
where date(aggregation_level at timezone 'UTC' at timezone 'America/New_York') between (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '31 days') and (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '1 day')
group by date(aggregation_level at timezone 'UTC' at timezone 'America/New_York'), ad_unit_id, advertiser_id

union

-- advertiser_travel_window_report_aggregations
select
	'advertiser_travel_window_report_aggregations' as table_name,
	'SSN' as product,
	date(aggregation_level at timezone 'UTC' at timezone 'America/New_York') as date_in_et,
	ad_unit_id, 
	advertiser_id, 
	cast(null as int) as ad_call_count,
	sum(impression_count) as impressions,
	sum(actual_cpc_sum) as gross_media_revenue
from intent_media_production.advertiser_travel_window_report_aggregations
where date(aggregation_level at timezone 'UTC' at timezone 'America/New_York') between (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '31 days') and (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '1 day')
group by date(aggregation_level at timezone 'UTC' at timezone 'America/New_York'), ad_unit_id, advertiser_id

union

-- air_ct_advertiser_performance_report_aggregations
select
	'air_ct_advertiser_performance_report_aggregations' as table_name,
	'AfT Flights' as product,
	date_in_et,
	cast(null as int) as ad_unit_id,
	advertiser_id,
	cast(null as int) as ad_call_count,
	sum(impression_count) as impressions,
	sum(actual_cpc_sum) as gross_media_revenue
from intent_media_production.air_ct_advertiser_performance_report_aggregations
where date_in_et between (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '31 days') and (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '1 day')
group by date_in_et, advertiser_id

union

-- air_ct_auction_position_performance_aggregations
select
	'air_ct_auction_position_performance_aggregations' as table_name,
	'AfT Flights' as product,
	date_in_et,
	ad_unit_id, 
	advertiser_id, 
	cast(null as int) as ad_call_count,
	sum(impression_count) as impressions,
	sum(actual_cpc_sum) as gross_media_revenue
from intent_media_production.air_ct_auction_position_performance_aggregations
where date_in_et between (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '31 days') and (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '1 day')
group by date_in_et, ad_unit_id, advertiser_id

union

-- air_ct_impression_share_report_aggregations
select
	'air_ct_impression_share_report_aggregations' as table_name,
	'AfT Flights' as product,
	aggregation_level_date_in_et as date_in_et,
	cast(null as int) as ad_unit_id,
	advertiser_id, 
	cast(null as int) as ad_call_count,
	sum(impression_count) as impressions,
	cast(null as float) as gross_media_revenue
from intent_media_production.air_ct_impression_share_report_aggregations
where aggregation_level_date_in_et between (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '31 days') and (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '1 day')
group by aggregation_level_date_in_et, advertiser_id

union

-- air_ct_media_performance_aggregations
select
	'air_ct_media_performance_aggregations' as table_name,
	'AfT Flights' as product,
	aggregation_level_date_in_et as date_in_et,
	ad_unit_id,
	cast(null as int) as advertiser_id,
	sum(ad_call_count) as ad_call_count,
	cast(null as int) as impresions,
	sum(gross_revenue_sum) as gross_media_revenue
from intent_media_production.air_ct_media_performance_aggregations
where aggregation_level_date_in_et between (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '31 days') and (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '1 day')
group by aggregation_level_date_in_et, ad_unit_id

union

-- air_ct_placement_type_performance_aggregations
select
	'air_ct_placement_type_performance_aggregations' as table_name,
	'AfT Flights' as product,
	date_in_et,
	ad_unit_id,
	cast(null as int) as advertiser_id,
	cast(null as int) as ad_call_count,
	cast(null as int) as impressions,
	sum(actual_cpc_sum) as gross_media_revenue
from intent_media_production.air_ct_placement_type_performance_aggregations
where date_in_et between (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '31 days') and (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '1 day')
group by date_in_et, ad_unit_id

union

-- hotel_ct_advertiser_performance_report_aggregations
select
	'hotel_ct_advertiser_performance_report_aggregations' as table_name,
	'Hotel AfT' as product,
	date_in_et,
	ad_unit_id, 
	advertiser_id, 
	cast(null as int) as ad_call_count,
	sum(impression_count) as impressions,
	sum(actual_cpc_sum) as gross_media_revenue
from intent_media_production.hotel_ct_advertiser_performance_report_aggregations
where date_in_et between (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '31 days') and (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '1 day')
group by date_in_et, ad_unit_id, advertiser_id

union

-- hotel_ct_impression_share_report_aggregations
select
	'hotel_ct_impression_share_report_aggregations' as table_name,
	'Hotel AfT' as product,
	aggregation_level_date_in_et as date_in_et,
	ad_unit_id, 
	advertiser_id, 
	cast(null as int) as ad_call_count,
	sum(impression_count) as impressions,
	cast(null as float) as gross_media_revenue
from intent_media_production.hotel_ct_impression_share_report_aggregations
where aggregation_level_date_in_et between (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '31 days') and (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '1 day')
group by aggregation_level_date_in_et, ad_unit_id, advertiser_id

union

-- hotel_ct_media_performance_aggregations
select
	'hotel_ct_media_performance_aggregations' as table_name,
	'Hotel AfT' as product,
	aggregation_level_date_in_et as date_in_et,
	ad_unit_id,
	cast(null as int) as advertiser_id,
	sum(ad_call_count) as ad_call_count,
	cast(null as int) as impressions,
	sum(gross_revenue_sum) as gross_media_revenue
from intent_media_production.hotel_ct_media_performance_aggregations
where aggregation_level_date_in_et between (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '31 days') and (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '1 day')
group by aggregation_level_date_in_et, ad_unit_id

union

-- hotel_ct_placement_type_performance_aggregations
select
	'hotel_ct_placement_type_performance_aggregations' as table_name,
	'Hotel AfT' as product,
	date_in_et,
	ad_unit_id,
	cast(null as int) as advertiser_id,
	cast(null as int) as ad_call_count,
	cast(null as int) as impressions,
	sum(actual_cpc_sum) as gross_media_revenue
from intent_media_production.hotel_ct_placement_type_performance_aggregations
where date_in_et between (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '31 days') and (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '1 day')
group by date_in_et, ad_unit_id

union

-- hotel_meta_advertiser_performance_aggregations
select
	'hotel_meta_advertiser_performance_aggregations' as table_name,
	'Hotel Meta' as product,
	date_in_et,
	ad_unit_id, 
	advertiser_id,
	cast(null as int) as ad_call_count,
	sum(impression_count) as impressions,
	sum(actual_cpc_sum) as gross_media_revenue
from intent_media_production.hotel_meta_advertiser_performance_aggregations
where date_in_et between (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '31 days') and (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '1 day')
group by date_in_et, ad_unit_id, advertiser_id

union

-- hotel_meta_media_performance_aggregations
select
	'hotel_meta_media_performance_aggregations' as table_name,
	'Hotel Meta' as product,
	aggregation_level_date_in_et as date_in_et,
	ad_unit_id,
	cast(null as int) as advertiser_id,
	sum(ad_call_count) as ad_call_count,
	cast(null as int) as impressions,
	sum(gross_revenue_sum) as gross_media_revenue
from intent_media_production.hotel_meta_media_performance_aggregations
where aggregation_level_date_in_et between (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '31 days') and (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '1 day')
group by aggregation_level_date_in_et, ad_unit_id

union

-- hotel_meta_position_media_performance_aggregations
select
	'hotel_meta_position_media_performance_aggregations' as table_name,
	'Hotel Meta' as product,
	aggregation_level_date_in_et as date_in_et,
	ad_unit_id,
	cast(null as int) as advertiser_id,
	sum(ad_call_count) as ad_call_count,
	cast(null as int) as impressions,
	sum(gross_revenue_sum) as gross_media_revenue
from intent_media_production.hotel_meta_position_media_performance_aggregations
where aggregation_level_date_in_et between (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '31 days') and (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '1 day')
group by aggregation_level_date_in_et, ad_unit_id

union

-- impression_share_report_aggregations
select
	'impression_share_report_aggregations' as table_name,
	'SSN' as product,
	aggregation_level_date_in_et as date_in_et,
	cast(null as int) as ad_unit_id,
	advertiser_id, 
	cast(null as int) as ad_call_count,
	sum(impression_count) as impressions,
	cast(null as float) as gross_media_revenue
from intent_media_production.impression_share_report_aggregations
where aggregation_level_date_in_et between (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '31 days') and (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '1 day')
group by aggregation_level_date_in_et, advertiser_id

union

-- publisher_market_report_aggregations
select
	'publisher_market_report_aggregations' as table_name,
	'SSN' as product,
	date(aggregation_level at timezone 'UTC' at timezone 'America/New_York') as date_in_et,
	cast(null as int) as ad_unit_id,
	cast(null as int) as advertiser_id,
	sum(ad_call_count) as ad_call_count,
	cast(null as int) as impressions,
	cast(null as float) as gross_media_revenue
from intent_media_production.publisher_market_report_aggregations
where date(aggregation_level at timezone 'UTC' at timezone 'America/New_York') between (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '31 days') and (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '1 day')
group by date(aggregation_level at timezone 'UTC' at timezone 'America/New_York')

union

-- publisher_performance_report_aggregations
select
	'publisher_performance_report_aggregations' as table_name,
	'SSN' as product,
	aggregation_level_date_in_et as date_in_et,
	ad_unit_id,
	cast(null as int) as advertiser_id,
	sum(ad_call_count) as ad_call_count,
	cast(null as int) as impressions,
	sum(gross_actual_cpc_sum) as gross_media_revenue
from intent_media_production.publisher_performance_report_aggregations
where aggregation_level_date_in_et between (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '31 days') and (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '1 day')
group by aggregation_level_date_in_et, ad_unit_id

union

-- publisher_slot_performance_report_aggregations
select
	'publisher_slot_performance_report_aggregations' as table_name,
	'SSN' as product,
	date(aggregation_level at timezone 'UTC' at timezone 'America/New_York') as date_in_et,
	cast(null as int) as ad_unit_id,
	cast(null as int) as advertiser_id,
	cast(null as int) as ad_call_count,
	cast(null as int) as impressions,
	sum(gross_actual_cpc_sum) as gross_media_revenue
from intent_media_production.publisher_slot_performance_report_aggregations
where date(aggregation_level at timezone 'UTC' at timezone 'America/New_York') between (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '31 days') and (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '1 day')
group by date(aggregation_level at timezone 'UTC' at timezone 'America/New_York')

union

-- ad_calls
select
	'ad_calls' as table_name,
	(case
		when ad_unit_type = 'CT' and product_category_type = 'FLIGHTS' then 'AfT Flights'
		when ad_unit_type = 'CT' and product_category_type = 'HOTELS' then 'Hotel AfT'
		when ad_unit_type = 'SSR' and product_category_type = 'HOTELS' then 'SSN'
		when ad_unit_type = 'META' then 'Hotel Meta'
		else 'Unknown'
	end) as product,
	requested_at_date_in_et as date_in_et,
	ad_unit_id,
	cast(null as int) as advertiser_id,
	count(1) as ad_call_count,
	cast(null as int) as impressions,
	cast(null as float) as gross_media_revenue
from intent_media_log_data_production.ad_calls 
where requested_at_date_in_et between (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '31 days') and (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '1 day')
	and ip_address_blacklisted = 0
	and ((ad_unit_type = 'SSR' and outcome_type = 'SERVED') or (ad_unit_type != 'SSR'))
group by requested_at_date_in_et, ad_unit_id, ad_unit_type, product_category_type

union

-- impressions
select
	'impressions' as table_name,
	(case
		when ad_type = 'CT' and product_category_type = 'FLIGHTS' then 'AfT Flights'
		when ad_type = 'CT' and product_category_type = 'HOTELS' then 'Hotel AfT'
		when ad_type = 'SSR' and product_category_type = 'HOTELS' then 'SSN'
		when ad_type = 'META' then 'Hotel Meta'
		else 'Unknown'
	end) as product,
	requested_at_date_in_et as date_in_et,
	ad_unit_id,
	advertiser_id,
	cast(null as int) as ad_call_count,
	count(1) as impressions,
	cast(null as float) as gross_media_revenue
from intent_media_log_data_production.impressions
left join
    (
      select ad_type, product_category_type, id
      from intent_media_production.ad_units
    ) ad_units on ad_units.id = impressions.ad_unit_id
where requested_at_date_in_et between (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '31 days') and (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '1 day')
	and ip_address_blacklisted = 0
group by requested_at_date_in_et, ad_unit_id, ad_type, product_category_type, advertiser_id

union

-- clicks

select
	'clicks' as table_name,
	(case
		when ad_type = 'CT' and ad_units.product_category_type = 'FLIGHTS' then 'AfT Flights'
		when ad_type = 'CT' and ad_units.product_category_type = 'HOTELS' then 'Hotel AfT'
		when ad_type = 'SSR' and ad_units.product_category_type = 'HOTELS' then 'SSN'
		when ad_type = 'META' then 'Hotel Meta'
		else 'Unknown'
	end) as product,
	i.requested_at_date_in_et as date_in_et,
	i.ad_unit_id,
	i.advertiser_id,
	cast(null as int) as ad_call_count,
	cast(null as int) as impressions,
	sum(c.actual_cpc) as gross_media_revenue
from intent_media_log_data_production.clicks c
left join intent_media_log_data_production.impressions i
	on c.external_impression_id = i.external_id
left join
    (
      select ad_type, product_category_type, id
      from intent_media_production.ad_units
    ) ad_units on ad_units.id = i.ad_unit_id
where i.requested_at_date_in_et between (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '31 days') and (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '1 day')
	and c.requested_at_date_in_et between (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York' - interval '31 days') and (date(current_timestamp) at timezone 'UTC' at timezone 'America/New_York')
	and c.requested_at_date_in_et <= (i.requested_at_date_in_et + interval '24 hours')
	and c.ip_address_blacklisted = 0
	and c.fraudulent = 0
	and i.ip_address_blacklisted = 0
group by i.requested_at_date_in_et, i.ad_unit_id, ad_type, ad_units.product_category_type, i.advertiser_id