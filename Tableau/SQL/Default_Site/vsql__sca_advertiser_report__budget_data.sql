select 
	'Flights' as "Product Category Type",
	pa.*,
	budget."Advertiser Name",
	budget."Advertiser Segment",
	budget."Total Budget"
from
	(select
		date_in_et as Date,
		advertiser_id as "Advertiser ID",
		sum(actual_cpc_sum) as Spend
	from intent_media_production.air_ct_advertiser_performance_report_aggregations
	where date_in_et < date(current_timestamp at timezone 'America/New_York')
	group by date_in_et, advertiser_id) pa
left join 
	(select
		hb.date_in_et as Date,
		hb.advertiser_id as "Advertiser ID",
		e.name as "Advertiser Name",
		(case e.advertiser_category_type
			when 'AIRLINE_DOMESTIC' then 'Domestic Airline'
			when 'AIRLINE_INTERNATIONAL' then 'International Airline'
			when 'HOTEL_CHAIN' then 'Hotel Chain'
			when 'META' then 'Meta'
			when 'TIER_1' then 'Tier 1'
			when 'TIER_2' then 'Tier 2'
			when 'OTA_BUDGET' then 'OTA Budget'
			when 'OTHER' then 'Other'
			else e.advertiser_category_type
		end) as "Advertiser Segment",
	sum(effective_budget) as "Total Budget"
	from intent_media_production.historical_budgets hb
	left join intent_media_production.entities e on hb.advertiser_id = e.id
	where e.entity_type = 'AftAdvertiser'
		and hb.date_in_et < date(current_timestamp at timezone 'America/New_York')
	group by hb.date_in_et, hb.advertiser_id, e.name, e.advertiser_category_type) budget
on pa."Advertiser ID" = budget."Advertiser ID"
and pa.Date = budget.Date

union

select 
	'Hotels' as "Product Category Type",
	pa.*,
	budget."Advertiser Name",
	budget."Advertiser Segment",
	budget."Total Budget"
from
	(select
		date_in_et as Date,
		advertiser_id as "Advertiser ID",
		sum(actual_cpc_sum) as Spend
	from intent_media_production.hotel_ct_advertiser_performance_report_aggregations
	where date_in_et < date(current_timestamp at timezone 'America/New_York')
	group by date_in_et, advertiser_id) pa
left join 
	(select
		hb.date_in_et as Date,
		hb.advertiser_id as "Advertiser ID",
		e.name as "Advertiser Name",
		(case e.advertiser_category_type
			when 'AIRLINE_DOMESTIC' then 'Domestic Airline'
			when 'AIRLINE_INTERNATIONAL' then 'International Airline'
			when 'HOTEL_CHAIN' then 'Hotel Chain'
			when 'META' then 'Meta'
			when 'TIER_1' then 'Tier 1'
			when 'TIER_2' then 'Tier 2'
			when 'OTA_BUDGET' then 'OTA Budget'
			when 'OTHER' then 'Other'
			else e.advertiser_category_type
		end) as "Advertiser Segment",
	sum(effective_budget) as "Total Budget"
	from intent_media_production.historical_budgets hb
	left join intent_media_production.entities e on hb.advertiser_id = e.id
	where e.entity_type = 'AftAdvertiser'
		and hb.date_in_et < date(current_timestamp at timezone 'America/New_York')
	group by hb.date_in_et, hb.advertiser_id, e.name, e.advertiser_category_type) budget
on pa."Advertiser ID" = budget."Advertiser ID"
and pa.Date = budget.Date


union

select 
	'Meta' as "Product Category Type",
	pa.*,
	budget."Advertiser Name",
	budget."Advertiser Segment",
	budget."Total Budget"
from
	(select
		date_in_et as Date,
		advertiser_id as "Advertiser ID",
		sum(actual_cpc_sum) as Spend
	from intent_media_production.hotel_meta_advertiser_performance_aggregations
	where date_in_et < date(current_timestamp at timezone 'America/New_York')
	group by date_in_et, advertiser_id) pa
left join 
	(select
		hb.date_in_et as Date,
		hb.advertiser_id as "Advertiser ID",
		e.name as "Advertiser Name",
		(case e.advertiser_category_type
			when 'AIRLINE_DOMESTIC' then 'Domestic Airline'
			when 'AIRLINE_INTERNATIONAL' then 'International Airline'
			when 'HOTEL_CHAIN' then 'Hotel Chain'
			when 'META' then 'Meta'
			when 'TIER_1' then 'Tier 1'
			when 'TIER_2' then 'Tier 2'
			when 'OTA_BUDGET' then 'OTA Budget'
			when 'OTHER' then 'Other'
			else e.advertiser_category_type
		end) as "Advertiser Segment",
	sum(effective_budget) as "Total Budget"
	from intent_media_production.historical_budgets hb
	left join intent_media_production.entities e on hb.advertiser_id = e.id
	where e.entity_type = 'AftAdvertiser'
		and hb.date_in_et < date(current_timestamp at timezone 'America/New_York')
	group by hb.date_in_et, hb.advertiser_id, e.name, e.advertiser_category_type) budget
on pa."Advertiser ID" = budget."Advertiser ID"
and pa.Date = budget.Date