---------------------
---- SCA Flights ----
---------------------

select 
	'SCA' as Network,
	'Flights' as Product,
	aggregation_level_date_in_et as Date,
	(case 
		when e.name = 'Orbitz' then 'OWW' 
		when e.name = 'Expedia' then 'Expedia Inc.'
		when e.name = 'Kayak Software Corporation' then 'Kayak' 
		when e.name = 'Travelocity on Expedia' then 'Expedia Inc.'
		else e.name
	end) as "Publisher",
	s.display_name as "Site",
	lpt.page_type as "Type of Ad Unit",
	(case au.name
		when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit - Flights - From Homepage' 
		when 'CheapTickets Exit Unit - Firefox' then 'Cheaptickets Exit Unit - Flights - From Homepage'
		else au.name 
	end) as "Ad Unit",
	sum(ad_call_count) as "Page Views",
	sum(not_pure_low_converting_addressable_ad_call_count) as "Addressable Pages",
	sum(ad_unit_served_count) as "Pages Served",
	sum(impression_count) as "Impressions",
	sum(interaction_count) as "Interactions",
	sum(click_count) as "Clicks",
	sum(gross_revenue_sum) as "Gross Media Revenue",
	sum(net_revenue_sum) as "Net Media Revenue"
from intent_media_production.air_ct_media_performance_aggregations acmpa
left join intent_media_production.ad_units au on acmpa.ad_unit_id = au.id
left join intent_media_production.legal_page_types lpt on lpt.id = au.legal_page_type_id	
left join intent_media_production.sites s on s.id = au.site_id 
left join intent_media_production.entities e on e.id = s.publisher_id 
where aggregation_level_date_in_et >= au.reporting_start_date
and aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York')
group by 
	aggregation_level_date_in_et,
	(case 
		when e.name = 'Orbitz' then 'OWW' 
		when e.name = 'Expedia' then 'Expedia Inc.'
		when e.name = 'Kayak Software Corporation' then 'Kayak' 
		when e.name = 'Travelocity on Expedia' then 'Expedia Inc.'
		else e.name
	end),
	s.display_name,
	lpt.page_type,
	(case au.name
		when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit - Flights - From Homepage' 
		when 'CheapTickets Exit Unit - Firefox' then 'Cheaptickets Exit Unit - Flights - From Homepage'
		else au.name 
	end)
	
---------------------
----- Hotel SCA -----
---------------------

union

select 
	'SCA' as Network,
	'Hotels' as Product,
	aggregation_level_date_in_et as Date,
	(case 
		when e.name = 'Orbitz' then 'OWW' 
		when e.name = 'Expedia' then 'Expedia Inc.'
		when e.name = 'Kayak Software Corporation' then 'Kayak' 
		when e.name = 'Travelocity on Expedia' then 'Expedia Inc.'
		else e.name
	end) as "Publisher",
	s.display_name as "Site",
	lpt.page_type as "Type of Ad Unit",
	(case au.name
		when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit - Flights - From Homepage' 
		when 'CheapTickets Exit Unit - Firefox' then 'Cheaptickets Exit Unit - Flights - From Homepage'
		else au.name 
	end) as "Ad Unit",
	sum(ad_call_count) as "Page Views",
	sum(not_pure_low_converting_addressable_ad_call_count) as "Addressable Pages",
	sum(served_ad_count) as "Pages Served",
	sum(impression_count) as "Impressions",
	sum(interaction_count) as "Interactions",
	sum(click_count) as "Clicks",
	sum(gross_revenue_sum) as "Gross Media Revenue",
	sum(net_revenue_sum) as "Net Media Revenue"
from intent_media_production.hotel_ct_media_performance_aggregations hcmpa
left join intent_media_production.ad_units au on hcmpa.ad_unit_id = au.id
left join intent_media_production.legal_page_types lpt on lpt.id = au.legal_page_type_id		
left join intent_media_production.sites s on s.id = au.site_id 
left join intent_media_production.entities e on e.id = s.publisher_id 
where aggregation_level_date_in_et >= au.reporting_start_date
and aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York')
group by 
	aggregation_level_date_in_et,
	(case 
		when e.name = 'Orbitz' then 'OWW' 
		when e.name = 'Expedia' then 'Expedia Inc.'
		when e.name = 'Kayak Software Corporation' then 'Kayak' 
		when e.name = 'Travelocity on Expedia' then 'Expedia Inc.'
		else e.name
	end),
	s.display_name,
    lpt.page_type,
	(case au.name
		when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit - Flights - From Homepage' 
		when 'CheapTickets Exit Unit - Firefox' then 'Cheaptickets Exit Unit - Flights - From Homepage'
		else au.name 
	end)
    
    
--------------------
---- SSN Hotels ----
--------------------   
    
union

select
	'SSN' as Network,
	'Hotels' as Product,
	aggregation_level_date_in_et as Date,
	(case 
		when e.name = 'Orbitz' then 'OWW' 
		when e.name = 'Expedia' then 'Expedia Inc.'
		when e.name = 'Kayak Software Corporation' then 'Kayak' 
		when e.name = 'Travelocity on Expedia' then 'Expedia Inc.'
		else e.name
	end) as "Publisher",
	s.display_name as "Site",
	lpt.page_type as "Type of Ad Unit",
	(case au.name
		when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit - Flights - From Homepage' 
		when 'CheapTickets Exit Unit - Firefox' then 'Cheaptickets Exit Unit - Flights - From Homepage'
		else au.name 
	end) as "Ad Unit",
	sum(ad_call_count) as "Pages Available", 
	sum(ad_call_count) as "Addressable Pages",
	sum(case when positions_filled > 0 then ad_call_count else 0 end) as "Pages Served", 
	sum(ad_call_count*positions_filled) as "Impressions",
	sum(click_count) as "Interactions",
	sum(click_count) as "Clicks", 
	sum(gross_actual_cpc_sum) as "Gross Media Revenue", 
	cast(null as float) as "Net Media Revenue"
from intent_media_production.publisher_performance_report_aggregations ppra
left join intent_media_production.ad_units au on au.id = ppra.ad_unit_id
left join intent_media_production.legal_page_types lpt on lpt.id = au.legal_page_type_id
left join intent_media_production.sites s on s.id = au.site_id
left join intent_media_production.entities e on e.id = s.publisher_id
left join intent_media_production.intent_media_markets_publisher_markets immpm on immpm.market_id = ppra.market_id
left join intent_media_production.intent_media_markets imm on imm.id = immpm.intent_media_market_id
where (case 
		when s.name = 'TRAVELOCITY'
			then aggregation_level_date_in_et >= '2011-04-01' and aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York')
		else
			aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York')
			and (((au.name like '%SEM%' or au.name like '%Hotel Details%') and aggregation_level_date_in_et >= '2012-05-15')
			or (au.name not like '%SEM%' and au.name not like '%Hotel Details%'))
	end)
group by
	aggregation_level_date_in_et,
	(case 
		when e.name = 'Orbitz' then 'OWW' 
		when e.name = 'Expedia' then 'Expedia Inc.'
		when e.name = 'Kayak Software Corporation' then 'Kayak' 
		when e.name = 'Travelocity on Expedia' then 'Expedia Inc.'
		else e.name
	end),
	s.display_name,
	lpt.page_type,
	(case au.name
		when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit - Flights - From Homepage' 
		when 'CheapTickets Exit Unit - Firefox' then 'Cheaptickets Exit Unit - Flights - From Homepage'
		else au.name 
	end)
    
 
--------------------
---- Hotel PPA ----
--------------------   

union

select
	'PPA' as Network,
	'Hotels' as Product,
	aggregation_level_date_in_et as Date,
	(case 
		when e.name = 'Orbitz' then 'OWW' 
		when e.name = 'Expedia' then 'Expedia Inc.'
		when e.name = 'Kayak Software Corporation' then 'Kayak' 
		when e.name = 'Travelocity on Expedia' then 'Expedia Inc.'
		else e.name
	end) as "Publisher",
	s.display_name as "Site",
	lpt.page_type as "Type of Ad Unit",
	(case au.name
		when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit - Flights - From Homepage' 
		when 'CheapTickets Exit Unit - Firefox' then 'Cheaptickets Exit Unit - Flights - From Homepage'
		else au.name 
	end) as "Ad Unit",
	sum(page_view_count) as "Page Views",
	sum(not_pure_page_view_count) as "Addressable Pages",
	sum(served_page_count) as "Pages Served",
	sum(impression_count) as "Impressions",
	sum(page_view_interaction_count) as "Interactions",
	sum(click_count) as "Clicks",
	sum(gross_revenue_sum) as "Gross Media Revenue",
	cast(null as float) as "Net Media Revenue"
from intent_media_production.hotel_meta_media_performance_aggregations hmmpa
left join intent_media_production.ad_units au on au.id = hmmpa.ad_unit_id
left join intent_media_production.legal_page_types lpt on lpt.id = au.legal_page_type_id
left join intent_media_production.sites s on s.id = au.site_id
left join intent_media_production.entities e on e.id = s.publisher_id
where aggregation_level_date_in_et >= au.reporting_start_date
      and aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York')
group by 
	aggregation_level_date_in_et,
	(case 
		when e.name = 'Orbitz' then 'OWW' 
		when e.name = 'Expedia' then 'Expedia Inc.'
		when e.name = 'Kayak Software Corporation' then 'Kayak' 
		when e.name = 'Travelocity on Expedia' then 'Expedia Inc.'
		else e.name
	end),
	s.display_name,
	lpt.page_type,
	(case au.name
		when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit - Flights - From Homepage' 
		when 'CheapTickets Exit Unit - Firefox' then 'Cheaptickets Exit Unit - Flights - From Homepage'
		else au.name 
	end)
