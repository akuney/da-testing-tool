select 
	'Flights' as "Product Category Type",
	dimensions.*,
	data."Page Views",
	data."Not Pure Page Views",
	data."Not Pure Low Converting Page Views",
	data."Addressable Page Views",
	data."Pure Page Views",
	data."Fillable Pages",
	data."Pages Served",
	data."Impressions",
	data."Interactions",
	data."Clicks",
	data."Gross Media Revenue",
	data."Available Impressions",
	data."Net Media Revenue",
	data."Pure Low Converting Page Views",
	data."Low Converting Page Views",
	data."Suppressed - Route",
	data."Suppressed - Cannibalization Segment",
	data."Suppressed - Click Blackout",
	data."Suppressed - No Valid Layout",
	data."Suppressed - Publisher Traffic Share",
	data."Suppressed - Cannibalization Threshold"
from
(select *
from
(select 
	distinct(aggregation_level_date_in_et) as Date,
	0 as Zero
from intent_media_production.air_ct_media_performance_aggregations
where aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York')) dates,

(select 
	(case when e.name = 'Orbitz' then 'OWW' when e.name = 'Kayak Software Corporation' then 'Kayak' else e.name end) as "Pub",
	s.display_name as "Site",
	(case
		when lower(au.name) like '%exit%' then 'Total Exit Units'
		when lower(au.name) like '%list%' then 'Total List Page'
		when lower(au.name) like '%media fill in%' then 'Total Media Fill In'
		when lower(au.name) like '%trip.com%' then 'Total Trip.com'
		else au.name 
	end) as "Type of Ad Unit",
	(case au.name
		when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit - Flights - From Homepage' 
		when 'CheapTickets Exit Unit - Firefox' then 'Cheaptickets Exit Unit - Flights - From Homepage'
		else au.name 
	end) as "Ad Unit"
from intent_media_production.air_ct_media_performance_aggregations acmpa
left join intent_media_production.ad_units au on au.id = acmpa.ad_unit_id
left join intent_media_production.sites s on au.site_id = s.id
left join intent_media_production.entities e on s.publisher_id = e.id
group by
	(case when e.name = 'Orbitz' then 'OWW' when e.name = 'Kayak Software Corporation' then 'Kayak' else e.name end),
	s.display_name,
	(case
		when lower(au.name) like '%exit%' then 'Total Exit Units'
		when lower(au.name) like '%list%' then 'Total List Page'
		when lower(au.name) like '%media fill in%' then 'Total Media Fill In'
		when lower(au.name) like '%trip.com%' then 'Total Trip.com'
		else au.name 
	end),
	(case au.name
		when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit - Flights - From Homepage' 
		when 'CheapTickets Exit Unit - Firefox' then 'Cheaptickets Exit Unit - Flights - From Homepage'
		else au.name 
	end) ) ad_unit_names

	) dimensions

left join

(select 
	aggregation_level_date_in_et as Date,
	(case when e.name = 'Orbitz' then 'OWW' when e.name = 'Kayak Software Corporation' then 'Kayak' else e.name end) as "Pub",
	s.display_name as "Site",
	(case
		when lower(au.name) like '%exit%' then 'Total Exit Units'
		when lower(au.name) like '%list%' then 'Total List Page'
		when lower(au.name) like '%media fill in%' then 'Total Media Fill In'
		when lower(au.name) like '%trip.com%' then 'Total Trip.com'
		else au.name 
	end) as "Type of Ad Unit",
	(case au.name
		when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit - Flights - From Homepage' 
		when 'CheapTickets Exit Unit - Firefox' then 'Cheaptickets Exit Unit - Flights - From Homepage'
		else au.name 
	end) as "Ad Unit",
	sum(ad_call_count) as "Page Views",
	sum(not_pure_ad_call_count) as "Not Pure Page Views",
	sum(not_pure_low_converting_ad_call_count) as "Not Pure Low Converting Page Views",
	sum(not_pure_low_converting_addressable_ad_call_count) as "Addressable Page Views",
	sum(pure_ad_call_count) as "Pure Page Views",
	sum(not_pure_low_converting_ad_call_with_ads_count) as "Fillable Pages",
	sum(ad_unit_served_count) as "Pages Served",
	sum(impression_count) as "Impressions",
	sum(interaction_count) as "Interactions",
	sum(click_count) as "Clicks",
	sum(gross_revenue_sum) as "Gross Media Revenue",
	sum(available_impression_count) as "Available Impressions",
	sum(net_revenue_sum) as "Net Media Revenue",
	sum(pure_low_converting_ad_call_count) as "Pure Low Converting Page Views",
	sum(low_converting_ad_call_count) as "Low Converting Page Views",
	sum(suppressed_by_route) as "Suppressed - Route",
	sum(suppressed_by_c13n_segment) as "Suppressed - Cannibalization Segment",
	sum(suppressed_by_click_blackout) as "Suppressed - Click Blackout",
	sum(suppressed_by_no_valid_layout) as "Suppressed - No Valid Layout",
	sum(suppressed_by_publisher_traffic_share) as "Suppressed - Publisher Traffic Share",
	sum(suppressed_by_c13n_above_threshold) as "Suppressed - Cannibalization Threshold"
from intent_media_production.air_ct_media_performance_aggregations acmpa
left join intent_media_production.ad_units au on acmpa.ad_unit_id = au.id		
left join intent_media_production.sites s on s.id = au.site_id 
left join intent_media_production.entities e on e.id = s.publisher_id 
where (aggregation_level_date_in_et > '2011-05-23' and s.name = 'ORBITZ_GLOBAL')
or (aggregation_level_date_in_et > '2011-06-02' and s.name = 'CHEAPTICKETS')
or (aggregation_level_date_in_et > '2011-06-22' and s.name = 'EXPEDIA')
or (aggregation_level_date_in_et > '2012-02-27' and s.name = 'VAYAMA')
or (aggregation_level_date_in_et > '2012-02-29' and s.name = 'BUDGETAIR')
or (aggregation_level_date_in_et > '2012-11-21' and s.name = 'HOTWIRE')
or (aggregation_level_date_in_et > '2013-03-03' and s.name = 'KAYAK')
or s.name not in ('ORBITZ_GLOBAL', 'CHEAPTICKETS', 'EXPEDIA', 'VAYAMA', 'BUDGETAIR', 'HOTWIRE', 'KAYAK')
group by 
	aggregation_level_date_in_et,
	e.name,
	s.display_name,
	(case
		when lower(au.name) like '%exit%' then 'Total Exit Units'
		when lower(au.name) like '%list%' then 'Total List Page'
		when lower(au.name) like '%media fill in%' then 'Total Media Fill In'
		when lower(au.name) like '%trip.com%' then 'Total Trip.com'
		else au.name 
	end),
	(case au.name
		when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit - Flights - From Homepage' 
		when 'CheapTickets Exit Unit - Firefox' then 'Cheaptickets Exit Unit - Flights - From Homepage'
		else au.name 
	end)
	
	) data

on dimensions.Date = data.Date
and dimensions.Pub = data.Pub
and dimensions.Site = data.Site
and dimensions."Type of Ad Unit" = data."Type of Ad Unit"
and dimensions."Ad Unit" = data."Ad Unit"



union



select 
	'Hotels' as "Product Category Type",
	dimensions.*,
	data."Page Views",
	data."Not Pure Page Views",
	data."Not Pure Low Converting Page Views",
	data."Addressable Page Views",
	data."Pure Page Views",
	data."Fillable Pages",
	data."Pages Served",
	data."Impressions",
	data."Interactions",
	data."Clicks",
	data."Gross Media Revenue",
	data."Available Impressions",
	data."Net Media Revenue",
	data."Pure Low Converting Page Views",
	data."Low Converting Page Views",
	data."Suppressed - Route",
	data."Suppressed - Cannibalization Segment",
	data."Suppressed - Click Blackout",
	data."Suppressed - No Valid Layout",
	data."Suppressed - Publisher Traffic Share",
	data."Suppressed - Cannibalization Threshold"
from
(select *
from
(select 
	distinct(aggregation_level_date_in_et) as Date,
	0 as Zero
from intent_media_production.air_ct_media_performance_aggregations
where aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York')) dates,

(select 
	(case when e.name = 'Orbitz' then 'OWW' when e.name = 'Kayak Software Corporation' then 'Kayak' else e.name end) as "Pub",
	s.display_name as "Site",
	(case
		when lower(au.name) like '%exit%' then 'Total Exit Units'
		when lower(au.name) like '%list%' then 'Total List Page'
		when lower(au.name) like '%media fill in%' then 'Total Media Fill In'
		when lower(au.name) like '%trip.com%' then 'Total Trip.com'
		else au.name 
	end) as "Type of Ad Unit",
	(case au.name
		when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit - Flights - From Homepage' 
		when 'CheapTickets Exit Unit - Firefox' then 'Cheaptickets Exit Unit - Flights - From Homepage'
		else au.name 
	end) as "Ad Unit"
from intent_media_production.hotel_ct_media_performance_aggregations acmpa
left join intent_media_production.ad_units au on au.id = acmpa.ad_unit_id
left join intent_media_production.sites s on au.site_id = s.id
left join intent_media_production.entities e on s.publisher_id = e.id
group by
	(case when e.name = 'Orbitz' then 'OWW' when e.name = 'Kayak Software Corporation' then 'Kayak' else e.name end),
	s.display_name,
	(case
		when lower(au.name) like '%exit%' then 'Total Exit Units'
		when lower(au.name) like '%list%' then 'Total List Page'
		when lower(au.name) like '%media fill in%' then 'Total Media Fill In'
		when lower(au.name) like '%trip.com%' then 'Total Trip.com'
		else au.name 
	end),
	(case au.name
		when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit - Flights - From Homepage' 
		when 'CheapTickets Exit Unit - Firefox' then 'Cheaptickets Exit Unit - Flights - From Homepage'
		else au.name 
	end) ) ad_unit_names

	) dimensions

left join

(select 
	aggregation_level_date_in_et as Date,
	(case when e.name = 'Orbitz' then 'OWW' when e.name = 'Kayak Software Corporation' then 'Kayak' else e.name end) as "Pub",
	s.display_name as "Site",
	(case
		when lower(au.name) like '%exit%' then 'Total Exit Units'
		when lower(au.name) like '%list%' then 'Total List Page'
		when lower(au.name) like '%media fill in%' then 'Total Media Fill In'
		when lower(au.name) like '%trip.com%' then 'Total Trip.com'
		else au.name 
	end) as "Type of Ad Unit",
	(case au.name
		when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit - Flights - From Homepage' 
		when 'CheapTickets Exit Unit - Firefox' then 'Cheaptickets Exit Unit - Flights - From Homepage'
		else au.name 
	end) as "Ad Unit",
	sum(ad_call_count) as "Page Views",
	sum(not_pure_ad_call_count) as "Not Pure Page Views",
	sum(not_pure_low_converting_ad_call_count) as "Not Pure Low Converting Page Views",
	sum(not_pure_low_converting_addressable_ad_call_count) as "Addressable Page Views",
	sum(pure_ad_call_count) as "Pure Page Views",
	sum(not_pure_low_converting_ad_call_with_ads_count) as "Fillable Pages",
	sum(served_ad_count) as "Pages Served",
	sum(impression_count) as "Impressions",
	sum(interaction_count) as "Interactions",
	sum(click_count) as "Clicks",
	sum(gross_revenue_sum) as "Gross Media Revenue",
	sum(available_impression_count) as "Available Impressions",
	sum(net_revenue_sum) as "Net Media Revenue",
	sum(pure_low_converting_ad_call_count) as "Pure Low Converting Page Views",
	sum(low_converting_ad_call_count) as "Low Converting Page Views",
	sum(suppressed_by_unknown_hotel_destination) as "Suppressed - Route",
	cast(null as integer) as "Suppressed - Cannibalization Segment",
	sum(suppressed_by_click_blackout) as "Suppressed - Click Blackout",
	sum(suppressed_by_no_valid_layout) as "Suppressed - No Valid Layout",
	sum(suppressed_by_publisher_traffic_share) as "Suppressed - Publisher Traffic Share",
	cast(null as integer) as "Suppressed - Cannibalization Threshold"
from intent_media_production.hotel_ct_media_performance_aggregations acmpa
left join intent_media_production.ad_units au on acmpa.ad_unit_id = au.id		
left join intent_media_production.sites s on s.id = au.site_id 
left join intent_media_production.entities e on e.id = s.publisher_id 
where acmpa.aggregation_level_date_in_et > '2012-12-10'
group by 
	aggregation_level_date_in_et,
	e.name,
	s.display_name,
	(case
		when lower(au.name) like '%exit%' then 'Total Exit Units'
		when lower(au.name) like '%list%' then 'Total List Page'
		when lower(au.name) like '%media fill in%' then 'Total Media Fill In'
		when lower(au.name) like '%trip.com%' then 'Total Trip.com'
		else au.name 
	end),
	(case au.name
		when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit - Flights - From Homepage' 
		when 'CheapTickets Exit Unit - Firefox' then 'Cheaptickets Exit Unit - Flights - From Homepage'
		else au.name 
	end)
	
	) data

on dimensions.Date = data.Date
and dimensions.Pub = data.Pub
and dimensions.Site = data.Site
and dimensions."Type of Ad Unit" = data."Type of Ad Unit"
and dimensions."Ad Unit" = data."Ad Unit"