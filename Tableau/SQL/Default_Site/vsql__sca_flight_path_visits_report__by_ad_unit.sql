select 
	dimensions.Date,
	dimensions.Pub,
	(case dimensions.Site
		when 'EXPEDIA_CA' then 'Expedia.ca'
		when 'HOTWIRE' then 'Hotwire'
		when 'HOTWIRE_MEDIA_FILL_IN' then 'Hotwire Media Fill In'
		when 'EXPEDIA' then 'Expedia'
		when 'ORBITZ_GLOBAL' then 'Orbitz'
 		when 'CHEAPTICKETS' then 'CheapTickets'
 		when 'BUDGETAIR' then 'BudgetAir'
 		when 'VAYAMA' then 'Vayama'
 		when 'TRIPDOTCOM' then 'Trip.com'
 		when 'BOOKIT' then 'Bookit'
 		when 'KAYAK' then 'Kayak'
 		else dimensions.Site
	end) as Site,
	dimensions."Type of Ad Unit",
	dimensions."Ad Unit",
	data."Flight Path Visits",
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
(

select 
	(case when e.name = 'Orbitz' then 'OWW' else e.name end) as "Pub",
	s.name as "Site",
	(case
		when lower(au.name) like '%exit%' then 'Total Exit Units'
		when lower(au.name) like '%list%' then 'Total List Page'
		else au.name 
	end) as "Type of Ad Unit",
	(case au.name
		when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit - Flights - From Homepage' 
		when 'CheapTickets Exit Unit - Firefox' then 'Cheaptickets Exit Unit - Flights - From Homepage'
		else au.name 
	end) as "Ad Unit"
from intent_media_production.ad_units au
left join intent_media_production.sites s on au.site_id = s.id
left join intent_media_production.entities e on s.publisher_id = e.id
where au.ad_type = 'CT' 
	and au.product_category_type = 'FLIGHTS'
	and au.active = 1
) ad_unit_names

	) dimensions

left join

(select 
	performance.*,
	fpv.flight_path_visits as "Flight Path Visits"
from
(

select
	date_in_et as Date,
	(case when e.name = 'Orbitz' then 'OWW' else e.name end) as "Pub",
	s.name as "Site",
	session_count as flight_path_visits
from intent_media_production.air_ct_flight_path_visit_aggregations acfpva
left join intent_media_production.sites s on acfpva.site_id = s.id
left join intent_media_production.entities e on e.id = s.publisher_id
where destination_name IS NULL

	) fpv,

(

select 
	aggregation_level_date_in_et as Date,
	(case when e.name = 'Orbitz' then 'OWW' else e.name end) as "Pub",
	sites.name as "Site",
	(case
		when lower(intent_media_production.ad_units.name) like '%exit%' then 'Total Exit Units'
		when lower(intent_media_production.ad_units.name) like '%list%' then 'Total List Page'
		else intent_media_production.ad_units.name 
	end) as "Type of Ad Unit",
	(case intent_media_production.ad_units.name
		when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit - Flights - From Homepage' 
		when 'CheapTickets Exit Unit - Firefox' then 'Cheaptickets Exit Unit - Flights - From Homepage'
		else intent_media_production.ad_units.name 
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
left join intent_media_production.ad_units on acmpa.ad_unit_id = ad_units.id		
left join intent_media_production.sites on sites.id = ad_units.site_id 
left join intent_media_production.entities e on e.id = sites.publisher_id
group by 
	aggregation_level_date_in_et, 
	e.name,
	sites.name,
	(case
		when lower(intent_media_production.ad_units.name) like '%exit%' then 'Total Exit Units'
		when lower(intent_media_production.ad_units.name) like '%list%' then 'Total List Page'
		else intent_media_production.ad_units.name 
	end),
	(case intent_media_production.ad_units.name
		when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit - Flights - From Homepage' 
		when 'CheapTickets Exit Unit - Firefox' then 'Cheaptickets Exit Unit - Flights - From Homepage'
		else intent_media_production.ad_units.name 
	end)
	
	
	) performance

where performance.Date = fpv.Date 
and performance.Pub = fpv.Pub
and performance.Site = fpv.Site) data

on dimensions.Date = data.Date
and dimensions.Pub = data.Pub
and dimensions.Site = data.Site
and dimensions."Type of Ad Unit" = data."Type of Ad Unit"
and dimensions."Ad Unit" = data."Ad Unit"