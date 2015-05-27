-- Flights --
select 
	'Flights' as "Product Category Type",
	dimensions.*,
	data."Page Views", 
	data."Not Pure Page Views", 
	data."Not Pure Low Value Page Views", 
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
	data."Pure Low Value Page Views", 
	data."Low Value Page Views", 
	data."Suppressed - Route",
	data."Suppressed - Cannibalization Segment",
	data."Suppressed - Click Blackout",
	data."Suppressed - No Valid Layout",
	data."Suppressed - Publisher Traffic Share",
	data."Suppressed - Cannibalization Threshold",
	data."Not Pure Low Value Intent Media Traffic Page Views"
from -- dimensions (Date, Zero, Pub, Site, Type of Ad Unit, Ad Unit) --
	(select *
	from
		(select 
			distinct(aggregation_level_date_in_et) as Date,
			0 as Zero
		from intent_media_production.air_ct_media_performance_aggregations
		where aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York')) dates,
		(select 
			(case 
				when e.name = 'Orbitz' then 'OWW' 
				when e.name = 'Expedia' then 'Expedia Inc.'
				when e.name = 'Kayak Software Corporation' then 'Kayak' 
				when e.name = 'Travelocity on Expedia' then 'Expedia Inc.'
				when e.name = 'Hotwire' then 'Expedia Inc.'
				else e.name
			end) as "Pub",
			s.display_name as "Site",
			(case
				when lower(au.name) like '%exit%' then 'Total Exit Units'
				when lower(au.name) like '%list%' or lower(au.name) like '%rail%' then 'Total List Page'
				when lower(au.name) like '%media fill in%' then 'Total Media Fill In'
				when lower(au.name) like '%trip.com%' then 'Total Trip.com'
				when lower(au.name) like '%ifc%' then 'Total Integrated Form Compare'
				else au.name 
			end) as "Type of Ad Unit",
			(case au.name
				when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit - Flights - From Homepage' 
				when 'CheapTickets Exit Unit - Firefox' then 'Cheaptickets Exit Unit - Flights - From Homepage'
				else au.name 
			end) as "Ad Unit",
			e.publisher_tier as "Publisher Tier",
			acptpa.placement_type as "Placement"
		from intent_media_production.air_ct_media_performance_aggregations acmpa
		left join intent_media_production.air_ct_placement_type_performance_aggregations acptpa on acmpa.ad_unit_id = acptpa.ad_unit_id
		left join intent_media_production.ad_units au on au.id = acmpa.ad_unit_id
		left join intent_media_production.legal_page_types lpt on lpt.id = au.legal_page_type_id
		left join intent_media_production.sites s on au.site_id = s.id
		left join intent_media_production.entities e on s.publisher_id = e.id
		where aggregation_level_date_in_et >= au.reporting_start_date
		group by 1,2,3,4,5,6) ad_unit_names
	) dimensions
left join -- data --
	(select 
		aggregation_level_date_in_et as Date,
		(case 
			when e.name = 'Orbitz' then 'OWW' 
			when e.name = 'Expedia' then 'Expedia Inc.'
			when e.name = 'Kayak Software Corporation' then 'Kayak' 
			when e.name = 'Travelocity on Expedia' then 'Expedia Inc.'
			when e.name = 'Hotwire' then 'Expedia Inc.'
			else e.name
		end) as "Pub",
		s.display_name as "Site",
		(case
			when lower(au.name) like '%exit%' then 'Total Exit Units'
			when lower(au.name) like '%list%' or lower(au.name) like '%rail%' then 'Total List Page'
			when lower(au.name) like '%media fill in%' then 'Total Media Fill In'
			when lower(au.name) like '%trip.com%' then 'Total Trip.com'
			when lower(au.name) like '%ifc%' then 'Total Integrated Form Compare'
			else au.name 
		end) as "Type of Ad Unit",
		(case au.name
			when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit - Flights - From Homepage' 
			when 'CheapTickets Exit Unit - Firefox' then 'Cheaptickets Exit Unit - Flights - From Homepage'
			else au.name 
		end) as "Ad Unit",
		e.publisher_tier as "Publisher Tier",
		sum(ad_call_count) as "Page Views",
		sum(not_pure_ad_call_count) as "Not Pure Page Views",
		sum(not_pure_low_converting_ad_call_count) as "Not Pure Low Value Page Views",
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
		sum(pure_low_converting_ad_call_count) as "Pure Low Value Page Views",
		sum(low_converting_ad_call_count) as "Low Value Page Views",
		sum(suppressed_by_route) as "Suppressed - Route",
		sum(suppressed_by_c13n_segment) as "Suppressed - Cannibalization Segment",
		sum(suppressed_by_click_blackout) as "Suppressed - Click Blackout",
		sum(suppressed_by_no_valid_layout) as "Suppressed - No Valid Layout",
		sum(suppressed_by_publisher_traffic_share) as "Suppressed - Publisher Traffic Share",
		sum(suppressed_by_c13n_above_threshold) as "Suppressed - Cannibalization Threshold",
		sum(not_pure_low_converting_intentmedia_traffic_share_ad_call_count) as "Not Pure Low Value Intent Media Traffic Page Views",
		sum(acuva.count) as "Unique Visitors"
	from intent_media_production.air_ct_media_performance_aggregations acmpa
	left join intent_media_production.ad_units au on acmpa.ad_unit_id = au.id
	left join intent_media_production.legal_page_types lpt on lpt.id = au.legal_page_type_id
	left join intent_media_production.sites s on s.id = au.site_id 
	left join intent_media_production.entities e on e.id = s.publisher_id
	left join intent_media_production.air_ct_unique_visitor_aggregations acuva on acuva.site_id = s.id
	where aggregation_level_date_in_et >= au.reporting_start_date
		and acuva.destination is null
		and acuva.segmentation_type is null
		and acuva.pure_group_type is null
	group by 1,2,3,4,5,6
	) data
on dimensions.Date = data.Date
	and dimensions.Pub = data.Pub
	and dimensions.Site = data.Site
	and dimensions."Type of Ad Unit" = data."Type of Ad Unit"
	and dimensions."Ad Unit" = data."Ad Unit"

union

-- Hotels --
select 
	'Hotels' as "Product Category Type",
	dimensions.*,
	data."Page Views",
	data."Not Pure Page Views",
	data."Not Pure Low Value Page Views",
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
	data."Pure Low Value Page Views",
	data."Low Value Page Views",
	data."Suppressed - Route",
	data."Suppressed - Cannibalization Segment",
	data."Suppressed - Click Blackout",
	data."Suppressed - No Valid Layout",
	data."Suppressed - Publisher Traffic Share",
	data."Suppressed - Cannibalization Threshold",
	data."Not Pure Low Value Intent Media Traffic Page Views"
from -- dimensions (Date, Zero, Pub, Site, Type of Ad Unit, Ad Unit) --
	(select *
	from
		(select 
			distinct(aggregation_level_date_in_et) as Date,
			0 as Zero
		from intent_media_production.air_ct_media_performance_aggregations
		where aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York')) dates,
		(select 
			(case 
				when e.name = 'Orbitz' then 'OWW' 
				when e.name = 'Expedia' then 'Expedia Inc.'
				when e.name = 'Kayak Software Corporation' then 'Kayak' 
				when e.name = 'Travelocity on Expedia' then 'Expedia Inc.'
				else e.name
			end) as "Pub",
			s.display_name as "Site",
			(case
				when lower(au.name) like '%exit%' then 'Total Exit Units'
				when lower(au.name) like '%list%' or lower(au.name) like '%rail%' then 'Total List Page'
				when lower(au.name) like '%media fill in%' then 'Total Media Fill In'
				when lower(au.name) like '%trip.com%' then 'Total Trip.com'
				when lower(au.name) like '%ifc%' then 'Total Integrated Form Compare'
				else au.name 
			end) as "Type of Ad Unit",
			(case au.name
				when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit - Flights - From Homepage' 
				when 'CheapTickets Exit Unit - Firefox' then 'Cheaptickets Exit Unit - Flights - From Homepage'
				else au.name 
			end) as "Ad Unit",
			e.publisher_tier as "Publisher Tier"
		from intent_media_production.hotel_ct_media_performance_aggregations hcmpa
		left join intent_media_production.ad_units au on au.id = hcmpa.ad_unit_id
		left join intent_media_production.legal_page_types lpt on lpt.id = au.legal_page_type_id	
		left join intent_media_production.sites s on au.site_id = s.id
		left join intent_media_production.entities e on s.publisher_id = e.id
		where aggregation_level_date_in_et >= au.reporting_start_date
		group by 1,2,3,4,5) ad_unit_names
	) dimensions
left join -- data --
	(select 
		aggregation_level_date_in_et as Date,
		(case 
			when e.name = 'Orbitz' then 'OWW' 
			when e.name = 'Expedia' then 'Expedia Inc.'
			when e.name = 'Kayak Software Corporation' then 'Kayak' 
			when e.name = 'Travelocity on Expedia' then 'Expedia Inc.'
			when e.name = 'Hotwire' then 'Expedia Inc.'
			else e.name
		end) as "Pub",
		s.display_name as "Site",
		(case
			when lower(au.name) like '%exit%' then 'Total Exit Units'
			when lower(au.name) like '%list%' or lower(au.name) like '%rail%' then 'Total List Page'
			when lower(au.name) like '%media fill in%' then 'Total Media Fill In'
			when lower(au.name) like '%trip.com%' then 'Total Trip.com'
			when lower(au.name) like '%ifc%' then 'Total Integrated Form Compare'
			else au.name 
		end) as "Type of Ad Unit",
		(case au.name
			when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit - Flights - From Homepage' 
			when 'CheapTickets Exit Unit - Firefox' then 'Cheaptickets Exit Unit - Flights - From Homepage'
			else au.name 
		end) as "Ad Unit",
		e.publisher_tier as "Publisher Tier",
		sum(ad_call_count) as "Page Views",
		sum(not_pure_ad_call_count) as "Not Pure Page Views",
		sum(not_pure_low_converting_ad_call_count) as "Not Pure Low Value Page Views",
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
		sum(pure_low_converting_ad_call_count) as "Pure Low Value Page Views",
		sum(low_converting_ad_call_count) as "Low Value Page Views",
		sum(suppressed_by_unknown_hotel_destination) as "Suppressed - Route",
		cast(null as integer) as "Suppressed - Cannibalization Segment",
		sum(suppressed_by_click_blackout) as "Suppressed - Click Blackout",
		sum(suppressed_by_no_valid_layout) as "Suppressed - No Valid Layout",
		sum(suppressed_by_publisher_traffic_share) as "Suppressed - Publisher Traffic Share",
		cast(null as integer) as "Suppressed - Cannibalization Threshold",
		sum(not_pure_low_converting_intentmedia_traffic_share_ad_call_count) as "Not Pure Low Value Intent Media Traffic Page Views"
	from intent_media_production.hotel_ct_media_performance_aggregations hcmpa
	left join intent_media_production.ad_units au on hcmpa.ad_unit_id = au.id
	left join intent_media_production.legal_page_types lpt on lpt.id = au.legal_page_type_id	
	left join intent_media_production.sites s on s.id = au.site_id 
	left join intent_media_production.entities e on e.id = s.publisher_id 
	where aggregation_level_date_in_et >= au.reporting_start_date
	group by 1,2,3,4,5,6	
	) data
on dimensions.Date = data.Date
	and dimensions.Pub = data.Pub
	and dimensions.Site = data.Site
	and dimensions."Type of Ad Unit" = data."Type of Ad Unit"
	and dimensions."Ad Unit" = data."Ad Unit"