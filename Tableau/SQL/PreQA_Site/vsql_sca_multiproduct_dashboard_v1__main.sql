-- flights by placement
select
	'Flights' as "Product Category Type",
	i.date_in_et as Date,
	i."Publisher",
	i."Type of Ad Unit",
	i."Site",
	i."Ad Unit",
	i."Publisher Tier",
	p."Placement",
	p.click_count as "Clicks",
	p.click_conversion_count as "Conversions",
	p.actual_cpc_sum as "Gross Media Revenue",
	p.click_conversion_value_sum as "Conversion Value Sum",
	p.interaction_count as "Interactions",
	i.ad_call_count as "Page Views",
	i.not_pure_ad_call_count as "Not Pure Page Views",
	i.not_pure_low_converting_ad_call_count as "Not Pure Low Value Page Views",
	i.not_pure_low_converting_addressable_ad_call_count as "Addressable Page Views",
	i.pure_ad_call_count as "Pure Page Views",
	i.pure_low_converting_ad_call_count as "Pure Low Value Page Views",
	i.low_converting_ad_call_count as "Low Value Page Views",
	i.not_pure_low_converting_ad_call_with_ads_count as "Fillable Pages",
	i.ad_unit_served_count as "Pages Served",
	i.impression_count as "Impressions",
	i.available_impression_count as "Available Impressions",
	i.net_revenue_sum as "Net Media Revenue",
	i.suppressed_by_route as "Suppressed by Route",
	i.suppressed_by_unknown_hotel_city as "Suppressed by Unknown Hotel City",
	i.suppressed_by_c13n_segment as "Suppressed by Cannibalization Segment",
	i.suppressed_by_click_blackout as "Suppressed by Click Blackout",
	i.suppressed_by_no_valid_layout as "Suppressed by No Valid Layout",
	i.suppressed_by_publisher_traffic_share as "Suppressed by Publisher Traffic Share",
	i.suppressed_by_c13n_above_threshold as "Suppressed by Cannibalization Threshold",
	i.not_pure_low_converting_intentmedia_traffic_share_ad_call_count as "Not Pure Low Value Intent Media Traffic Page Views"
from
	(select 
		aggregation_level_date_in_et as date_in_et,
		(case 
			when e.name = 'Orbitz' then 'OWW' 
			when e.name = 'Expedia' then 'Expedia Inc.'
			when e.name = 'Kayak Software Corporation' then 'Kayak' 
			when e.name = 'Travelocity on Expedia' then 'Expedia Inc.'
			when e.name = 'Hotwire' then 'Expedia Inc.'
			else e.name
		end) as "Publisher",
		(case
			when lower(au.name) like '%exit%' then 'Total Exit Units'
			when lower(au.name) like '%list%' or lower(au.name) like '%rail%' then 'Total List Page'
			when lower(au.name) like '%media fill in%' then 'Total Media Fill In'
			when lower(au.name) like '%trip.com%' then 'Total Trip.com'
			when lower(au.name) like '%ifc%' then 'Total Integrated Form Compare'
			else au.name 
		end) as "Type of Ad Unit",
		s.display_name as "Site",
		(case au.name
			when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit' 
			when 'CheapTickets Exit Unit - Firefox' then 'CheapTickets Exit Unit'
			else au.name 
		end) as "Ad Unit",
		e.publisher_tier as "Publisher Tier",
		sum(ad_call_count) as ad_call_count,
		sum(not_pure_ad_call_count) as not_pure_ad_call_count,
		sum(not_pure_low_converting_ad_call_count) as not_pure_low_converting_ad_call_count,
		sum(not_pure_low_converting_addressable_ad_call_count) as not_pure_low_converting_addressable_ad_call_count,
		sum(pure_ad_call_count) as pure_ad_call_count,
		sum(not_pure_low_converting_ad_call_with_ads_count) as not_pure_low_converting_ad_call_with_ads_count,
		sum(ad_unit_served_count) as ad_unit_served_count,
		sum(impression_count) as impression_count,
		sum(interaction_count) as interaction_count,
		sum(click_count) as click_count,
		sum(gross_revenue_sum) as gross_revenue_sum,
		sum(available_impression_count) as available_impression_count,
		sum(net_revenue_sum) as net_revenue_sum,
		sum(pure_low_converting_ad_call_count) as pure_low_converting_ad_call_count,
		sum(low_converting_ad_call_count) as low_converting_ad_call_count,
		sum(suppressed_by_route) as suppressed_by_route,
		0 as suppressed_by_unknown_hotel_city,
		sum(suppressed_by_c13n_segment) as suppressed_by_c13n_segment,
		sum(suppressed_by_click_blackout) as suppressed_by_click_blackout,
		sum(suppressed_by_no_valid_layout) as suppressed_by_no_valid_layout,
		sum(suppressed_by_publisher_traffic_share) as suppressed_by_publisher_traffic_share,
		sum(suppressed_by_c13n_above_threshold) as suppressed_by_c13n_above_threshold,
		sum(not_pure_low_converting_intentmedia_traffic_share_ad_call_count) as not_pure_low_converting_intentmedia_traffic_share_ad_call_count
	from intent_media_production.air_ct_media_performance_aggregations acmpa
	left join intent_media_production.ad_units au on acmpa.ad_unit_id = au.id	
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
			when e.name = 'Hotwire' then 'Expedia Inc.'
			else e.name
		end),
		(case
			when lower(au.name) like '%exit%' then 'Total Exit Units'
			when lower(au.name) like '%list%' or lower(au.name) like '%rail%' then 'Total List Page'
			when lower(au.name) like '%media fill in%' then 'Total Media Fill In'
			when lower(au.name) like '%trip.com%' then 'Total Trip.com'
			when lower(au.name) like '%ifc%' then 'Total Integrated Form Compare'
			else au.name 
		end),
		s.display_name,
		(case au.name
			when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit' 
			when 'CheapTickets Exit Unit - Firefox' then 'CheapTickets Exit Unit'
			else au.name 
		end),
		e.publisher_tier
	) as i
left join 
	(select
		acptpa.date_in_et,
		(case au.name
			when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit' 
			when 'CheapTickets Exit Unit - Firefox' then 'CheapTickets Exit Unit'
			else au.name 
		end) as "Ad Unit",
		(case
			when au.name like '%Firefox%' then 'Exit Unit FF' 
			when au.name = 'Hotwire Media Fill In' then 'MFI'
			when au.name like '%Trip.com%' then 'Search Form'
			else (case acptpa.placement_type 
					when 'INTER_CARD' then 'Inter Card'
					when 'MINI_CARD' then 'Mini Card'
					when 'RIGHT_RAIL' then 'Rail'
					when 'EXIT_UNIT' then 'Exit Unit'
					when 'FOOTER' then 'Footer'
					when 'TOP_CARD' then 'Top Card'
					when 'FORM_COMPARE' then 'Integrated Form Compare'
					else acptpa.placement_type
				end)
		end) as "Placement",
		sum(acptpa.click_count) as click_count,
		sum(acptpa.click_conversion_count) as click_conversion_count,
		sum(acptpa.actual_cpc_sum) as actual_cpc_sum,
		sum(acptpa.click_conversion_value_sum) as click_conversion_value_sum,
		sum(acptpa.interaction_count) as interaction_count
	from intent_media_production.air_ct_placement_type_performance_aggregations acptpa
	left join intent_media_production.ad_units au on au.id = acptpa.ad_unit_id
	left join intent_media_production.sites s on s.id = au.site_id
	where date_in_et >= au.reporting_start_date
		and date_in_et < date(current_timestamp at timezone 'America/New_York')
		and (not (au.name like '%Firefox%' or au.name = 'Hotwire Media Fill In' or au.name like '%Trip.com%' or acptpa.placement_type = 'EXIT_UNIT'))
	group by 
		acptpa.date_in_et, 
		(case au.name
			when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit' 
			when 'CheapTickets Exit Unit - Firefox' then 'CheapTickets Exit Unit'
			else au.name 
		end),
		(case
			when au.name like '%Firefox%' then 'Exit Unit FF' 
			when au.name = 'Hotwire Media Fill In' then 'MFI'
			when au.name like '%Trip.com%' then 'Search Form'
			else (case acptpa.placement_type 
					when 'INTER_CARD' then 'Inter Card'
					when 'MINI_CARD' then 'Mini Card'
					when 'RIGHT_RAIL' then 'Rail'
					when 'EXIT_UNIT' then 'Exit Unit'
					when 'FOOTER' then 'Footer'
					when 'TOP_CARD' then 'Top Card'
					when 'FORM_COMPARE' then 'Integrated Form Compare'
					else acptpa.placement_type
				end)
		end)
	) p
on i.date_in_et = p.date_in_et
and (i."Ad Unit" = p."Ad Unit" or p."Ad Unit" is null)

union


-- flight total
select
	'Flights' as "Product Category Type",
	i.date_in_et as Date,
	i."Publisher",
	i."Type of Ad Unit",
	i."Site",
	i."Ad Unit",
	i."Publisher Tier",
	'Total List Page' as Placement,
	p.click_count as "Clicks",
	p.click_conversion_count as "Conversions",
	p.actual_cpc_sum as "Gross Media Revenue",
	p.click_conversion_value_sum as "Conversion Value Sum",
	p.interaction_count as "Interactions",
	i.ad_call_count as "Page Views",
	i.not_pure_ad_call_count as "Not Pure Page Views",
	i.not_pure_low_converting_ad_call_count as "Not Pure Low Value Page Views",
	i.not_pure_low_converting_addressable_ad_call_count as "Addressable Page Views",
	i.pure_ad_call_count as "Pure Page Views",
	i.pure_low_converting_ad_call_count as "Pure Low Value Page Views",
	i.low_converting_ad_call_count as "Low Value Page Views",
	i.not_pure_low_converting_ad_call_with_ads_count as "Fillable Pages",
	i.ad_unit_served_count as "Pages Served",
	i.impression_count as "Impressions",
	i.available_impression_count as "Available Impressions",
	i.net_revenue_sum as "Net Media Revenue",
	i.suppressed_by_route as "Suppressed by Route",
	i.suppressed_by_unknown_hotel_city as "Suppressed by Unknown Hotel City",
	i.suppressed_by_c13n_segment as "Suppressed by Cannibalization Segment",
	i.suppressed_by_click_blackout as "Suppressed by Click Blackout",
	i.suppressed_by_no_valid_layout as "Suppressed by No Valid Layout",
	i.suppressed_by_publisher_traffic_share as "Suppressed by Publisher Traffic Share",
	i.suppressed_by_c13n_above_threshold as "Suppressed by Cannibalization Threshold",
	i.not_pure_low_converting_intentmedia_traffic_share_ad_call_count as "Not Pure Low Value Intent Media Traffic Page Views"
from
	(select 
		aggregation_level_date_in_et as date_in_et,
		(case 
			when e.name = 'Orbitz' then 'OWW' 
			when e.name = 'Expedia' then 'Expedia Inc.'
			when e.name = 'Kayak Software Corporation' then 'Kayak' 
			when e.name = 'Travelocity on Expedia' then 'Expedia Inc.'
			when e.name = 'Hotwire' then 'Expedia Inc.'
			else e.name
		end) as "Publisher",
		(case
			when lower(au.name) like '%exit%' then 'Total Exit Units'
			when lower(au.name) like '%list%' or lower(au.name) like '%rail%' then 'Total List Page'
			when lower(au.name) like '%media fill in%' then 'Total Media Fill In'
			when lower(au.name) like '%trip.com%' then 'Total Trip.com'
			when lower(au.name) like '%ifc%' then 'Total Integrated Form Compare'
			else au.name 
		end) as "Type of Ad Unit",
		s.display_name as "Site",
		(case au.name
			when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit' 
			when 'CheapTickets Exit Unit - Firefox' then 'CheapTickets Exit Unit'
			else au.name 
		end) as "Ad Unit",
		e.publisher_tier as "Publisher Tier",
		sum(ad_call_count) as ad_call_count,
		sum(not_pure_ad_call_count) as not_pure_ad_call_count,
		sum(not_pure_low_converting_ad_call_count) as not_pure_low_converting_ad_call_count,
		sum(not_pure_low_converting_addressable_ad_call_count) as not_pure_low_converting_addressable_ad_call_count,
		sum(pure_ad_call_count) as pure_ad_call_count,
		sum(not_pure_low_converting_ad_call_with_ads_count) as not_pure_low_converting_ad_call_with_ads_count,
		sum(ad_unit_served_count) as ad_unit_served_count,
		sum(impression_count) as impression_count,
		sum(interaction_count) as interaction_count,
		sum(click_count) as click_count,
		sum(gross_revenue_sum) as gross_revenue_sum,
		sum(available_impression_count) as available_impression_count,
		sum(net_revenue_sum) as net_revenue_sum,
		sum(pure_low_converting_ad_call_count) as pure_low_converting_ad_call_count,
		sum(low_converting_ad_call_count) as low_converting_ad_call_count,
		sum(suppressed_by_route) as suppressed_by_route,
		0 as suppressed_by_unknown_hotel_city,
		sum(suppressed_by_c13n_segment) as suppressed_by_c13n_segment,
		sum(suppressed_by_click_blackout) as suppressed_by_click_blackout,
		sum(suppressed_by_no_valid_layout) as suppressed_by_no_valid_layout,
		sum(suppressed_by_publisher_traffic_share) as suppressed_by_publisher_traffic_share,
		sum(suppressed_by_c13n_above_threshold) as suppressed_by_c13n_above_threshold,
		sum(not_pure_low_converting_intentmedia_traffic_share_ad_call_count) as not_pure_low_converting_intentmedia_traffic_share_ad_call_count
	from intent_media_production.air_ct_media_performance_aggregations acmpa
	left join intent_media_production.ad_units au on acmpa.ad_unit_id = au.id	
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
			when e.name = 'Hotwire' then 'Expedia Inc.'
			else e.name
		end),
		(case
			when lower(au.name) like '%exit%' then 'Total Exit Units'
			when lower(au.name) like '%list%' or lower(au.name) like '%rail%' then 'Total List Page'
			when lower(au.name) like '%media fill in%' then 'Total Media Fill In'
			when lower(au.name) like '%trip.com%' then 'Total Trip.com'
			when lower(au.name) like '%ifc%' then 'Total Integrated Form Compare'
			else au.name 
		end),
		s.display_name,
		(case au.name
			when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit' 
			when 'CheapTickets Exit Unit - Firefox' then 'CheapTickets Exit Unit'
			else au.name 
		end),
		e.publisher_tier
	) as i
left join 
	(select
		acptpa.date_in_et,
		(case au.name
			when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit' 
			when 'CheapTickets Exit Unit - Firefox' then 'CheapTickets Exit Unit'
			else au.name 
		end) as "Ad Unit",
		sum(acptpa.click_count) as click_count,
		sum(acptpa.click_conversion_count) as click_conversion_count,
		sum(acptpa.actual_cpc_sum) as actual_cpc_sum,
		sum(acptpa.click_conversion_value_sum) as click_conversion_value_sum,
		sum(acptpa.interaction_count) as interaction_count
	from intent_media_production.air_ct_placement_type_performance_aggregations acptpa
	left join intent_media_production.ad_units au on au.id = acptpa.ad_unit_id
	left join intent_media_production.sites s on s.id = au.site_id
	where date_in_et >= au.reporting_start_date
		and date_in_et < date(current_timestamp at timezone 'America/New_York')
		and (not (au.name like '%Firefox%' or au.name = 'Hotwire Media Fill In' or au.name like '%Trip.com%' or acptpa.placement_type = 'EXIT_UNIT'))
	group by 
		acptpa.date_in_et, 
		(case au.name
			when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit' 
			when 'CheapTickets Exit Unit - Firefox' then 'CheapTickets Exit Unit'
			else au.name 
		end)
	) p
on i.date_in_et = p.date_in_et
and (i."Ad Unit" = p."Ad Unit" or p."Ad Unit" is null)

union

-- hotels by placement
select
	'Hotels' as "Product Category Type",
	i.date_in_et as Date,
	i."Publisher",
	i."Type of Ad Unit",
	i."Site",
	i."Ad Unit",
	i."Publisher Tier",
	p."Placement",
	p.click_count as "Clicks",
	p.click_conversion_count as "Conversions",
	p.actual_cpc_sum as "Gross Media Revenue",
	p.click_conversion_value_sum as "Conversion Value Sum",
	p.interaction_count as "Interactions",
	i.ad_call_count as "Page Views",
	i.not_pure_ad_call_count as "Not Pure Page Views",
	i.not_pure_low_converting_ad_call_count as "Not Pure Low Value Page Views",
	i.not_pure_low_converting_addressable_ad_call_count as "Addressable Page Views",
	i.pure_ad_call_count as "Pure Page Views",
	i.pure_low_converting_ad_call_count as "Pure Low Value Page Views",
	i.low_converting_ad_call_count as "Low Value Page Views",
	i.not_pure_low_converting_ad_call_with_ads_count as "Fillable Pages",
	i.ad_unit_served_count as "Pages Served",
	i.impression_count as "Impressions",
	i.available_impression_count as "Available Impressions",
	i.net_revenue_sum as "Net Media Revenue",
	i.suppressed_by_route as "Suppressed by Route",
	i.suppressed_by_unknown_hotel_city as "Suppressed by Unknown Hotel City",
	i.suppressed_by_c13n_segment as "Suppressed by Cannibalization Segment",
	i.suppressed_by_click_blackout as "Suppressed by Click Blackout",
	i.suppressed_by_no_valid_layout as "Suppressed by No Valid Layout",
	i.suppressed_by_publisher_traffic_share as "Suppressed by Publisher Traffic Share",
	i.suppressed_by_c13n_above_threshold as "Suppressed by Cannibalization Threshold",
	i.not_pure_low_converting_intentmedia_traffic_share_ad_call_count as "Not Pure Low Value Intent Media Traffic Page Views"
from
	(select 
		aggregation_level_date_in_et as date_in_et,
		(case 
			when e.name = 'Orbitz' then 'OWW' 
			when e.name = 'Expedia' then 'Expedia Inc.'
			when e.name = 'Kayak Software Corporation' then 'Kayak' 
			when e.name = 'Travelocity on Expedia' then 'Expedia Inc.'
			when e.name = 'Hotwire' then 'Expedia Inc.'
			else e.name
		end) as "Publisher",
		(case
			when lower(au.name) like '%exit%' then 'Total Exit Units'
			when lower(au.name) like '%list%' or lower(au.name) like '%rail%' then 'Total List Page'
			when lower(au.name) like '%media fill in%' then 'Total Media Fill In'
			when lower(au.name) like '%trip.com%' then 'Total Trip.com'
			when lower(au.name) like '%ifc%' then 'Total Integrated Form Compare'
			else au.name 
		end) as "Type of Ad Unit",
		s.display_name as "Site",
		(case au.name
			when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit' 
			when 'CheapTickets Exit Unit - Firefox' then 'CheapTickets Exit Unit'
			else au.name 
		end) as "Ad Unit",
		e.publisher_tier as "Publisher Tier",
		sum(ad_call_count) as ad_call_count,
		sum(not_pure_ad_call_count) as not_pure_ad_call_count,
		sum(not_pure_low_converting_ad_call_count) as not_pure_low_converting_ad_call_count,
		sum(not_pure_low_converting_addressable_ad_call_count) as not_pure_low_converting_addressable_ad_call_count,
		sum(pure_ad_call_count) as pure_ad_call_count,
		sum(not_pure_low_converting_ad_call_with_ads_count) as not_pure_low_converting_ad_call_with_ads_count,
		sum(served_ad_count) as ad_unit_served_count,
		sum(impression_count) as impression_count,
		sum(interaction_count) as interaction_count,
		sum(click_count) as click_count,
		sum(gross_revenue_sum) as gross_revenue_sum,
		sum(available_impression_count) as available_impression_count,
		sum(net_revenue_sum) as net_revenue_sum,
		sum(pure_low_converting_ad_call_count) as pure_low_converting_ad_call_count,
		sum(low_converting_ad_call_count) as low_converting_ad_call_count,
		0 as suppressed_by_route,
		sum(suppressed_by_unknown_hotel_destination) as suppressed_by_unknown_hotel_city,
		0 as suppressed_by_c13n_segment,
		sum(suppressed_by_click_blackout) as suppressed_by_click_blackout,
		sum(suppressed_by_no_valid_layout) as suppressed_by_no_valid_layout,
		sum(suppressed_by_publisher_traffic_share) as suppressed_by_publisher_traffic_share,
		0 as suppressed_by_c13n_above_threshold,
		sum(not_pure_low_converting_intentmedia_traffic_share_ad_call_count) as not_pure_low_converting_intentmedia_traffic_share_ad_call_count
	from intent_media_production.hotel_ct_media_performance_aggregations hcmpa
	left join intent_media_production.ad_units au on hcmpa.ad_unit_id = au.id	
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
			when e.name = 'Hotwire' then 'Expedia Inc.'
			else e.name
		end),
		(case
			when lower(au.name) like '%exit%' then 'Total Exit Units'
			when lower(au.name) like '%list%' or lower(au.name) like '%rail%' then 'Total List Page'
			when lower(au.name) like '%media fill in%' then 'Total Media Fill In'
			when lower(au.name) like '%trip.com%' then 'Total Trip.com'
			when lower(au.name) like '%ifc%' then 'Total Integrated Form Compare'
			else au.name 
		end),
		s.display_name,
		(case au.name
			when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit' 
			when 'CheapTickets Exit Unit - Firefox' then 'CheapTickets Exit Unit'
			else au.name 
		end),
		e.publisher_tier
	) as i
left join 
	(select
		hcptpa.date_in_et,
		(case au.name
			when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit' 
			when 'CheapTickets Exit Unit - Firefox' then 'CheapTickets Exit Unit'
			else au.name 
		end) as "Ad Unit",
		(case
			when au.name like '%Firefox%' then 'Exit Unit FF' 
			when au.name = 'Hotwire Media Fill In' then 'MFI'
			when au.name like '%Trip.com%' then 'Search Form'
			else (case hcptpa.placement_type 
					when 'INTER_CARD' then 'Inter Card'
					when 'MINI_CARD' then 'Mini Card'
					when 'RIGHT_RAIL' then 'Rail'
					when 'EXIT_UNIT' then 'Exit Unit'
					when 'FOOTER' then 'Footer'
					when 'TOP_CARD' then 'Top Card'
					when 'FORM_COMPARE' then 'Integrated Form Compare'
					else hcptpa.placement_type
				end)
		end) as "Placement",
		sum(hcptpa.click_count) as click_count,
		sum(hcptpa.click_conversion_count) as click_conversion_count,
		sum(hcptpa.actual_cpc_sum) as actual_cpc_sum,
		sum(hcptpa.click_conversion_value_sum) as click_conversion_value_sum,
		sum(hcptpa.interaction_count) as interaction_count
	from intent_media_production.hotel_ct_placement_type_performance_aggregations hcptpa
	left join intent_media_production.ad_units au on au.id = hcptpa.ad_unit_id
	left join intent_media_production.sites s on s.id = au.site_id
	where date_in_et >= au.reporting_start_date
		and date_in_et < date(current_timestamp at timezone 'America/New_York')
		and (not (au.name like '%Firefox%' or au.name = 'Hotwire Media Fill In' or au.name like '%Trip.com%' or hcptpa.placement_type = 'EXIT_UNIT'))
	group by 
		hcptpa.date_in_et, 
		(case au.name
			when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit' 
			when 'CheapTickets Exit Unit - Firefox' then 'CheapTickets Exit Unit'
			else au.name 
		end),
		(case
			when au.name like '%Firefox%' then 'Exit Unit FF' 
			when au.name = 'Hotwire Media Fill In' then 'MFI'
			when au.name like '%Trip.com%' then 'Search Form'
			else (case hcptpa.placement_type 
					when 'INTER_CARD' then 'Inter Card'
					when 'MINI_CARD' then 'Mini Card'
					when 'RIGHT_RAIL' then 'Rail'
					when 'EXIT_UNIT' then 'Exit Unit'
					when 'FOOTER' then 'Footer'
					when 'TOP_CARD' then 'Top Card'
					when 'FORM_COMPARE' then 'Integrated Form Compare'
					else hcptpa.placement_type
				end)
		end)
	) p
on i.date_in_et = p.date_in_et
and (i."Ad Unit" = p."Ad Unit" or p."Ad Unit" is null)

union

-- hotel total
select
	'Hotels' as "Product Category Type",
	i.date_in_et as Date,
	i."Publisher",
	i."Type of Ad Unit",
	i."Site",
	i."Ad Unit",
	i."Publisher Tier",
	'Total List Page' as Placement,
	p.click_count as "Clicks",
	p.click_conversion_count as "Conversions",
	p.actual_cpc_sum as "Gross Media Revenue",
	p.click_conversion_value_sum as "Conversion Value Sum",
	p.interaction_count as "Interactions",
	i.ad_call_count as "Page Views",
	i.not_pure_ad_call_count as "Not Pure Page Views",
	i.not_pure_low_converting_ad_call_count as "Not Pure Low Value Page Views",
	i.not_pure_low_converting_addressable_ad_call_count as "Addressable Page Views",
	i.pure_ad_call_count as "Pure Page Views",
	i.pure_low_converting_ad_call_count as "Pure Low Value Page Views",
	i.low_converting_ad_call_count as "Low Value Page Views",
	i.not_pure_low_converting_ad_call_with_ads_count as "Fillable Pages",
	i.ad_unit_served_count as "Pages Served",
	i.impression_count as "Impressions",
	i.available_impression_count as "Available Impressions",
	i.net_revenue_sum as "Net Media Revenue",
	i.suppressed_by_route as "Suppressed by Route",
	i.suppressed_by_unknown_hotel_city as "Suppressed by Unknown Hotel City",
	i.suppressed_by_c13n_segment as "Suppressed by Cannibalization Segment",
	i.suppressed_by_click_blackout as "Suppressed by Click Blackout",
	i.suppressed_by_no_valid_layout as "Suppressed by No Valid Layout",
	i.suppressed_by_publisher_traffic_share as "Suppressed by Publisher Traffic Share",
	i.suppressed_by_c13n_above_threshold as "Suppressed by Cannibalization Threshold",
	i.not_pure_low_converting_intentmedia_traffic_share_ad_call_count as "Not Pure Low Value Intent Media Traffic Page Views"
from
	(select 
		aggregation_level_date_in_et as date_in_et,
		(case 
			when e.name = 'Orbitz' then 'OWW' 
			when e.name = 'Expedia' then 'Expedia Inc.'
			when e.name = 'Kayak Software Corporation' then 'Kayak' 
			when e.name = 'Travelocity on Expedia' then 'Expedia Inc.'
			when e.name = 'Hotwire' then 'Expedia Inc.'
			else e.name
		end) as "Publisher",
		(case
			when lower(au.name) like '%exit%' then 'Total Exit Units'
			when lower(au.name) like '%list%' or lower(au.name) like '%rail%' then 'Total List Page'
			when lower(au.name) like '%media fill in%' then 'Total Media Fill In'
			when lower(au.name) like '%trip.com%' then 'Total Trip.com'
			when lower(au.name) like '%ifc%' then 'Total Integrated Form Compare'
			else au.name 
		end) as "Type of Ad Unit",
		s.display_name as "Site",
		(case au.name
			when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit' 
			when 'CheapTickets Exit Unit - Firefox' then 'CheapTickets Exit Unit'
			else au.name 
		end) as "Ad Unit",
		e.publisher_tier as "Publisher Tier",
		sum(ad_call_count) as ad_call_count,
		sum(not_pure_ad_call_count) as not_pure_ad_call_count,
		sum(not_pure_low_converting_ad_call_count) as not_pure_low_converting_ad_call_count,
		sum(not_pure_low_converting_addressable_ad_call_count) as not_pure_low_converting_addressable_ad_call_count,
		sum(pure_ad_call_count) as pure_ad_call_count,
		sum(not_pure_low_converting_ad_call_with_ads_count) as not_pure_low_converting_ad_call_with_ads_count,
		sum(served_ad_count) as ad_unit_served_count,
		sum(impression_count) as impression_count,
		sum(interaction_count) as interaction_count,
		sum(click_count) as click_count,
		sum(gross_revenue_sum) as gross_revenue_sum,
		sum(available_impression_count) as available_impression_count,
		sum(net_revenue_sum) as net_revenue_sum,
		sum(pure_low_converting_ad_call_count) as pure_low_converting_ad_call_count,
		sum(low_converting_ad_call_count) as low_converting_ad_call_count,
		0 as suppressed_by_route,
		sum(suppressed_by_unknown_hotel_destination) as suppressed_by_unknown_hotel_city,
		0 as suppressed_by_c13n_segment,
		sum(suppressed_by_click_blackout) as suppressed_by_click_blackout,
		sum(suppressed_by_no_valid_layout) as suppressed_by_no_valid_layout,
		sum(suppressed_by_publisher_traffic_share) as suppressed_by_publisher_traffic_share,
		0 as suppressed_by_c13n_above_threshold,
		sum(not_pure_low_converting_intentmedia_traffic_share_ad_call_count) as not_pure_low_converting_intentmedia_traffic_share_ad_call_count
	from intent_media_production.hotel_ct_media_performance_aggregations hcmpa
	left join intent_media_production.ad_units au on hcmpa.ad_unit_id = au.id	
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
			when e.name = 'Hotwire' then 'Expedia Inc.'
			else e.name
		end),
		(case
			when lower(au.name) like '%exit%' then 'Total Exit Units'
			when lower(au.name) like '%list%' or lower(au.name) like '%rail%' then 'Total List Page'
			when lower(au.name) like '%media fill in%' then 'Total Media Fill In'
			when lower(au.name) like '%trip.com%' then 'Total Trip.com'
			when lower(au.name) like '%ifc%' then 'Total Integrated Form Compare'
			else au.name 
		end),
		s.display_name,
		(case au.name
			when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit' 
			when 'CheapTickets Exit Unit - Firefox' then 'CheapTickets Exit Unit'
			else au.name 
		end),
		e.publisher_tier
	) as i
left join 
	(select
		hcptpa.date_in_et,
		(case au.name
			when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit' 
			when 'CheapTickets Exit Unit - Firefox' then 'CheapTickets Exit Unit'
			else au.name 
		end) as "Ad Unit",
		sum(hcptpa.click_count) as click_count,
		sum(hcptpa.click_conversion_count) as click_conversion_count,
		sum(hcptpa.actual_cpc_sum) as actual_cpc_sum,
		sum(hcptpa.click_conversion_value_sum) as click_conversion_value_sum,
		sum(hcptpa.interaction_count) as interaction_count
	from intent_media_production.hotel_ct_placement_type_performance_aggregations hcptpa
	left join intent_media_production.ad_units au on au.id = hcptpa.ad_unit_id
	left join intent_media_production.sites s on s.id = au.site_id
	where date_in_et >= au.reporting_start_date
		and date_in_et < date(current_timestamp at timezone 'America/New_York')
		and (not (au.name like '%Firefox%' or au.name = 'Hotwire Media Fill In' or au.name like '%Trip.com%' or hcptpa.placement_type = 'EXIT_UNIT'))
	group by 
		hcptpa.date_in_et, 
		(case au.name
			when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit' 
			when 'CheapTickets Exit Unit - Firefox' then 'CheapTickets Exit Unit'
			else au.name 
		end)
	) p
on i.date_in_et = p.date_in_et
and (i."Ad Unit" = p."Ad Unit" or p."Ad Unit" is null)