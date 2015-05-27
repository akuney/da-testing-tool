-- flight by placement
select 
	'Flights' AS 'Product Category Type',
	p.date_in_et AS Date,
	(case when e.name = 'Orbitz' then 'OWW' when e.name = 'Kayak Software Corporation' then 'Kayak' else e.name end) as "Pub",
	s.display_name AS "Site",
	(CASE au.name
		WHEN 'Orbitz Exit Unit - Firefox' THEN 'Orbitz Exit Unit' 
		WHEN 'CheapTickets Exit Unit - Firefox' THEN 'CheapTickets Exit Unit'
		ELSE au.name 
	END) AS "Ad Unit",
	au.id as "Ad Unit ID",
	p."Placement",
	p.click_count as "Clicks",
	p.actual_cpc_sum as "Gross Media Revenue",
	p.interaction_count as "Interactions",
	i.ad_call_count as "Page Views",
	i.not_pure_low_converting_ad_call_count as "Not Pure Low Converting Page Views",
	i.not_pure_low_converting_addressable_ad_call_count as "Addressable Page Views",
	i.ad_unit_served_count as "Pages Served",
	i.suppressed_by_publisher_traffic_share as "Suppressed by Publisher Traffic Share",
	(i.suppressed_by_route + i.suppressed_by_unknown_hotel_city + i.suppressed_by_c13n_segment + i.suppressed_by_click_blackout + i.suppressed_by_no_valid_layout + i.suppressed_by_c13n_above_threshold) as "Suppressed by Other Business Rules"
from 
	(select
		acptpa.date_in_et,
		acptpa.ad_unit_id,
		(CASE
			when au.name LIKE '%Firefox%' then 'Exit Unit FF' 
			when au.name = 'Hotwire Media Fill In' then 'MFI'
			when au.name like '%Trip.com%' then 'Search Form'
			else (CASE acptpa.placement_type 
					WHEN 'INTER_CARD' THEN 'Inter Card'
					WHEN 'MINI_CARD' THEN 'Mini Card'
					WHEN 'RIGHT_RAIL' THEN 'Rail'
					WHEN 'EXIT_UNIT' THEN 'Exit Unit'
					WHEN 'FOOTER' THEN 'Footer'
					WHEN 'FORM_COMPARE' then 'Integrated Form Compare'
					ELSE acptpa.placement_type
				END)
		END) AS "Placement",
		sum(acptpa.click_count) as click_count,
		sum(acptpa.click_conversion_count) as click_conversion_count,
		sum(acptpa.actual_cpc_sum) as actual_cpc_sum,
		sum(acptpa.click_conversion_value_sum) as click_conversion_value_sum,
		sum(acptpa.interaction_count) as interaction_count
	from intent_media_production.air_ct_placement_type_performance_aggregations acptpa
	left join intent_media_production.ad_units au on au.id = acptpa.ad_unit_id
	left join intent_media_production.sites s on s.id = au.site_id
	where date_in_et < date(current_timestamp at timezone 'America/New_York')
		and ((date_in_et > '2013-07-29' and s.name = 'TRIPADVISOR')
		or (date_in_et > '2013-10-27' and s.name = 'LOWFARES')
		or (date_in_et > '2013-10-28' and s.name = 'WEBJET')
		or (s.name not in ('TRIPADVISOR','LOWFARES','WEBJET') and date_in_et > '2012-03-26')) 
	group by 
		acptpa.date_in_et, 
		acptpa.ad_unit_id,
		(CASE
			when au.name LIKE '%Firefox%' then 'Exit Unit FF' 
			when au.name = 'Hotwire Media Fill In' then 'MFI'
			when au.name like '%Trip.com%' then 'Search Form'
			else (CASE acptpa.placement_type 
					WHEN 'INTER_CARD' THEN 'Inter Card'
					WHEN 'MINI_CARD' THEN 'Mini Card'
					WHEN 'RIGHT_RAIL' THEN 'Rail'
					WHEN 'EXIT_UNIT' THEN 'Exit Unit'
					WHEN 'FOOTER' THEN 'Footer'
					WHEN 'FORM_COMPARE' then 'Integrated Form Compare'
					ELSE acptpa.placement_type
				END)
		END) ) p
left join intent_media_production.ad_units au on p.ad_unit_id = au.id
left join intent_media_production.sites s on au.site_id = s.id
left join intent_media_production.entities e on e.id = s.publisher_id
join 
	(select 
		aggregation_level_date_in_et as date_in_et,
		ad_unit_id,		
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
		sum(pure_low_converting_ad_call_count) as pure_low_converting_ad_call_count,
		sum(low_converting_ad_call_count) as low_converting_ad_call_count,
		sum(suppressed_by_route) as suppressed_by_route,
		0 as suppressed_by_unknown_hotel_city,
		sum(suppressed_by_c13n_segment) as suppressed_by_c13n_segment,
		sum(suppressed_by_click_blackout) as suppressed_by_click_blackout,
		sum(suppressed_by_no_valid_layout) as suppressed_by_no_valid_layout,
		sum(suppressed_by_publisher_traffic_share) as suppressed_by_publisher_traffic_share,
		sum(suppressed_by_c13n_above_threshold) as suppressed_by_c13n_above_threshold
	from intent_media_production.air_ct_media_performance_aggregations acmpa
	left join intent_media_production.ad_units au on au.id = acmpa.ad_unit_id
	left join intent_media_production.sites s on s.id = au.site_id
	where aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York')
		and ((aggregation_level_date_in_et > '2013-07-29' and s.name = 'TRIPADVISOR')
		or (aggregation_level_date_in_et > '2013-10-27' and s.name = 'LOWFARES')
		or (aggregation_level_date_in_et > '2013-10-28' and s.name = 'WEBJET')
		or (s.name not in ('TRIPADVISOR','LOWFARES','WEBJET') and aggregation_level_date_in_et > '2012-03-26')) 
	group by aggregation_level_date_in_et, ad_unit_id) as i
on i.date_in_et = p.date_in_et
and i.ad_unit_id = p.ad_unit_id

union


-- flight total
select
	'Flights' AS 'Product Category Type',
	p.date_in_et AS Date,
	(case when e.name = 'Orbitz' then 'OWW' when e.name = 'Kayak Software Corporation' then 'Kayak' else e.name end) as "Pub",
	s.display_name AS "Site",
	(CASE au.name
		WHEN 'Orbitz Exit Unit - Firefox' THEN 'Orbitz Exit Unit' 
		WHEN 'CheapTickets Exit Unit - Firefox' THEN 'CheapTickets Exit Unit'
		ELSE au.name 
	END) AS "Ad Unit",
	au.id as "Ad Unit ID",
	'Total List Page' as Placement,
	p.click_count as "Clicks",
	p.actual_cpc_sum as "Gross Media Revenue",
	p.interaction_count as "Interactions",
	i.ad_call_count as "Page Views",
	i.not_pure_low_converting_ad_call_count as "Not Pure Low Converting Page Views",
	i.not_pure_low_converting_addressable_ad_call_count as "Addressable Page Views",
	i.ad_unit_served_count as "Pages Served",
	i.suppressed_by_publisher_traffic_share as "Suppressed by Publisher Traffic Share",
	(i.suppressed_by_route + i.suppressed_by_unknown_hotel_city + i.suppressed_by_c13n_segment + i.suppressed_by_click_blackout + i.suppressed_by_no_valid_layout + i.suppressed_by_c13n_above_threshold) as "Suppressed by Other Business Rules"
from
	(select
		acptpa.date_in_et,
		acptpa.ad_unit_id,
		sum(acptpa.click_count) as click_count,
		sum(acptpa.click_conversion_count) as click_conversion_count,
		sum(acptpa.actual_cpc_sum) as actual_cpc_sum,
		sum(acptpa.click_conversion_value_sum) as click_conversion_value_sum,
		sum(acptpa.interaction_count) as interaction_count
	from intent_media_production.air_ct_placement_type_performance_aggregations acptpa
	left join intent_media_production.ad_units au on au.id = acptpa.ad_unit_id
	left join intent_media_production.sites s on s.id = au.site_id
	where date_in_et < date(current_timestamp at timezone 'America/New_York')
		and ((date_in_et > '2013-07-29' and s.name = 'TRIPADVISOR')
		or (date_in_et > '2013-10-27' and s.name = 'LOWFARES')
		or (date_in_et > '2013-10-28' and s.name = 'WEBJET')
		or (s.name not in ('TRIPADVISOR','LOWFARES','WEBJET') and date_in_et > '2012-03-26'))
		and (not (au.name like '%Firefox%' or au.name = 'Hotwire Media Fill In' or au.name like '%Trip.com%' or acptpa.placement_type = 'EXIT_UNIT'))
	group by 
		acptpa.date_in_et, 
		acptpa.ad_unit_id) p
left join intent_media_production.ad_units au on p.ad_unit_id = au.id
left join intent_media_production.sites s on au.site_id = s.id
left join intent_media_production.entities e on e.id = s.publisher_id
join 
	(select 
		aggregation_level_date_in_et as date_in_et,
		ad_unit_id,
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
		sum(suppressed_by_c13n_above_threshold) as suppressed_by_c13n_above_threshold
	from intent_media_production.air_ct_media_performance_aggregations acmpa
	left join intent_media_production.ad_units au on acmpa.ad_unit_id = au.id	
	left join intent_media_production.sites s on s.id = au.site_id		
	where aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York')
		and ((aggregation_level_date_in_et > '2013-07-29' and s.name = 'TRIPADVISOR')
		or (aggregation_level_date_in_et > '2013-10-27' and s.name = 'LOWFARES')
		or (aggregation_level_date_in_et > '2013-10-28' and s.name = 'WEBJET')
		or (s.name not in ('TRIPADVISOR','LOWFARES','WEBJET') and aggregation_level_date_in_et > '2012-03-26'))
	group by 
		aggregation_level_date_in_et, 
		ad_unit_id) as i
on i.date_in_et = p.date_in_et
and i.ad_unit_id = p.ad_unit_id


union
	
-- hotel by placement
select 
	'Hotels' AS 'Product Category Type',
	p.date_in_et AS Date,
	(case when e.name = 'Orbitz' then 'OWW' when e.name = 'Kayak Software Corporation' then 'Kayak' else e.name end) as "Pub",
	s.display_name AS "Site",
	(CASE au.name
		WHEN 'Orbitz Exit Unit - Firefox' THEN 'Orbitz Exit Unit' 
		WHEN 'CheapTickets Exit Unit - Firefox' THEN 'CheapTickets Exit Unit'
		ELSE au.name 
	END) AS "Ad Unit",
	au.id as "Ad Unit ID",
	p."Placement",
	p.click_count as "Clicks",
	p.actual_cpc_sum as "Gross Media Revenue",
	p.interaction_count as "Interactions",
	i.ad_call_count as "Page Views",
	i.not_pure_low_converting_ad_call_count as "Not Pure Low Converting Page Views",
	i.not_pure_low_converting_addressable_ad_call_count as "Addressable Page Views",
	i.ad_unit_served_count as "Pages Served",
	i.suppressed_by_publisher_traffic_share as "Suppressed by Publisher Traffic Share",
	(i.suppressed_by_route + i.suppressed_by_unknown_hotel_city + i.suppressed_by_c13n_segment + i.suppressed_by_click_blackout + i.suppressed_by_no_valid_layout + i.suppressed_by_c13n_above_threshold) as "Suppressed by Other Business Rules"
from 
	(select
		hcptpa.date_in_et,
		hcptpa.ad_unit_id,
		(CASE
			when au.name LIKE '%Firefox%' then 'Exit Unit FF' 
			when au.name = 'Hotwire Media Fill In' then 'MFI'
			when au.name like '%Trip.com%' then 'Search Form'
			else (CASE hcptpa.placement_type 
					WHEN 'INTER_CARD' THEN 'Inter Card'
					WHEN 'MINI_CARD' THEN 'Mini Card'
					WHEN 'RIGHT_RAIL' THEN 'Rail'
					WHEN 'EXIT_UNIT' THEN 'Exit Unit'
					WHEN 'FOOTER' THEN 'Footer'
					WHEN 'FORM_COMPARE' then 'Integrated Form Compare'
					ELSE hcptpa.placement_type
				END)
		END) AS "Placement",
		sum(hcptpa.click_count) as click_count,
		sum(hcptpa.click_conversion_count) as click_conversion_count,
		sum(hcptpa.actual_cpc_sum) as actual_cpc_sum,
		sum(hcptpa.click_conversion_value_sum) as click_conversion_value_sum,
		sum(hcptpa.interaction_count) as interaction_count
	from intent_media_production.hotel_ct_placement_type_performance_aggregations hcptpa
	left join intent_media_production.ad_units au on au.id = hcptpa.ad_unit_id
	left join intent_media_production.sites s on s.id = au.site_id
	where date_in_et < date(current_timestamp at timezone 'America/New_York')
		and ((date_in_et > '2013-07-29' and s.name = 'TRIPADVISOR')
		or (date_in_et > '2013-10-27' and s.name = 'LOWFARES')
		or (date_in_et > '2013-10-28' and s.name = 'WEBJET')
		or (s.name not in ('TRIPADVISOR','LOWFARES','WEBJET') and date_in_et > '2012-03-26'))
	group by 
		hcptpa.date_in_et, 
		hcptpa.ad_unit_id,
		(CASE
		when au.name LIKE '%Firefox%' then 'Exit Unit FF' 
		when au.name = 'Hotwire Media Fill In' then 'MFI'
		when au.name like '%Trip.com%' then 'Search Form'
		else (CASE hcptpa.placement_type 
				WHEN 'INTER_CARD' THEN 'Inter Card'
				WHEN 'MINI_CARD' THEN 'Mini Card'
				WHEN 'RIGHT_RAIL' THEN 'Rail'
				WHEN 'EXIT_UNIT' THEN 'Exit Unit'
				WHEN 'FOOTER' THEN 'Footer'
				WHEN 'FORM_COMPARE' then 'Integrated Form Compare'
				ELSE hcptpa.placement_type
			END)
	END) ) p
left join intent_media_production.ad_units au on p.ad_unit_id = au.id
left join intent_media_production.sites s on au.site_id = s.id
left join intent_media_production.entities e on e.id = s.publisher_id
join 
	(select 
		aggregation_level_date_in_et as date_in_et,
		ad_unit_id,		
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
		sum(pure_low_converting_ad_call_count) as pure_low_converting_ad_call_count,
		sum(low_converting_ad_call_count) as low_converting_ad_call_count,
		0 as suppressed_by_route,
		sum(suppressed_by_unknown_hotel_destination) as suppressed_by_unknown_hotel_city,
		0 as suppressed_by_c13n_segment,
		sum(suppressed_by_click_blackout) as suppressed_by_click_blackout,
		sum(suppressed_by_no_valid_layout) as suppressed_by_no_valid_layout,
		sum(suppressed_by_publisher_traffic_share) as suppressed_by_publisher_traffic_share,
		0 as suppressed_by_c13n_above_threshold
	from intent_media_production.hotel_ct_media_performance_aggregations hcmpa
	left join intent_media_production.ad_units au on au.id = hcmpa.ad_unit_id
	left join intent_media_production.sites s on s.id = au.site_id
	where aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York')
		and ((aggregation_level_date_in_et > '2013-07-29' and s.name = 'TRIPADVISOR')
		or (aggregation_level_date_in_et > '2013-10-27' and s.name = 'LOWFARES')
		or (aggregation_level_date_in_et > '2013-10-28' and s.name = 'WEBJET')
		or (s.name not in ('TRIPADVISOR','LOWFARES','WEBJET') and aggregation_level_date_in_et > '2012-03-26')) 	
	group by aggregation_level_date_in_et, ad_unit_id) as i
on i.date_in_et = p.date_in_et
and i.ad_unit_id = p.ad_unit_id
	
union

-- hotel total
select
	'Hotels' AS 'Product Category Type',
	p.date_in_et AS Date,
	(case when e.name = 'Orbitz' then 'OWW' when e.name = 'Kayak Software Corporation' then 'Kayak' else e.name end) as "Pub",
	s.display_name AS "Site",
	(CASE au.name
		WHEN 'Orbitz Exit Unit - Firefox' THEN 'Orbitz Exit Unit' 
		WHEN 'CheapTickets Exit Unit - Firefox' THEN 'CheapTickets Exit Unit'
		ELSE au.name 
	END) AS "Ad Unit",
	au.id as "Ad Unit ID",
	'Total List Page' as Placement,
	p.click_count as "Clicks",
	p.actual_cpc_sum as "Gross Media Revenue",
	p.interaction_count as "Interactions",
	i.ad_call_count as "Page Views",
	i.not_pure_low_converting_ad_call_count as "Not Pure Low Converting Page Views",
	i.not_pure_low_converting_addressable_ad_call_count as "Addressable Page Views",
	i.ad_unit_served_count as "Pages Served",
	i.suppressed_by_publisher_traffic_share as "Suppressed by Publisher Traffic Share",
	(i.suppressed_by_route + i.suppressed_by_unknown_hotel_city + i.suppressed_by_c13n_segment + i.suppressed_by_click_blackout + i.suppressed_by_no_valid_layout + i.suppressed_by_c13n_above_threshold) as "Suppressed by Other Business Rules"
from
	(select
		hcptpa.date_in_et,
		hcptpa.ad_unit_id,
		sum(hcptpa.click_count) as click_count,
		sum(hcptpa.click_conversion_count) as click_conversion_count,
		sum(hcptpa.actual_cpc_sum) as actual_cpc_sum,
		sum(hcptpa.click_conversion_value_sum) as click_conversion_value_sum,
		sum(hcptpa.interaction_count) as interaction_count
	from intent_media_production.hotel_ct_placement_type_performance_aggregations hcptpa
	left join intent_media_production.ad_units au on au.id = hcptpa.ad_unit_id
	left join intent_media_production.sites s on s.id = au.site_id
	where date_in_et < date(current_timestamp at timezone 'America/New_York')
		and ((date_in_et > '2013-07-29' and s.name = 'TRIPADVISOR')
		or (date_in_et > '2013-10-27' and s.name = 'LOWFARES')
		or (date_in_et > '2013-10-28' and s.name = 'WEBJET')
		or (s.name not in ('TRIPADVISOR','LOWFARES','WEBJET') and date_in_et > '2012-03-26'))
		and (not (au.name like '%Firefox%' or au.name = 'Hotwire Media Fill In' or au.name like '%Trip.com%' or hcptpa.placement_type = 'EXIT_UNIT'))
	group by 
		hcptpa.date_in_et, 
		hcptpa.ad_unit_id) p
left join intent_media_production.ad_units au on p.ad_unit_id = au.id
left join intent_media_production.sites s on au.site_id = s.id
left join intent_media_production.entities e on e.id = s.publisher_id
join 
	(select 
		hcmpa.aggregation_level_date_in_et as date_in_et,
		hcmpa.ad_unit_id,
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
		0 as suppressed_by_c13n_above_threshold
	from intent_media_production.hotel_ct_media_performance_aggregations hcmpa
	left join intent_media_production.ad_units au on hcmpa.ad_unit_id = au.id		
	left join intent_media_production.sites s on s.id = au.site_id
	where aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York')
		and ((aggregation_level_date_in_et > '2013-07-29' and s.name = 'TRIPADVISOR')
		or (aggregation_level_date_in_et > '2013-10-27' and s.name = 'LOWFARES')
		or (aggregation_level_date_in_et > '2013-10-28' and s.name = 'WEBJET')
		or (s.name not in ('TRIPADVISOR','LOWFARES','WEBJET') and aggregation_level_date_in_et > '2012-03-26')) 		
	group by 
		hcmpa.aggregation_level_date_in_et, 
		hcmpa.ad_unit_id) as i
on i.date_in_et = p.date_in_et
and i.ad_unit_id = p.ad_unit_id