/* FLIGHTS Placement Level Breakdown */
select 
	'Flights' as "Product Category Type",
	i.date_in_et as Date,
	(case 
		when s.name = 'AIRFASTTICKETS' then 'AirFastTickets'
		when s.name = 'AIRTKT' then 'AirTkt'
		when s.name = 'BUDGETAIR' then 'Airtrade International'
		when s.name = 'VAYAMA' then 'Airtrade International'
		when s.name = 'AMOMA' then 'Amoma'
		when s.name = 'BOOKIT' then 'Bookit'
		when s.name = 'CHEAPAIR' then 'CheapAir'
		when s.name = 'EXPEDIA_IAB' then 'Expedia Inc. IAB'
		when s.name = 'EXPEDIA' then 'Expedia Inc.'
		when s.name = 'EXPEDIA_CA' then 'Expedia Inc.'
		when s.name = 'EXPEDIA_UK' then 'Expedia Inc.'
		when s.name = 'HOTWIRE' then 'Expedia Inc.'
		when s.name = 'HOTWIRE_UK' then 'Expedia Inc.'
		when s.name = 'HOTWIRE_MEDIA_FILL_IN' then 'Expedia Inc.'
		when s.name = 'TRAVELOCITY' then 'Expedia Inc.'
		when s.name = 'TVLY' then 'Expedia Inc.'
		when s.name = 'CHEAPOAIR' then 'Fareportal Inc.'
		when s.name = 'ONETRAVEL' then 'Fareportal Inc.'
		when s.name = 'HIPMUNK' then 'Hipmunk'
		when s.name = 'KAYAK' then 'Kayak'
		when s.name = 'KAYAK_UK' then 'Kayak'
		when s.name = 'LOWFARES' then 'Oversee'
		when s.name = 'FARESPOTTER' then 'Oversee'
		when s.name = 'CHEAPTICKETS' then 'OWW'
		when s.name = 'EBOOKERS' then 'OWW'
		when s.name = 'ORBITZ_CLASSIC' then 'OWW'
		when s.name = 'ORBITZ_GLOBAL' then 'OWW'
		when s.name = 'TRIPDOTCOM' then 'OWW'
		when s.name = 'LASTMINUTE_DOT_COM' then 'Sabre Holdings'
		when s.name = 'TRAVELZOO' then 'Travelzoo'
		when s.name = 'FLY_DOT_COM' then 'Travelzoo'
		when s.name = 'TRIPADVISOR' then 'TripAdvisor'
		when s.name = 'WEBJET' then 'WebJet'
		when s.name = 'WEGO' then 'Wego'
		when s.name = 'GOGOBOT' then 'Gogobot'
		when s.name = 'LOWCOSTAIRLINES' then 'LBF Travel'
		else 'Other'
	end) as Publisher,
	i.Country,
	ifnull(lpt.page_type, 'Other') as "Type of Ad Unit",
        s.display_name as Site,
	(case au.name
		when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit' 
		when 'CheapTickets Exit Unit - Firefox' then 'CheapTickets Exit Unit'
		else au.name 
	end) as "Ad Unit",
	e.publisher_tier as "Publisher Tier",
	p.Placement,
	p.is_prechecked as "Placement is Prechecked",
	i.click_count as "Clicks from Media",
	p.click_count as "Clicks from Placement",
	p.click_conversion_count as Conversions,
	p.actual_cpc_sum as "Gross Media Revenue from Placement",
	p.click_conversion_value_sum as "Conversion Value Sum",
	i.interaction_count as "Interactions from Media",
	p.interaction_count as "Interactions from Placement",
	i.ad_call_count as "Page Views",
	i.not_pure_ad_call_count as "Not Pure Page Views",
	i.not_pure_low_converting_ad_call_count as "Not Pure Low Value Page Views",
	i.not_pure_low_converting_addressable_ad_call_count as "Addressable Page Views",
	i.pure_ad_call_count as "Pure Page Views",
	i.pure_low_converting_ad_call_count as "Pure Low Value Page Views",
	i.low_converting_ad_call_count as "Low Value Page Views",
	i.not_pure_low_converting_ad_call_with_ads_count as "Fillable Pages",
	i.ad_unit_served_count as "Pages Served",
	i.impression_count as Impressions,
	i.available_impression_count as "Available Impressions",
	i.gross_revenue_sum as "Gross Media Revenue from Media",
	i.net_revenue_sum as "Net Media Revenue",
	i.suppressed_by_route as "Suppressed - Route",
	i.suppressed_by_unknown_hotel_city as "Suppressed - Unknown Hotel City",
	i.suppressed_by_c13n_segment as "Suppressed - Cannibalization Segment",
	i.suppressed_by_click_blackout as "Suppressed - Click Blackout",
	i.suppressed_by_no_valid_layout as "Suppressed - No Valid Layout",
	i.suppressed_by_publisher_traffic_share as "Suppressed - Publisher Traffic Share",
	i.suppressed_by_c13n_above_threshold as "Suppressed - Cannibalization Threshold",
	i.not_pure_low_converting_intentmedia_traffic_share_ad_call_count as "Not Pure Low Value Intent Media Traffic Page Views",
	i.number_of_prechecks_served as "Number of Prechecks Served",
	i.requested_number_of_prechecks as "Requested Number of Prechecks"
from
---------air SCA Media Performance
  (
    select
      aggregation_level_date_in_et as date_in_et,
      (case
        when au.name like '%UK%' then 'UK'
        when au.name like '%.ca%' then 'CA'
        else 'US'
      end) as Country,
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
      sum(suppressed_by_c13n_above_threshold) as suppressed_by_c13n_above_threshold,
      sum(not_pure_low_converting_intentmedia_traffic_share_ad_call_count) as not_pure_low_converting_intentmedia_traffic_share_ad_call_count,
      sum(number_of_prechecks_served) as number_of_prechecks_served,
      sum(requested_number_of_prechecks) as requested_number_of_prechecks
    from intent_media_production.air_ct_media_performance_aggregations acmpa
    left join intent_media_production.ad_units au on au.id = acmpa.ad_unit_id
    left join intent_media_production.sites s on s.id = au.site_id
    where aggregation_level_date_in_et >= au.reporting_start_date
      and aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York')
    group by
      aggregation_level_date_in_et,
      (case
        when au.name like '%UK%' then 'UK'
        when au.name like '%.ca%' then 'CA'
        else 'US'
      end),
      ad_unit_id
  ) i
LEFT join
------ air SCA Placement Performance
  (
    select
      acptpa.date_in_et,
      (case
        when au.name like '%UK%' then 'UK'
        when au.name like '%.ca%' then 'CA'
        else 'US'
      end) as Country,
      acptpa.ad_unit_id,
      acptpa.placement_type as Placement,
      (Case is_prechecked
      when TRUE then 'Prechecked'
      when FALSE then 'Not Prechecked'
      else 'ERROR' end) as is_prechecked,
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
    group by
      acptpa.date_in_et,
      (case
        when au.name like '%UK%' then 'UK'
        when au.name like '%.ca%' then 'CA'
        else 'US'
      end),
      acptpa.ad_unit_id,
      acptpa.placement_type,
      (Case is_prechecked
      when TRUE then 'Prechecked'
      when FALSE then 'Not Prechecked'
      else 'ERROR' end)
	) p
on i.date_in_et = p.date_in_et and i.Country = p.Country and i.ad_unit_id = p.ad_unit_id
left join intent_media_production.ad_units au on i.ad_unit_id = au.id
left join intent_media_production.sites s on au.site_id = s.id
left join intent_media_production.entities e on e.id = s.publisher_id
left join intent_media_production.legal_page_types lpt on lpt.id = au.legal_page_type_id

union

/* FLIGHTS Total */
select
	'Flights' as "Product Category Type",
	i.date_in_et as Date,
	(case
		when s.name = 'AIRFASTTICKETS' then 'AirFastTickets'
		when s.name = 'AIRTKT' then 'AirTkt'
		when s.name = 'BUDGETAIR' then 'Airtrade International'
		when s.name = 'VAYAMA' then 'Airtrade International'
		when s.name = 'AMOMA' then 'Amoma'
		when s.name = 'BOOKIT' then 'Bookit'
		when s.name = 'CHEAPAIR' then 'CheapAir'
		when s.name = 'EXPEDIA_IAB' then 'Expedia Inc. IAB'
		when s.name = 'EXPEDIA' then 'Expedia Inc.'
		when s.name = 'EXPEDIA_CA' then 'Expedia Inc.'
		when s.name = 'EXPEDIA_UK' then 'Expedia Inc.'
		when s.name = 'HOTWIRE' then 'Expedia Inc.'
		when s.name = 'HOTWIRE_UK' then 'Expedia Inc.'
		when s.name = 'HOTWIRE_MEDIA_FILL_IN' then 'Expedia Inc.'
		when s.name = 'TRAVELOCITY' then 'Expedia Inc.'
		when s.name = 'TVLY' then 'Expedia Inc.'
		when s.name = 'CHEAPOAIR' then 'Fareportal Inc.'
		when s.name = 'ONETRAVEL' then 'Fareportal Inc.'
		when s.name = 'HIPMUNK' then 'Hipmunk'
		when s.name = 'KAYAK' then 'Kayak'
		when s.name = 'KAYAK_UK' then 'Kayak'
		when s.name = 'LOWFARES' then 'Oversee'
		when s.name = 'FARESPOTTER' then 'Oversee'
		when s.name = 'CHEAPTICKETS' then 'OWW'
		when s.name = 'EBOOKERS' then 'OWW'
		when s.name = 'ORBITZ_CLASSIC' then 'OWW'
		when s.name = 'ORBITZ_GLOBAL' then 'OWW'
		when s.name = 'TRIPDOTCOM' then 'OWW'
		when s.name = 'LASTMINUTE_DOT_COM' then 'Sabre Holdings'
		when s.name = 'TRAVELZOO' then 'Travelzoo'
		when s.name = 'FLY_DOT_COM' then 'Travelzoo'
		when s.name = 'TRIPADVISOR' then 'TripAdvisor'
		when s.name = 'WEBJET' then 'WebJet'
		when s.name = 'WEGO' then 'Wego'
		when s.name = 'GOGOBOT' then 'Gogobot'
		when s.name = 'LOWCOSTAIRLINES' then 'LBF Travel'
		else 'Other'
	end) as Publisher,
	i.Country,
	ifnull(lpt.page_type, 'Other') as "Type of Ad Unit",
	s.display_name as Site,
	(case au.name
		when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit'
		when 'CheapTickets Exit Unit - Firefox' then 'CheapTickets Exit Unit'
		else au.name
	end) as "Ad Unit",
	e.publisher_tier as "Publisher Tier",
	'Total' as Placement,
	'Total Placement' as "Placement is Prechecked",
	i.click_count as "Clicks from Media",
	p.click_count as "Clicks from Placement",
	p.click_conversion_count as Conversions,
	p.actual_cpc_sum as "Gross Media Revenue from Placement",
	p.click_conversion_value_sum as "Conversion Value Sum",
	i.interaction_count as "Interactions from Media",
	p.interaction_count as "Interactions from Placement",
	i.ad_call_count as "Page Views",
	i.not_pure_ad_call_count as "Not Pure Page Views",
	i.not_pure_low_converting_ad_call_count as "Not Pure Low Value Page Views",
	i.not_pure_low_converting_addressable_ad_call_count as "Addressable Page Views",
	i.pure_ad_call_count as "Pure Page Views",
	i.pure_low_converting_ad_call_count as "Pure Low Value Page Views",
	i.low_converting_ad_call_count as "Low Value Page Views",
	i.not_pure_low_converting_ad_call_with_ads_count as "Fillable Pages",
	i.ad_unit_served_count as "Pages Served",
	i.impression_count as Impressions,
	i.available_impression_count as "Available Impressions",
	i.gross_revenue_sum as "Gross Media Revenue from Media",
	i.net_revenue_sum as "Net Media Revenue",
	i.suppressed_by_route as "Suppressed - Route",
	i.suppressed_by_unknown_hotel_city as "Suppressed - Unknown Hotel City",
	i.suppressed_by_c13n_segment as "Suppressed - Cannibalization Segment",
	i.suppressed_by_click_blackout as "Suppressed - Click Blackout",
	i.suppressed_by_no_valid_layout as "Suppressed - No Valid Layout",
	i.suppressed_by_publisher_traffic_share as "Suppressed - Publisher Traffic Share",
	i.suppressed_by_c13n_above_threshold as "Suppressed - Cannibalization Threshold",
	i.not_pure_low_converting_intentmedia_traffic_share_ad_call_count as "Not Pure Low Value Intent Media Traffic Page Views",
	i.number_of_prechecks_served as "Number of Prechecks Served",
	i.requested_number_of_prechecks as "Requested Number of Prechecks"
from
---------air SCA Media Performance
	(
    select
      aggregation_level_date_in_et as date_in_et,
      (case
        when au.name like '%UK%' then 'UK'
        when au.name like '%.ca%' then 'CA'
        else 'US'
      end) as Country,
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
      sum(suppressed_by_c13n_above_threshold) as suppressed_by_c13n_above_threshold,
      sum(not_pure_low_converting_intentmedia_traffic_share_ad_call_count) as not_pure_low_converting_intentmedia_traffic_share_ad_call_count,
      sum(number_of_prechecks_served) as number_of_prechecks_served,
      sum(requested_number_of_prechecks) as requested_number_of_prechecks
    from intent_media_production.air_ct_media_performance_aggregations acmpa
    left join intent_media_production.ad_units au on au.id = acmpa.ad_unit_id
    where aggregation_level_date_in_et >= au.reporting_start_date
      and aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York')
    group by
      aggregation_level_date_in_et,
      (case
        when au.name like '%UK%' then 'UK'
        when au.name like '%.ca%' then 'CA'
        else 'US'
      end),
      ad_unit_id
  ) i
LEFT join
------ air SCA Placement Performance
	(
    select
      acptpa.date_in_et,
      (case
        when au.name like '%UK%' then 'UK'
        when au.name like '%.ca%' then 'CA'
        else 'US'
      end) as Country,
      acptpa.ad_unit_id,
      sum(acptpa.click_count) as click_count,
      sum(acptpa.click_conversion_count) as click_conversion_count,
      sum(acptpa.actual_cpc_sum) as actual_cpc_sum,
      sum(acptpa.click_conversion_value_sum) as click_conversion_value_sum,
      sum(acptpa.interaction_count) as interaction_count
    from intent_media_production.air_ct_placement_type_performance_aggregations acptpa
    left join intent_media_production.ad_units au on au.id = acptpa.ad_unit_id
    where date_in_et >= au.reporting_start_date
      and date_in_et < date(current_timestamp at timezone 'America/New_York')
    group by
      acptpa.date_in_et,
      (case
        when au.name like '%UK%' then 'UK'
        when au.name like '%.ca%' then 'CA'
        else 'US'
      end),
      acptpa.ad_unit_id
	) p
on i.date_in_et = p.date_in_et and i.Country = p.Country and i.ad_unit_id = p.ad_unit_id
left join intent_media_production.ad_units au on i.ad_unit_id = au.id
left join intent_media_production.sites s on au.site_id = s.id
left join intent_media_production.entities e on e.id = s.publisher_id
left join intent_media_production.legal_page_types lpt on lpt.id = au.legal_page_type_id

union

/* FLIGHTS Placement Total */
select
	'Flights' as "Product Category Type",
	i.date_in_et as Date,
	(case
		when s.name = 'AIRFASTTICKETS' then 'AirFastTickets'
		when s.name = 'AIRTKT' then 'AirTkt'
		when s.name = 'BUDGETAIR' then 'Airtrade International'
		when s.name = 'VAYAMA' then 'Airtrade International'
		when s.name = 'AMOMA' then 'Amoma'
		when s.name = 'BOOKIT' then 'Bookit'
		when s.name = 'CHEAPAIR' then 'CheapAir'
		when s.name = 'EXPEDIA_IAB' then 'Expedia Inc. IAB'
		when s.name = 'EXPEDIA' then 'Expedia Inc.'
		when s.name = 'EXPEDIA_CA' then 'Expedia Inc.'
		when s.name = 'EXPEDIA_UK' then 'Expedia Inc.'
		when s.name = 'HOTWIRE' then 'Expedia Inc.'
		when s.name = 'HOTWIRE_UK' then 'Expedia Inc.'
		when s.name = 'HOTWIRE_MEDIA_FILL_IN' then 'Expedia Inc.'
		when s.name = 'TRAVELOCITY' then 'Expedia Inc.'
		when s.name = 'TVLY' then 'Expedia Inc.'
		when s.name = 'CHEAPOAIR' then 'Fareportal Inc.'
		when s.name = 'ONETRAVEL' then 'Fareportal Inc.'
		when s.name = 'HIPMUNK' then 'Hipmunk'
		when s.name = 'KAYAK' then 'Kayak'
		when s.name = 'KAYAK_UK' then 'Kayak'
		when s.name = 'LOWFARES' then 'Oversee'
		when s.name = 'FARESPOTTER' then 'Oversee'
		when s.name = 'CHEAPTICKETS' then 'OWW'
		when s.name = 'EBOOKERS' then 'OWW'
		when s.name = 'ORBITZ_CLASSIC' then 'OWW'
		when s.name = 'ORBITZ_GLOBAL' then 'OWW'
		when s.name = 'TRIPDOTCOM' then 'OWW'
		when s.name = 'LASTMINUTE_DOT_COM' then 'Sabre Holdings'
		when s.name = 'TRAVELZOO' then 'Travelzoo'
		when s.name = 'FLY_DOT_COM' then 'Travelzoo'
		when s.name = 'TRIPADVISOR' then 'TripAdvisor'
		when s.name = 'WEBJET' then 'WebJet'
		when s.name = 'WEGO' then 'Wego'
		when s.name = 'GOGOBOT' then 'Gogobot'
		when s.name = 'LOWCOSTAIRLINES' then 'LBF Travel'
		else 'Other'
	end) as Publisher,
	i.Country,
	ifnull(lpt.page_type, 'Other') as "Type of Ad Unit",
	s.display_name as Site,
	(case au.name
		when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit'
		when 'CheapTickets Exit Unit - Firefox' then 'CheapTickets Exit Unit'
		else au.name
	end) as "Ad Unit",
	e.publisher_tier as "Publisher Tier",
	p.Placement,
	'Total Placement' as "Placement is Prechecked",
	i.click_count as "Clicks from Media",
	p.click_count as "Clicks from Placement",
	p.click_conversion_count as Conversions,
	p.actual_cpc_sum as "Gross Media Revenue from Placement",
	p.click_conversion_value_sum as "Conversion Value Sum",
	i.interaction_count as "Interactions from Media",
	p.interaction_count as "Interactions from Placement",
	i.ad_call_count as "Page Views",
	i.not_pure_ad_call_count as "Not Pure Page Views",
	i.not_pure_low_converting_ad_call_count as "Not Pure Low Value Page Views",
	i.not_pure_low_converting_addressable_ad_call_count as "Addressable Page Views",
	i.pure_ad_call_count as "Pure Page Views",
	i.pure_low_converting_ad_call_count as "Pure Low Value Page Views",
	i.low_converting_ad_call_count as "Low Value Page Views",
	i.not_pure_low_converting_ad_call_with_ads_count as "Fillable Pages",
	i.ad_unit_served_count as "Pages Served",
	i.impression_count as Impressions,
	i.available_impression_count as "Available Impressions",
	i.gross_revenue_sum as "Gross Media Revenue from Media",
	i.net_revenue_sum as "Net Media Revenue",
	i.suppressed_by_route as "Suppressed - Route",
	i.suppressed_by_unknown_hotel_city as "Suppressed - Unknown Hotel City",
	i.suppressed_by_c13n_segment as "Suppressed - Cannibalization Segment",
	i.suppressed_by_click_blackout as "Suppressed - Click Blackout",
	i.suppressed_by_no_valid_layout as "Suppressed - No Valid Layout",
	i.suppressed_by_publisher_traffic_share as "Suppressed - Publisher Traffic Share",
	i.suppressed_by_c13n_above_threshold as "Suppressed - Cannibalization Threshold",
	i.not_pure_low_converting_intentmedia_traffic_share_ad_call_count as "Not Pure Low Value Intent Media Traffic Page Views",
	i.number_of_prechecks_served as "Number of Prechecks Served",
	i.requested_number_of_prechecks as "Requested Number of Prechecks"
from
---------air SCA Media Performance
	(
    select
      aggregation_level_date_in_et as date_in_et,
      (case
        when au.name like '%UK%' then 'UK'
        when au.name like '%.ca%' then 'CA'
        else 'US'
      end) as Country,
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
      sum(suppressed_by_c13n_above_threshold) as suppressed_by_c13n_above_threshold,
      sum(not_pure_low_converting_intentmedia_traffic_share_ad_call_count) as not_pure_low_converting_intentmedia_traffic_share_ad_call_count,
      sum(number_of_prechecks_served) as number_of_prechecks_served,
      sum(requested_number_of_prechecks) as requested_number_of_prechecks
    from intent_media_production.air_ct_media_performance_aggregations acmpa
    left join intent_media_production.ad_units au on au.id = acmpa.ad_unit_id
    where aggregation_level_date_in_et >= au.reporting_start_date
      and aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York')
    group by
      aggregation_level_date_in_et,
      (case
        when au.name like '%UK%' then 'UK'
        when au.name like '%.ca%' then 'CA'
        else 'US'
      end),
      ad_unit_id
  ) i
LEFT join
------ air SCA Placement Performance
	(
    select
      acptpa.date_in_et,
      placement_Type as Placement,
      (case
        when au.name like '%UK%' then 'UK'
        when au.name like '%.ca%' then 'CA'
        else 'US'
      end) as Country,
      acptpa.ad_unit_id,
      sum(acptpa.click_count) as click_count,
      sum(acptpa.click_conversion_count) as click_conversion_count,
      sum(acptpa.actual_cpc_sum) as actual_cpc_sum,
      sum(acptpa.click_conversion_value_sum) as click_conversion_value_sum,
      sum(acptpa.interaction_count) as interaction_count
    from intent_media_production.air_ct_placement_type_performance_aggregations acptpa
    left join intent_media_production.ad_units au on au.id = acptpa.ad_unit_id
    where date_in_et >= au.reporting_start_date
      and date_in_et < date(current_timestamp at timezone 'America/New_York')
    group by
      acptpa.date_in_et,
      placement_Type,
      (case
        when au.name like '%UK%' then 'UK'
        when au.name like '%.ca%' then 'CA'
        else 'US'
      end),
      acptpa.ad_unit_id
	) p
on i.date_in_et = p.date_in_et and i.Country = p.Country and i.ad_unit_id = p.ad_unit_id
left join intent_media_production.ad_units au on i.ad_unit_id = au.id
left join intent_media_production.sites s on au.site_id = s.id
left join intent_media_production.entities e on e.id = s.publisher_id
left join intent_media_production.legal_page_types lpt on lpt.id = au.legal_page_type_id


Union

---------------------------------------------------------------------------------------HOTELS----------------------------------------------------------------------
/* HOTELS Placement Level Breakdown */
select 
	'Hotels' as "Product Category Type",
	i.date_in_et as Date,
	(case 
		when s.name = 'AIRFASTTICKETS' then 'AirFastTickets'
		when s.name = 'AIRTKT' then 'AirTkt'
		when s.name = 'BUDGETAIR' then 'Airtrade International'
		when s.name = 'VAYAMA' then 'Airtrade International'
		when s.name = 'AMOMA' then 'Amoma'
		when s.name = 'BOOKIT' then 'Bookit'
		when s.name = 'CHEAPAIR' then 'CheapAir'
		when s.name = 'EXPEDIA_IAB' then 'Expedia Inc. IAB'
		when s.name = 'EXPEDIA' then 'Expedia Inc.'
		when s.name = 'EXPEDIA_CA' then 'Expedia Inc.'
		when s.name = 'EXPEDIA_UK' then 'Expedia Inc.'
		when s.name = 'HOTWIRE' then 'Expedia Inc.'
		when s.name = 'HOTWIRE_UK' then 'Expedia Inc.'
		when s.name = 'HOTWIRE_MEDIA_FILL_IN' then 'Expedia Inc.'
		when s.name = 'TRAVELOCITY' then 'Expedia Inc.'
		when s.name = 'TVLY' then 'Expedia Inc.'
		when s.name = 'CHEAPOAIR' then 'Fareportal Inc.'
		when s.name = 'ONETRAVEL' then 'Fareportal Inc.'
		when s.name = 'HIPMUNK' then 'Hipmunk'
		when s.name = 'KAYAK' then 'Kayak'
		when s.name = 'KAYAK_UK' then 'Kayak'
		when s.name = 'LOWFARES' then 'Oversee'
		when s.name = 'FARESPOTTER' then 'Oversee'
		when s.name = 'CHEAPTICKETS' then 'OWW'
		when s.name = 'EBOOKERS' then 'OWW'
		when s.name = 'ORBITZ_CLASSIC' then 'OWW'
		when s.name = 'ORBITZ_GLOBAL' then 'OWW'
		when s.name = 'TRIPDOTCOM' then 'OWW'
		when s.name = 'LASTMINUTE_DOT_COM' then 'Sabre Holdings'
		when s.name = 'TRAVELZOO' then 'Travelzoo'
		when s.name = 'FLY_DOT_COM' then 'Travelzoo'
		when s.name = 'TRIPADVISOR' then 'TripAdvisor'
		when s.name = 'WEBJET' then 'WebJet'
		when s.name = 'WEGO' then 'Wego'
		when s.name = 'GOGOBOT' then 'Gogobot'
		when s.name = 'LOWCOSTAIRLINES' then 'LBF Travel'
		else 'Other'
	end) as Publisher,
	i.Country,
	ifnull(lpt.page_type, 'Other') as "Type of Ad Unit",
	s.display_name as Site,
	(CASE au.name
		WHEN 'Orbitz Exit Unit - Firefox' THEN 'Orbitz Exit Unit' 
		WHEN 'CheapTickets Exit Unit - Firefox' THEN 'CheapTickets Exit Unit'
		ELSE au.name 
	END) as "Ad Unit",
	e.publisher_tier as "Publisher Tier",
	p.Placement,
	p.is_prechecked as "Placement is Prechecked",
	i.click_count as "Clicks from Media",
	p.click_count as "Clicks from Placement",
	p.click_conversion_count as Conversions,
	p.actual_cpc_sum as "Gross Media Revenue from Placement",
	p.click_conversion_value_sum as "Conversion Value Sum",
	i.interaction_count as "Interactions from Media",
	p.interaction_count as "Interactions from Placement",
	i.ad_call_count as "Page Views",
	i.not_pure_ad_call_count as "Not Pure Page Views",
	i.not_pure_low_converting_ad_call_count as "Not Pure Low Value Page Views",
	i.not_pure_low_converting_addressable_ad_call_count as "Addressable Page Views",
	i.pure_ad_call_count as "Pure Page Views",
	i.pure_low_converting_ad_call_count as "Pure Low Value Page Views",
	i.low_converting_ad_call_count as "Low Value Page Views",
	i.not_pure_low_converting_ad_call_with_ads_count as "Fillable Pages",
	i.ad_unit_served_count as "Pages Served",
	i.impression_count as Impressions,
	i.available_impression_count as "Available Impressions",
	i.gross_revenue_sum as "Gross Media Revenue from Media",
	i.net_revenue_sum as "Net Media Revenue",
	i.suppressed_by_route as "Suppressed - Route",
	i.suppressed_by_unknown_hotel_city as "Suppressed - Unknown Hotel City",
	i.suppressed_by_c13n_segment as "Suppressed - Cannibalization Segment",
	i.suppressed_by_click_blackout as "Suppressed - Click Blackout",
	i.suppressed_by_no_valid_layout as "Suppressed - No Valid Layout",
	i.suppressed_by_publisher_traffic_share as "Suppressed - Publisher Traffic Share",
	i.suppressed_by_c13n_above_threshold as "Suppressed - Cannibalization Threshold",
	i.not_pure_low_converting_intentmedia_traffic_share_ad_call_count as "Not Pure Low Value Intent Media Traffic Page Views",
	i.number_of_prechecks_served as "Number of Prechecks Served",
	i.requested_number_of_prechecks as "Requested Number of Prechecks"
from 
---------Hotel SCA Media Performance
	(
    select
      aggregation_level_date_in_et as date_in_et,
      (case
        when au.name like '%UK%' then 'UK'
        when au.name like '%.ca%' then 'CA'
        else 'US'
      end) as Country,
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
      sum(not_pure_low_converting_intentmedia_traffic_share_ad_call_count) as not_pure_low_converting_intentmedia_traffic_share_ad_call_count,
      sum(number_of_prechecks_served) as number_of_prechecks_served,
      sum(requested_number_of_prechecks) as requested_number_of_prechecks
    from intent_media_production.hotel_ct_media_performance_aggregations hcmpa
    left join intent_media_production.ad_units au on au.id = hcmpa.ad_unit_id
    where aggregation_level_date_in_et >= au.reporting_start_date
      and aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York')
    group by
      aggregation_level_date_in_et,
      (case
        when au.name like '%UK%' then 'UK'
        when au.name like '%.ca%' then 'CA'
        else 'US'
      end),
      ad_unit_id
  ) i
LEFT join
/* HOTELS Placement Level Breakdown */
	(
    select
      hcptpa.date_in_et,
      (case
        when au.name like '%UK%' then 'UK'
        when au.name like '%.ca%' then 'CA'
        else 'US'
      end) as Country,
      hcptpa.ad_unit_id,
      hcptpa.placement_type as Placement,
      (Case is_prechecked
      when TRUE then 'Prechecked'
      when FALSE then 'Not Prechecked'
      else 'ERROR' end) as is_prechecked,
      sum(hcptpa.click_count) as click_count,
      sum(hcptpa.click_conversion_count) as click_conversion_count,
      sum(hcptpa.actual_cpc_sum) as actual_cpc_sum,
      sum(hcptpa.click_conversion_value_sum) as click_conversion_value_sum,
      sum(hcptpa.interaction_count) as interaction_count
    from intent_media_production.hotel_ct_placement_type_performance_aggregations hcptpa
    left join intent_media_production.ad_units au on au.id = hcptpa.ad_unit_id
    where date_in_et >= au.reporting_start_date
      and date_in_et < date(current_timestamp at timezone 'America/New_York')
    group by
      hcptpa.date_in_et,
      (case
        when au.name like '%UK%' then 'UK'
        when au.name like '%.ca%' then 'CA'
        else 'US'
      end),
      hcptpa.ad_unit_id,
      hcptpa.placement_type,
      (Case is_prechecked
      when TRUE then 'Prechecked'
      when FALSE then 'Not Prechecked'
      else 'ERROR' end) 
  ) p
on i.date_in_et = p.date_in_et and i.Country = p.Country and i.ad_unit_id = p.ad_unit_id
left join intent_media_production.ad_units au on i.ad_unit_id = au.id
left join intent_media_production.sites s on au.site_id = s.id
left join intent_media_production.entities e on e.id = s.publisher_id
left join intent_media_production.legal_page_types lpt on lpt.id = au.legal_page_type_id

union

/* HOTELS Total */
select
	'Hotels' as "Product Category Type",
	i.date_in_et as Date,
	(case
		when s.name = 'AIRFASTTICKETS' then 'AirFastTickets'
		when s.name = 'AIRTKT' then 'AirTkt'
		when s.name = 'BUDGETAIR' then 'Airtrade International'
		when s.name = 'VAYAMA' then 'Airtrade International'
		when s.name = 'AMOMA' then 'Amoma'
		when s.name = 'BOOKIT' then 'Bookit'
		when s.name = 'CHEAPAIR' then 'CheapAir'
		when s.name = 'EXPEDIA_IAB' then 'Expedia Inc. IAB'
		when s.name = 'EXPEDIA' then 'Expedia Inc.'
		when s.name = 'EXPEDIA_CA' then 'Expedia Inc.'
		when s.name = 'EXPEDIA_UK' then 'Expedia Inc.'
		when s.name = 'HOTWIRE' then 'Expedia Inc.'
		when s.name = 'HOTWIRE_UK' then 'Expedia Inc.'
		when s.name = 'HOTWIRE_MEDIA_FILL_IN' then 'Expedia Inc.'
		when s.name = 'TRAVELOCITY' then 'Expedia Inc.'
		when s.name = 'TVLY' then 'Expedia Inc.'
		when s.name = 'CHEAPOAIR' then 'Fareportal Inc.'
		when s.name = 'ONETRAVEL' then 'Fareportal Inc.'
		when s.name = 'HIPMUNK' then 'Hipmunk'
		when s.name = 'KAYAK' then 'Kayak'
		when s.name = 'KAYAK_UK' then 'Kayak'
		when s.name = 'LOWFARES' then 'Oversee'
		when s.name = 'FARESPOTTER' then 'Oversee'
		when s.name = 'CHEAPTICKETS' then 'OWW'
		when s.name = 'EBOOKERS' then 'OWW'
		when s.name = 'ORBITZ_CLASSIC' then 'OWW'
		when s.name = 'ORBITZ_GLOBAL' then 'OWW'
		when s.name = 'TRIPDOTCOM' then 'OWW'
		when s.name = 'LASTMINUTE_DOT_COM' then 'Sabre Holdings'
		when s.name = 'TRAVELZOO' then 'Travelzoo'
		when s.name = 'FLY_DOT_COM' then 'Travelzoo'
		when s.name = 'TRIPADVISOR' then 'TripAdvisor'
		when s.name = 'WEBJET' then 'WebJet'
		when s.name = 'WEGO' then 'Wego'
		when s.name = 'GOGOBOT' then 'Gogobot'
		when s.name = 'LOWCOSTAIRLINES' then 'LBF Travel'
		else 'Other'
	end) as Publisher,
	i.Country,
	ifnull(lpt.page_type, 'Other') as "Type of Ad Unit",
	s.display_name as Site,
	(CASE au.name
		WHEN 'Orbitz Exit Unit - Firefox' THEN 'Orbitz Exit Unit'
		WHEN 'CheapTickets Exit Unit - Firefox' THEN 'CheapTickets Exit Unit'
		ELSE au.name
	END) as "Ad Unit",
	e.publisher_tier as "Publisher Tier",
	'Total' as Placement,
	'Total Placement' as "Placement is Prechecked",
	i.click_count as "Clicks from Media",
	p.click_count as "Clicks from Placement",
	p.click_conversion_count as Conversions,
	p.actual_cpc_sum as "Gross Media Revenue from Placement",
	p.click_conversion_value_sum as "Conversion Value Sum",
	i.interaction_count as "Interactions from Media",
	p.interaction_count as "Interactions from Placement",
	i.ad_call_count as "Page Views",
	i.not_pure_ad_call_count as "Not Pure Page Views",
	i.not_pure_low_converting_ad_call_count as "Not Pure Low Value Page Views",
	i.not_pure_low_converting_addressable_ad_call_count as "Addressable Page Views",
	i.pure_ad_call_count as "Pure Page Views",
	i.pure_low_converting_ad_call_count as "Pure Low Value Page Views",
	i.low_converting_ad_call_count as "Low Value Page Views",
	i.not_pure_low_converting_ad_call_with_ads_count as "Fillable Pages",
	i.ad_unit_served_count as "Pages Served",
	i.impression_count as Impressions,
	i.available_impression_count as "Available Impressions",
	i.gross_revenue_sum as "Gross Media Revenue from Media",
	i.net_revenue_sum as "Net Media Revenue",
	i.suppressed_by_route as "Suppressed - Route",
	i.suppressed_by_unknown_hotel_city as "Suppressed - Unknown Hotel City",
	i.suppressed_by_c13n_segment as "Suppressed - Cannibalization Segment",
	i.suppressed_by_click_blackout as "Suppressed - Click Blackout",
	i.suppressed_by_no_valid_layout as "Suppressed - No Valid Layout",
	i.suppressed_by_publisher_traffic_share as "Suppressed - Publisher Traffic Share",
	i.suppressed_by_c13n_above_threshold as "Suppressed - Cannibalization Threshold",
	i.not_pure_low_converting_intentmedia_traffic_share_ad_call_count as "Not Pure Low Value Intent Media Traffic Page Views",
	i.number_of_prechecks_served as "Number of Prechecks Served",
	i.requested_number_of_prechecks as "Requested Number of Prechecks"
from
---------Hotel SCA Media Performance
	(
    select
      aggregation_level_date_in_et as date_in_et,
      (case
        when au.name like '%UK%' then 'UK'
        when au.name like '%.ca%' then 'CA'
        else 'US'
      end) as Country,
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
      sum(not_pure_low_converting_intentmedia_traffic_share_ad_call_count) as not_pure_low_converting_intentmedia_traffic_share_ad_call_count,
      sum(number_of_prechecks_served) as number_of_prechecks_served,
      sum(requested_number_of_prechecks) as requested_number_of_prechecks
    from intent_media_production.hotel_ct_media_performance_aggregations hcmpa
    left join intent_media_production.ad_units au on au.id = hcmpa.ad_unit_id
    where aggregation_level_date_in_et >= au.reporting_start_date
      and aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York')
    group by
      aggregation_level_date_in_et,
      (case
        when au.name like '%UK%' then 'UK'
        when au.name like '%.ca%' then 'CA'
        else 'US'
      end),
      ad_unit_id
  ) i
LEFT join
/* HOTELS Placement Level Breakdown */
	(
    select
      hcptpa.date_in_et,
      (case
        when au.name like '%UK%' then 'UK'
        when au.name like '%.ca%' then 'CA'
        else 'US'
      end) as Country,
      hcptpa.ad_unit_id,
      sum(hcptpa.click_count) as click_count,
      sum(hcptpa.click_conversion_count) as click_conversion_count,
      sum(hcptpa.actual_cpc_sum) as actual_cpc_sum,
      sum(hcptpa.click_conversion_value_sum) as click_conversion_value_sum,
      sum(hcptpa.interaction_count) as interaction_count
    from intent_media_production.hotel_ct_placement_type_performance_aggregations hcptpa
    left join intent_media_production.ad_units au on au.id = hcptpa.ad_unit_id
    where date_in_et >= au.reporting_start_date
      and date_in_et < date(current_timestamp at timezone 'America/New_York')
    group by
      hcptpa.date_in_et,
      (case
        when au.name like '%UK%' then 'UK'
        when au.name like '%.ca%' then 'CA'
        else 'US'
      end),
      hcptpa.ad_unit_id
  ) p
on i.date_in_et = p.date_in_et and i.Country = p.Country and i.ad_unit_id = p.ad_unit_id
left join intent_media_production.ad_units au on i.ad_unit_id = au.id
left join intent_media_production.sites s on au.site_id = s.id
left join intent_media_production.entities e on e.id = s.publisher_id
left join intent_media_production.legal_page_types lpt on lpt.id = au.legal_page_type_id

Union



/* HOTELS Placement Total */
select
	'Hotels' as "Product Category Type",
	i.date_in_et as Date,
	(case
		when s.name = 'AIRFASTTICKETS' then 'AirFastTickets'
		when s.name = 'AIRTKT' then 'AirTkt'
		when s.name = 'BUDGETAIR' then 'Airtrade International'
		when s.name = 'VAYAMA' then 'Airtrade International'
		when s.name = 'AMOMA' then 'Amoma'
		when s.name = 'BOOKIT' then 'Bookit'
		when s.name = 'CHEAPAIR' then 'CheapAir'
		when s.name = 'EXPEDIA_IAB' then 'Expedia Inc. IAB'
		when s.name = 'EXPEDIA' then 'Expedia Inc.'
		when s.name = 'EXPEDIA_CA' then 'Expedia Inc.'
		when s.name = 'EXPEDIA_UK' then 'Expedia Inc.'
		when s.name = 'HOTWIRE' then 'Expedia Inc.'
		when s.name = 'HOTWIRE_UK' then 'Expedia Inc.'
		when s.name = 'HOTWIRE_MEDIA_FILL_IN' then 'Expedia Inc.'
		when s.name = 'TRAVELOCITY' then 'Expedia Inc.'
		when s.name = 'TVLY' then 'Expedia Inc.'
		when s.name = 'CHEAPOAIR' then 'Fareportal Inc.'
		when s.name = 'ONETRAVEL' then 'Fareportal Inc.'
		when s.name = 'HIPMUNK' then 'Hipmunk'
		when s.name = 'KAYAK' then 'Kayak'
		when s.name = 'KAYAK_UK' then 'Kayak'
		when s.name = 'LOWFARES' then 'Oversee'
		when s.name = 'FARESPOTTER' then 'Oversee'
		when s.name = 'CHEAPTICKETS' then 'OWW'
		when s.name = 'EBOOKERS' then 'OWW'
		when s.name = 'ORBITZ_CLASSIC' then 'OWW'
		when s.name = 'ORBITZ_GLOBAL' then 'OWW'
		when s.name = 'TRIPDOTCOM' then 'OWW'
		when s.name = 'LASTMINUTE_DOT_COM' then 'Sabre Holdings'
		when s.name = 'TRAVELZOO' then 'Travelzoo'
		when s.name = 'FLY_DOT_COM' then 'Travelzoo'
		when s.name = 'TRIPADVISOR' then 'TripAdvisor'
		when s.name = 'WEBJET' then 'WebJet'
		when s.name = 'WEGO' then 'Wego'
		when s.name = 'GOGOBOT' then 'Gogobot'
		when s.name = 'LOWCOSTAIRLINES' then 'LBF Travel'
		else 'Other'
	end) as Publisher,
	i.Country,
	ifnull(lpt.page_type, 'Other') as "Type of Ad Unit",
	s.display_name as Site,
	(CASE au.name
		WHEN 'Orbitz Exit Unit - Firefox' THEN 'Orbitz Exit Unit'
		WHEN 'CheapTickets Exit Unit - Firefox' THEN 'CheapTickets Exit Unit'
		ELSE au.name
	END) as "Ad Unit",
	e.publisher_tier as "Publisher Tier",
	p.placement as Placement,
	'Total Placement' as "Placement is Prechecked",
	i.click_count as "Clicks from Media",
	p.click_count as "Clicks from Placement",
	p.click_conversion_count as Conversions,
	p.actual_cpc_sum as "Gross Media Revenue from Placement",
	p.click_conversion_value_sum as "Conversion Value Sum",
	i.interaction_count as "Interactions from Media",
	p.interaction_count as "Interactions from Placement",
	i.ad_call_count as "Page Views",
	i.not_pure_ad_call_count as "Not Pure Page Views",
	i.not_pure_low_converting_ad_call_count as "Not Pure Low Value Page Views",
	i.not_pure_low_converting_addressable_ad_call_count as "Addressable Page Views",
	i.pure_ad_call_count as "Pure Page Views",
	i.pure_low_converting_ad_call_count as "Pure Low Value Page Views",
	i.low_converting_ad_call_count as "Low Value Page Views",
	i.not_pure_low_converting_ad_call_with_ads_count as "Fillable Pages",
	i.ad_unit_served_count as "Pages Served",
	i.impression_count as Impressions,
	i.available_impression_count as "Available Impressions",
	i.gross_revenue_sum as "Gross Media Revenue from Media",
	i.net_revenue_sum as "Net Media Revenue",
	i.suppressed_by_route as "Suppressed - Route",
	i.suppressed_by_unknown_hotel_city as "Suppressed - Unknown Hotel City",
	i.suppressed_by_c13n_segment as "Suppressed - Cannibalization Segment",
	i.suppressed_by_click_blackout as "Suppressed - Click Blackout",
	i.suppressed_by_no_valid_layout as "Suppressed - No Valid Layout",
	i.suppressed_by_publisher_traffic_share as "Suppressed - Publisher Traffic Share",
	i.suppressed_by_c13n_above_threshold as "Suppressed - Cannibalization Threshold",
	i.not_pure_low_converting_intentmedia_traffic_share_ad_call_count as "Not Pure Low Value Intent Media Traffic Page Views",
	i.number_of_prechecks_served as "Number of Prechecks Served",
	i.requested_number_of_prechecks as "Requested Number of Prechecks"
from
---------Hotel SCA Media Performance
	(
    select
      aggregation_level_date_in_et as date_in_et,
      (case
        when au.name like '%UK%' then 'UK'
        when au.name like '%.ca%' then 'CA'
        else 'US'
      end) as Country,
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
      sum(not_pure_low_converting_intentmedia_traffic_share_ad_call_count) as not_pure_low_converting_intentmedia_traffic_share_ad_call_count,
      sum(number_of_prechecks_served) as number_of_prechecks_served,
      sum(requested_number_of_prechecks) as requested_number_of_prechecks
    from intent_media_production.hotel_ct_media_performance_aggregations hcmpa
    left join intent_media_production.ad_units au on au.id = hcmpa.ad_unit_id
    where aggregation_level_date_in_et >= au.reporting_start_date
      and aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York')
    group by
      aggregation_level_date_in_et,
      (case
        when au.name like '%UK%' then 'UK'
        when au.name like '%.ca%' then 'CA'
        else 'US'
      end),
      ad_unit_id
  ) i
LEFT join
/* HOTELS Placement Level Breakdown */
	(
    select
      hcptpa.date_in_et,
      placement_type as placement,
      (case
        when au.name like '%UK%' then 'UK'
        when au.name like '%.ca%' then 'CA'
        else 'US'
      end) as Country,
      hcptpa.ad_unit_id,
      sum(hcptpa.click_count) as click_count,
      sum(hcptpa.click_conversion_count) as click_conversion_count,
      sum(hcptpa.actual_cpc_sum) as actual_cpc_sum,
      sum(hcptpa.click_conversion_value_sum) as click_conversion_value_sum,
      sum(hcptpa.interaction_count) as interaction_count
    from intent_media_production.hotel_ct_placement_type_performance_aggregations hcptpa
    left join intent_media_production.ad_units au on au.id = hcptpa.ad_unit_id
    where date_in_et >= au.reporting_start_date
      and date_in_et < date(current_timestamp at timezone 'America/New_York')
    group by
      hcptpa.date_in_et,
      (case
        when au.name like '%UK%' then 'UK'
        when au.name like '%.ca%' then 'CA'
        else 'US'
      end),
      hcptpa.ad_unit_id,
      placement_type
  ) p
on i.date_in_et = p.date_in_et and i.Country = p.Country and i.ad_unit_id = p.ad_unit_id
left join intent_media_production.ad_units au on i.ad_unit_id = au.id
left join intent_media_production.sites s on au.site_id = s.id
left join intent_media_production.entities e on e.id = s.publisher_id
left join intent_media_production.legal_page_types lpt on lpt.id = au.legal_page_type_id

