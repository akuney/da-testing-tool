/* Note: 'site_country', 'device_family', 'browser_family', and 'browser' are populated in media_performance_aggregations from 2014/08/30 for hotels and from 2014/08/31 for air */

/* FLIGHTS Placement Level Breakdown */
select 
	'Flights' as "Product Category Type",
	i.date_in_et as Date,
	i.Country,
	--i.device_family as "Device Family",
	--i.browser_family as "Browser Family",
	--i.browser as "Browser",
	e.publisher_tier as "Publisher Tier",
	(case
	  when e.name = 'Air Fast Tickets' then 'AirFastTickets'
	  when e.name = 'Amoma' then 'AMOMA.com'
	  when e.name = 'Bookit' then 'BookIt.com'
	  when e.name = 'Expedia' then 'Expedia Inc.'
	  when e.name = 'Hotwire' then 'Expedia Inc.'
	  when e.name = 'Travelocity on Expedia' then 'Expedia Inc.'
	  when e.name = 'Kayak Software Corporation' then 'KAYAK'
	  when e.name = 'lastminute.com' then 'Sabre Holdings'
	  when e.name = 'Orbitz' then 'Orbitz Worldwide, Inc.'
	  when e.name = 'Oversee' then 'Oversee.net'
    else e.name
  end) as Publisher,
	ifnull(lpt.page_type, 'Other') as "Type of Ad Unit",
  s.display_name as Site,
	au.name as "Ad Unit",
	p.Placement,
	p.is_prechecked as "Auction Type",
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
/* Media */
  (
    select
      aggregation_level_date_in_et as date_in_et,
      (case
        when aggregation_level_date_in_et >= '2014-08-31' then site_country
        else (case when au.name like '%UK%' then 'GB' when au.name like '%.ca%' then 'CA' else 'US' end)
      end) as Country,
      --device_family,
      --browser_family,
      --browser,
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
        when aggregation_level_date_in_et >= '2014-08-31' then site_country
        else (case when au.name like '%UK%' then 'GB' when au.name like '%.ca%' then 'CA' else 'US' end)
      end),
      --device_family,
      --browser_family,
      --browser,
      ad_unit_id
  ) i
left join
/* Placement */
  (
    select
      acptpa.date_in_et,
      acptpa.ad_unit_id,
      (case acptpa.placement_type
        when 'CLIENT_DEFINED' then 'Client Defined'
        when 'DIALOG' then 'Dialog'
        when 'EXIT_OVERLAY' then 'Exit Overlay'
        when 'EXIT_UNIT' then 'Exit Unit'
        when 'FOOTER' then 'Footer'
        when 'FORM_COMPARE' then 'Form Compare'
        when 'INTER_CARD' then 'Inter Card'
        when 'MAP' then 'Map'
        when 'MINI_CARD' then 'Mini Card'
        when 'RIGHT_RAIL' then 'Right Rail'
        when 'SEARCH_FORM' then 'Search Form'
        when 'TOASTER' then 'Toaster'
        when 'TOP_CARD' then 'Top Card'
        else acptpa.placement_type
      end) as "Placement",
      (case is_prechecked when true then 'Precheck' else 'Non-Precheck' end) as is_prechecked,
      sum(acptpa.interaction_count) as interaction_count,
      sum(acptpa.click_count) as click_count,
      sum(acptpa.click_conversion_count) as click_conversion_count,
      sum(acptpa.actual_cpc_sum) as actual_cpc_sum,
      sum(acptpa.click_conversion_value_sum) as click_conversion_value_sum
    from intent_media_production.air_ct_placement_type_performance_aggregations acptpa
    left join intent_media_production.ad_units au on au.id = acptpa.ad_unit_id
    left join intent_media_production.sites s on s.id = au.site_id
    where acptpa.date_in_et >= au.reporting_start_date
      and acptpa.date_in_et < date(current_timestamp at timezone 'America/New_York')
    group by
      acptpa.date_in_et,
      acptpa.ad_unit_id,
      (case acptpa.placement_type
        when 'CLIENT_DEFINED' then 'Client Defined'
        when 'DIALOG' then 'Dialog'
        when 'EXIT_OVERLAY' then 'Exit Overlay'
        when 'EXIT_UNIT' then 'Exit Unit'
        when 'FOOTER' then 'Footer'
        when 'FORM_COMPARE' then 'Form Compare'
        when 'INTER_CARD' then 'Inter Card'
        when 'MAP' then 'Map'
        when 'MINI_CARD' then 'Mini Card'
        when 'RIGHT_RAIL' then 'Right Rail'
        when 'SEARCH_FORM' then 'Search Form'
        when 'TOASTER' then 'Toaster'
        when 'TOP_CARD' then 'Top Card'
        else acptpa.placement_type
      end),
      (case is_prechecked when true then 'Precheck' else 'Non-Precheck' end)
	) p
on i.date_in_et = p.date_in_et and i.ad_unit_id = p.ad_unit_id
left join intent_media_production.ad_units au on i.ad_unit_id = au.id
left join intent_media_production.sites s on au.site_id = s.id
left join intent_media_production.entities e on e.id = s.publisher_id
left join intent_media_production.legal_page_types lpt on lpt.id = au.legal_page_type_id

union

/* FLIGHTS Total */
select
	'Flights' as "Product Category Type",
	i.date_in_et as Date,
	i.Country,
	--i.device_family as "Device Family",
	--i.browser_family as "Browser Family",
	--i.browser as "Browser",
	e.publisher_tier as "Publisher Tier",
	(case
	  when e.name = 'Air Fast Tickets' then 'AirFastTickets'
	  when e.name = 'Amoma' then 'AMOMA.com'
	  when e.name = 'Bookit' then 'BookIt.com'
	  when e.name = 'Expedia' then 'Expedia Inc.'
	  when e.name = 'Hotwire' then 'Expedia Inc.'
	  when e.name = 'Travelocity on Expedia' then 'Expedia Inc.'
	  when e.name = 'Kayak Software Corporation' then 'KAYAK'
	  when e.name = 'lastminute.com' then 'Sabre Holdings'
	  when e.name = 'Orbitz' then 'Orbitz Worldwide, Inc.'
	  when e.name = 'Oversee' then 'Oversee.net'
    else e.name
  end) as Publisher,
	ifnull(lpt.page_type, 'Other') as "Type of Ad Unit",
	s.display_name as Site,
	au.name as "Ad Unit",
	'Total' as Placement,
	'Total Placement' as "Auction Type",
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
/* Media */
	(
    select
      aggregation_level_date_in_et as date_in_et,
      (case
        when aggregation_level_date_in_et >= '2014-08-31' then site_country
        else (case when au.name like '%UK%' then 'GB' when au.name like '%.ca%' then 'CA' else 'US' end)
      end) as Country,
      --device_family,
      --browser_family,
      --browser,
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
        when aggregation_level_date_in_et >= '2014-08-31' then site_country
        else (case when au.name like '%UK%' then 'GB' when au.name like '%.ca%' then 'CA' else 'US' end)
      end),
      --device_family,
      --browser_family,
      --browser,
      ad_unit_id
  ) i
left join
/* Placement */
	(
    select
      acptpa.date_in_et,
      acptpa.ad_unit_id,
      sum(acptpa.click_count) as click_count,
      sum(acptpa.click_conversion_count) as click_conversion_count,
      sum(acptpa.actual_cpc_sum) as actual_cpc_sum,
      sum(acptpa.click_conversion_value_sum) as click_conversion_value_sum,
      sum(acptpa.interaction_count) as interaction_count
    from intent_media_production.air_ct_placement_type_performance_aggregations acptpa
    left join intent_media_production.ad_units au on au.id = acptpa.ad_unit_id
    where acptpa.date_in_et >= au.reporting_start_date
      and acptpa.date_in_et < date(current_timestamp at timezone 'America/New_York')
    group by
      acptpa.date_in_et,
      acptpa.ad_unit_id
	) p
on i.date_in_et = p.date_in_et and i.ad_unit_id = p.ad_unit_id
left join intent_media_production.ad_units au on i.ad_unit_id = au.id
left join intent_media_production.sites s on au.site_id = s.id
left join intent_media_production.entities e on e.id = s.publisher_id
left join intent_media_production.legal_page_types lpt on lpt.id = au.legal_page_type_id

union

/* HOTELS Placement Level Breakdown */
select 
	'Hotels' as "Product Category Type",
	i.date_in_et as Date,
	i.Country,
	--i.device_family as "Device Family",
	--i.browser_family as "Browser Family",
	--i.browser as "Browser",
	e.publisher_tier as "Publisher Tier",
	(case
	  when e.name = 'Air Fast Tickets' then 'AirFastTickets'
	  when e.name = 'Amoma' then 'AMOMA.com'
	  when e.name = 'Bookit' then 'BookIt.com'
	  when e.name = 'Expedia' then 'Expedia Inc.'
	  when e.name = 'Hotwire' then 'Expedia Inc.'
	  when e.name = 'Travelocity on Expedia' then 'Expedia Inc.'
	  when e.name = 'Kayak Software Corporation' then 'KAYAK'
	  when e.name = 'lastminute.com' then 'Sabre Holdings'
	  when e.name = 'Orbitz' then 'Orbitz Worldwide, Inc.'
	  when e.name = 'Oversee' then 'Oversee.net'
    else e.name
  end) as Publisher,
	ifnull(lpt.page_type, 'Other') as "Type of Ad Unit",
	s.display_name as Site,
	au.name as "Ad Unit",
	p.Placement,
	p.is_prechecked as "Auction Type",
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
/* Media */
	(
    select
      aggregation_level_date_in_et as date_in_et,
      (case
        when aggregation_level_date_in_et >= '2014-08-31' then site_country
        else (case when au.name like '%UK%' then 'GB' when au.name like '%.ca%' then 'CA' else 'US' end)
      end) as Country,
      --device_family,
      --browser_family,
      --browser,
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
        when aggregation_level_date_in_et >= '2014-08-31' then site_country
        else (case when au.name like '%UK%' then 'GB' when au.name like '%.ca%' then 'CA' else 'US' end)
      end),
      --device_family,
      --browser_family,
      --browser,
      ad_unit_id
  ) i
left join
/* Placement */
	(
    select
      hcptpa.date_in_et,
      hcptpa.ad_unit_id,
      (case hcptpa.placement_type
        when 'CLIENT_DEFINED' then 'Client Defined'
        when 'DIALOG' then 'Dialog'
        when 'EXIT_OVERLAY' then 'Exit Overlay'
        when 'EXIT_UNIT' then 'Exit Unit'
        when 'FOOTER' then 'Footer'
        when 'FORM_COMPARE' then 'Form Compare'
        when 'INTER_CARD' then 'Inter Card'
        when 'MAP' then 'Map'
        when 'MINI_CARD' then 'Mini Card'
        when 'RIGHT_RAIL' then 'Right Rail'
        when 'SEARCH_FORM' then 'Search Form'
        when 'TOASTER' then 'Toaster'
        when 'TOP_CARD' then 'Top Card'
        else hcptpa.placement_type
      end) as Placement,
      (case is_prechecked when true then 'Precheck' else 'Non-Precheck' end) as is_prechecked,
      sum(hcptpa.click_count) as click_count,
      sum(hcptpa.click_conversion_count) as click_conversion_count,
      sum(hcptpa.actual_cpc_sum) as actual_cpc_sum,
      sum(hcptpa.click_conversion_value_sum) as click_conversion_value_sum,
      sum(hcptpa.interaction_count) as interaction_count
    from intent_media_production.hotel_ct_placement_type_performance_aggregations hcptpa
    left join intent_media_production.ad_units au on au.id = hcptpa.ad_unit_id
    where hcptpa.date_in_et >= au.reporting_start_date
      and hcptpa.date_in_et < date(current_timestamp at timezone 'America/New_York')
    group by
      hcptpa.date_in_et,
      hcptpa.ad_unit_id,
      (case hcptpa.placement_type
        when 'CLIENT_DEFINED' then 'Client Defined'
        when 'DIALOG' then 'Dialog'
        when 'EXIT_OVERLAY' then 'Exit Overlay'
        when 'EXIT_UNIT' then 'Exit Unit'
        when 'FOOTER' then 'Footer'
        when 'FORM_COMPARE' then 'Form Compare'
        when 'INTER_CARD' then 'Inter Card'
        when 'MAP' then 'Map'
        when 'MINI_CARD' then 'Mini Card'
        when 'RIGHT_RAIL' then 'Right Rail'
        when 'SEARCH_FORM' then 'Search Form'
        when 'TOASTER' then 'Toaster'
        when 'TOP_CARD' then 'Top Card'
        else hcptpa.placement_type
      end),
      (case is_prechecked when true then 'Precheck' else 'Non-Precheck' end)
  ) p
on i.date_in_et = p.date_in_et and i.ad_unit_id = p.ad_unit_id
left join intent_media_production.ad_units au on i.ad_unit_id = au.id
left join intent_media_production.sites s on au.site_id = s.id
left join intent_media_production.entities e on e.id = s.publisher_id
left join intent_media_production.legal_page_types lpt on lpt.id = au.legal_page_type_id

union

/* HOTELS Total */
select
	'Hotels' as "Product Category Type",
	i.date_in_et as Date,
	i.Country,
	--i.device_family as "Device Family",
	--i.browser_family as "Browser Family",
	--i.browser as "Browser",
	e.publisher_tier as "Publisher Tier",
	(case
	  when e.name = 'Air Fast Tickets' then 'AirFastTickets'
	  when e.name = 'Amoma' then 'AMOMA.com'
	  when e.name = 'Bookit' then 'BookIt.com'
	  when e.name = 'Expedia' then 'Expedia Inc.'
	  when e.name = 'Hotwire' then 'Expedia Inc.'
	  when e.name = 'Travelocity on Expedia' then 'Expedia Inc.'
	  when e.name = 'Kayak Software Corporation' then 'KAYAK'
	  when e.name = 'lastminute.com' then 'Sabre Holdings'
	  when e.name = 'Orbitz' then 'Orbitz Worldwide, Inc.'
	  when e.name = 'Oversee' then 'Oversee.net'
    else e.name
  end) as Publisher,
	ifnull(lpt.page_type, 'Other') as "Type of Ad Unit",
	s.display_name as Site,
	au.name as "Ad Unit",
	'Total' as Placement,
	'Total Placement' as "Auction Type",
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
/* Media */
	(
    select
      aggregation_level_date_in_et as date_in_et,
      (case
        when aggregation_level_date_in_et >= '2014-08-31' then site_country
        else (case when au.name like '%UK%' then 'GB' when au.name like '%.ca%' then 'CA' else 'US' end)
      end) as Country,
      --device_family,
      --browser_family,
      --browser,
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
        when aggregation_level_date_in_et >= '2014-08-31' then site_country
        else (case when au.name like '%UK%' then 'GB' when au.name like '%.ca%' then 'CA' else 'US' end)
      end),
      --device_family,
      --browser_family,
      --browser,
      ad_unit_id
  ) i
LEFT join
/* HOTELS Placement Level Breakdown */
	(
    select
      hcptpa.date_in_et,
      hcptpa.ad_unit_id,
      sum(hcptpa.click_count) as click_count,
      sum(hcptpa.click_conversion_count) as click_conversion_count,
      sum(hcptpa.actual_cpc_sum) as actual_cpc_sum,
      sum(hcptpa.click_conversion_value_sum) as click_conversion_value_sum,
      sum(hcptpa.interaction_count) as interaction_count
    from intent_media_production.hotel_ct_placement_type_performance_aggregations hcptpa
    left join intent_media_production.ad_units au on au.id = hcptpa.ad_unit_id
    where hcptpa.date_in_et >= au.reporting_start_date
      and hcptpa.date_in_et < date(current_timestamp at timezone 'America/New_York')
    group by
      hcptpa.date_in_et,
      hcptpa.ad_unit_id
  ) p
on i.date_in_et = p.date_in_et and i.ad_unit_id = p.ad_unit_id
left join intent_media_production.ad_units au on i.ad_unit_id = au.id
left join intent_media_production.sites s on au.site_id = s.id
left join intent_media_production.entities e on e.id = s.publisher_id
left join intent_media_production.legal_page_types lpt on lpt.id = au.legal_page_type_id