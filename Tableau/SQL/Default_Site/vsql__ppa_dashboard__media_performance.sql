-----------// Data By Placement

SELECT i.*, ISNULL(p.Placement, 'Total') as Placement, p."Clicks From Placement",  	p."Gross Revenue From Placement", p."Page View Interactions From Placement"
FROM 

(select
	'Meta' as "Product Category Type",
	aggregation_level_date_in_et as "Date",
	au.id as "Ad Unit ID",
	au.name as "Ad Unit",
	(case
        when au.name like '%UK%' then 'UK'
         else 'US'
        end) as Country,
	s.display_name as "Site",
	(case
		when s.name = 'AIRFASTTICKETS' then 'AirFastTickets'
		when s.name = 'AIRTKT' then 'AirTkt'
		when s.name = 'BUDGETAIR' then 'Airtrade International'
		when s.name = 'VAYAMA' then 'Airtrade International'
		when s.name = 'AMOMA' then 'Amoma'
		when s.name = 'BOOKIT' then 'Bookit'
		when s.name = 'CHEAPAIR' then 'CheapAir'
		when s.name = 'EXPEDIA' then 'Expedia Inc.'
		when s.name = 'EXPEDIA_CA' then 'Expedia Inc.'
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
		when s.name = 'LOWCOSTAIRLINES' then 'LowCostAirlines'
		when s.name = 'SHARETRIPS' then 'ShareTrips'
		when s.name = 'HOTELRESERVATIONS' then 'HotelReservations'
		when s.name = 'TRAVELJUNCTION' then 'Traveljunction'
		when s.name = 'ROOMER_TRAVEL' then 'Roomer Travel'		
		when s.name = 'HOTELS_DOT_COM' then 'Hotels.com'
		when s.name = 'DEALBASE' then 'Dealbase'
		when s.name = 'ROOM_SEVENTY_SEVEN' then 'Room77'		
		else 'Other'
	end) as Publisher,
	e.publisher_tier as "Publisher Tier",
	lpt.page_type as "Type of Ad Unit",
	sum(page_view_count) as "Page Views",
	sum(pure_page_view_count) as "Pure Page Views",
	sum(not_pure_page_view_count) as "Not Pure Page Views",
	sum(served_page_count) as "Served Pages",
	sum(page_view_interaction_count) as "Page View Interactions From Media",
	sum(ad_call_count) as "Ad Calls",
	sum(not_pure_ad_call_count) as "Not Pure Ad Calls",
	sum(pure_ad_call_count) as "Pure Ad Calls",
	sum(served_ad_call_count) as "Served Ad Calls",
	sum(ad_call_interaction_count) as "Ad Call Interactions",
	sum(impression_count) as "Impressions",
	sum(hmmpa.click_count) as "Clicks From Media",
	sum(gross_revenue_sum) as "Gross Revenue From Media",
	sum(clicked_ad_call_position_sum) as "Clicked Ad Call Position Sum"
from Intent_media_Production.hotel_meta_media_performance_aggregations hmmpa
left join intent_media_production.ad_units au on au.id = hmmpa.ad_unit_id
left join intent_media_production.sites s on s.id = au.site_id
left join intent_media_production.entities e on e.id = s.publisher_id
left join intent_media_production.legal_page_types lpt on lpt.id = au.legal_page_type_id


where aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York')
	and aggregation_level_date_in_et > '2014-01-14'

group by
	aggregation_level_date_in_et,
	au.id,
        s.display_name,
        au.name,
	(case
		when s.name = 'AIRFASTTICKETS' then 'AirFastTickets'
		when s.name = 'AIRTKT' then 'AirTkt'
		when s.name = 'BUDGETAIR' then 'Airtrade International'
		when s.name = 'VAYAMA' then 'Airtrade International'
		when s.name = 'AMOMA' then 'Amoma'
		when s.name = 'BOOKIT' then 'Bookit'
		when s.name = 'CHEAPAIR' then 'CheapAir'
		when s.name = 'EXPEDIA' then 'Expedia Inc.'
		when s.name = 'EXPEDIA_CA' then 'Expedia Inc.'
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
		when s.name = 'LOWCOSTAIRLINES' then 'LowCostAirlines'
		when s.name = 'SHARETRIPS' then 'ShareTrips'
		when s.name = 'HOTELRESERVATIONS' then 'HotelReservations'
		when s.name = 'TRAVELJUNCTION' then 'Traveljunction'				
		when s.name = 'ROOMER_TRAVEL' then 'Roomer Travel'
		when s.name = 'HOTELS_DOT_COM' then 'Hotels.com'		
		when s.name = 'DEALBASE' then 'Dealbase'
		when s.name = 'ROOM_SEVENTY_SEVEN' then 'Room77'		
		else 'Other'
	end),
	e.publisher_tier,
	lpt.page_type ) i
	
	
LEFT OUTER JOIN

    (select
      hmptpa.date_in_et,
      hmptpa.ad_unit_id,
      ISNULL(hmptpa.placement_type,'Unknown') as Placement,
      sum(hmptpa.click_count) as "Clicks From Placement",
      sum(hmptpa.actual_cpc_sum) as "Gross Revenue From Placement",
      sum(hmptpa.interaction_count) as "Page View Interactions From Placement"
    from intent_media_production.hotel_meta_placement_type_performance_aggregations hmptpa
    group by
      hmptpa.date_in_et,
      hmptpa.ad_unit_id,
      hmptpa.placement_type) p
      
on i.date = p.date_in_et and i."Ad Unit ID" = p.ad_unit_id
	
UNION
----------------------// Totals

-----------// Data By Placement

SELECT i.*, ISNULL(p.Placement, 'Total') as Placement, p."Clicks From Placement",  	p."Gross Revenue From Placement", p."Page View Interactions From Placement"
FROM 

(select
	'Meta' as "Product Category Type",
	aggregation_level_date_in_et as "Date",
	au.id as "Ad Unit ID",
	au.name as "Ad Unit",
	(case
        when au.name like '%UK%' then 'UK'
         else 'US'
        end) as Country,
	s.display_name as "Site",
	(case
		when s.name = 'AIRFASTTICKETS' then 'AirFastTickets'
		when s.name = 'AIRTKT' then 'AirTkt'
		when s.name = 'BUDGETAIR' then 'Airtrade International'
		when s.name = 'VAYAMA' then 'Airtrade International'
		when s.name = 'AMOMA' then 'Amoma'
		when s.name = 'BOOKIT' then 'Bookit'
		when s.name = 'CHEAPAIR' then 'CheapAir'
		when s.name = 'EXPEDIA' then 'Expedia Inc.'
		when s.name = 'EXPEDIA_CA' then 'Expedia Inc.'
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
		when s.name = 'LOWCOSTAIRLINES' then 'LowCostAirlines'
		when s.name = 'SHARETRIPS' then 'ShareTrips'
		when s.name = 'HOTELRESERVATIONS' then 'HotelReservations'
		when s.name = 'TRAVELJUNCTION' then 'Traveljunction'		
		when s.name = 'ROOMER_TRAVEL' then 'Roomer Travel'
		when s.name = 'HOTELS_DOT_COM' then 'Hotels.com'		
		when s.name = 'DEALBASE' then 'Dealbase'
		when s.name = 'ROOM_SEVENTY_SEVEN' then 'Room77'		
		else 'Other'
	end) as Publisher,
	e.publisher_tier as "Publisher Tier",
	lpt.page_type as "Type of Ad Unit",
	sum(page_view_count) as "Page Views",
	sum(pure_page_view_count) as "Pure Page Views",
	sum(not_pure_page_view_count) as "Not Pure Page Views",
	sum(served_page_count) as "Served Pages",
	sum(page_view_interaction_count) as "Page View Interactions From Media",
	sum(ad_call_count) as "Ad Calls",
	sum(not_pure_ad_call_count) as "Not Pure Ad Calls",
	sum(pure_ad_call_count) as "Pure Ad Calls",
	sum(served_ad_call_count) as "Served Ad Calls",
	sum(ad_call_interaction_count) as "Ad Call Interactions",
	sum(impression_count) as "Impressions",
	sum(hmmpa.click_count) as "Clicks From Media",
	sum(gross_revenue_sum) as "Gross Revenue From Media",
	sum(clicked_ad_call_position_sum) as "Clicked Ad Call Position Sum"
from Intent_media_Production.hotel_meta_media_performance_aggregations hmmpa
left join intent_media_production.ad_units au on au.id = hmmpa.ad_unit_id
left join intent_media_production.sites s on s.id = au.site_id
left join intent_media_production.entities e on e.id = s.publisher_id
left join intent_media_production.legal_page_types lpt on lpt.id = au.legal_page_type_id


where aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York')
	and aggregation_level_date_in_et > '2014-01-14'

group by
	aggregation_level_date_in_et,
	au.id,
        s.display_name,
        au.name,
	(case
		when s.name = 'AIRFASTTICKETS' then 'AirFastTickets'
		when s.name = 'AIRTKT' then 'AirTkt'
		when s.name = 'BUDGETAIR' then 'Airtrade International'
		when s.name = 'VAYAMA' then 'Airtrade International'
		when s.name = 'AMOMA' then 'Amoma'
		when s.name = 'BOOKIT' then 'Bookit'
		when s.name = 'CHEAPAIR' then 'CheapAir'
		when s.name = 'EXPEDIA' then 'Expedia Inc.'
		when s.name = 'EXPEDIA_CA' then 'Expedia Inc.'
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
		when s.name = 'LOWCOSTAIRLINES' then 'LowCostAirlines'
		when s.name = 'SHARETRIPS' then 'ShareTrips'
		when s.name = 'HOTELRESERVATIONS' then 'HotelReservations'
		when s.name = 'TRAVELJUNCTION' then 'Traveljunction'				
		when s.name = 'ROOMER_TRAVEL' then 'Roomer Travel'
		when s.name = 'HOTELS_DOT_COM' then 'Hotels.com'	
		when s.name = 'DEALBASE' then 'Dealbase'
		when s.name = 'ROOM_SEVENTY_SEVEN' then 'Room77'					
		else 'Other'
	end),
	e.publisher_tier,
	lpt.page_type ) i
	
	
LEFT OUTER JOIN

    (select
      hmptpa.date_in_et,
      hmptpa.ad_unit_id,
      'Total' as Placement,
      sum(hmptpa.click_count) as "Clicks From Placement",
      sum(hmptpa.actual_cpc_sum) as "Gross Revenue From Placement",
      sum(hmptpa.interaction_count) as "Page View Interactions From Placement"
    from intent_media_production.hotel_meta_placement_type_performance_aggregations hmptpa
    group by
      hmptpa.date_in_et,
      hmptpa.ad_unit_id) p
      
on i.date = p.date_in_et and i."Ad Unit ID" = p.ad_unit_id
ORDER BY Date, "Ad Unit ID"
	