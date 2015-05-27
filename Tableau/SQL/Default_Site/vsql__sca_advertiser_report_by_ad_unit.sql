select
	a.date_in_et as Date,
	e.name as "Advertiser Name",
	(case e.advertiser_category_type
		when 'AIRLINE_DOMESTIC' then 'Domestic Airline'
		when 'AIRLINE_INTERNATIONAL' then 'International Airline'
		when 'HOTEL_CHAIN' then 'Hotel Chain'
		when 'META' then 'Meta'
		when 'TIER_1' then 'Tier 1'
		when 'TIER_2' then 'Tier 2'
		when 'OTHER' then 'Other'
		else e.advertiser_category_type
	end) as "Advertiser Segment",
	(case au.name
		when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit - Flights - From Homepage' 
		when 'CheapTickets Exit Unit - Firefox' then 'Cheaptickets Exit Unit - Flights - From Homepage'
		else au.name 
	end) as "Ad Unit",
	(case
		when lower(au.name) like '%exit%' then 'Total Exit Units'
		when lower(au.name) like '%list%' or lower(au.name) like '%rail%' then 'Total List Page'
		when lower(au.name) like '%media fill in%' then 'Total Media Fill In'
		when lower(au.name) like '%trip.com%' then 'Total Trip.com'
		when lower(au.name) like '%ifc%' then 'Total Integrated Form Compare'
		else au.name 
	end) as "Type of Ad Unit",
	s.display_name as "Site",
	'Flights' as "Product Category Type",
	c.precheck_eligibility_type as "Precheck Type",
	c.display_format as "Display Type",
	sum(a.impression_count) Impressions,
	sum(a.click_count) Clicks,
	sum(a.actual_cpc_sum) Cost,
	sum(a.click_conversion_count) Conversions,
	sum(a.click_conversion_value_sum) Revenue
from intent_media_production.air_ct_advertiser_performance_report_aggregations a	
left join intent_media_production.entities e on e.id = a.advertiser_id	
left join intent_media_production.ad_units au on au.id = a.ad_unit_id
left join intent_media_production.sites s on s.id = au.site_id
left join intent_media_production.campaigns c on c.id = a.campaign_ID
where 
        e.name in
                (
                'Alaska Airlines',
                'American Airlines',
                'CheapTickets-Ads',
                'Expedia-Ads',
                'FlightNetwork',
                'Hotwire-Ads',
                'JetBlue',
                'Orbitz-ads-on-Network',
                'Travelocity-Ads',
                'United Airlines',
                'Vayama',
                'Virgin America'
                )
and ((a.date_in_et > '2011-05-23' and s.name = 'ORBITZ_GLOBAL')
or (a.date_in_et > '2011-06-02' and s.name = 'CHEAPTICKETS')
or (a.date_in_et > '2011-06-22' and s.name = 'EXPEDIA')
or (a.date_in_et > '2012-02-27' and s.name = 'VAYAMA')
or (a.date_in_et > '2012-02-29' and s.name = 'BUDGETAIR')
or (a.date_in_et > '2012-11-21' and s.name = 'HOTWIRE')
or (a.date_in_et > '2013-03-03' and s.name = 'KAYAK')
or s.name not in ('ORBITZ_GLOBAL', 'CHEAPTICKETS', 'EXPEDIA', 'VAYAMA', 'BUDGETAIR', 'HOTWIRE', 'KAYAK'))
group by 
	a.date_in_et,
	e.name,
	(case e.advertiser_category_type
		when 'AIRLINE_DOMESTIC' then 'Domestic Airline'
		when 'AIRLINE_INTERNATIONAL' then 'International Airline'
		when 'HOTEL_CHAIN' then 'Hotel Chain'
		when 'META' then 'Meta'
		when 'TIER_1' then 'Tier 1'
		when 'TIER_2' then 'Tier 2'
		when 'OTHER' then 'Other'
		else e.advertiser_category_type
	end),
	(case au.name
		when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit - Flights - From Homepage' 
		when 'CheapTickets Exit Unit - Firefox' then 'Cheaptickets Exit Unit - Flights - From Homepage'
		else au.name 
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
	c.precheck_eligibility_type,
	display_format
	
	
union
---------------------------------Hotel--------------------

select
	a.date_in_et as Date,
	e.name as "Advertiser Name",
	(case e.advertiser_category_type
		when 'AIRLINE_DOMESTIC' then 'Domestic Airline'
		when 'AIRLINE_INTERNATIONAL' then 'International Airline'
		when 'HOTEL_CHAIN' then 'Hotel Chain'
		when 'META' then 'Meta'
		when 'TIER_1' then 'Tier 1'
		when 'TIER_2' then 'Tier 2'
		when 'OTHER' then 'Other'
		else e.advertiser_category_type
	end) as "Advertiser Segment",
	(case au.name
		when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit - Flights - From Homepage' 
		when 'CheapTickets Exit Unit - Firefox' then 'Cheaptickets Exit Unit - Flights - From Homepage'
		else au.name 
	end) as "Ad Unit",
	(case
		when lower(au.name) like '%exit%' then 'Total Exit Units'
		when lower(au.name) like '%list%' or lower(au.name) like '%rail%' then 'Total List Page'
		when lower(au.name) like '%media fill in%' then 'Total Media Fill In'
		when lower(au.name) like '%trip.com%' then 'Total Trip.com'
		when lower(au.name) like '%ifc%' then 'Total Integrated Form Compare'
		else au.name 
	end) as "Type of Ad Unit",
	s.display_name as "Site",
	'Hotels' as "Product Category Type",
	c.precheck_eligibility_type as "Precheck Type",
	c.display_format as "Display Type",
	sum(a.impression_count) Impressions,
	sum(a.click_count) Clicks,
	sum(a.actual_cpc_sum) Cost,
	sum(a.click_conversion_count) Conversions,
	sum(a.click_conversion_value_sum) Revenue
from intent_media_production.hotel_ct_advertiser_performance_report_aggregations a	
left join intent_media_production.entities e on e.id = a.advertiser_id	
left join intent_media_production.ad_units au on au.id = a.ad_unit_id
left join intent_media_production.sites s on s.id = au.site_id
left join intent_media_production.campaigns c on c.id = a.campaign_ID
where 
        e.name in
                (
                'Alaska Airlines',
                'American Airlines',
                'CheapTickets-Ads',
                'Expedia-Ads',
                'FlightNetwork',
                'Hotwire-Ads',
                'JetBlue',
                'Orbitz-ads-on-Network',
                'Travelocity-Ads',
                'United Airlines',
                'Vayama',
                'Virgin America'
                )
group by 
	a.date_in_et,
	e.name,
	(case e.advertiser_category_type
		when 'AIRLINE_DOMESTIC' then 'Domestic Airline'
		when 'AIRLINE_INTERNATIONAL' then 'International Airline'
		when 'HOTEL_CHAIN' then 'Hotel Chain'
		when 'META' then 'Meta'
		when 'TIER_1' then 'Tier 1'
		when 'TIER_2' then 'Tier 2'
		when 'OTHER' then 'Other'
		else e.advertiser_category_type
	end),
	(case au.name
		when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit - Flights - From Homepage' 
		when 'CheapTickets Exit Unit - Firefox' then 'Cheaptickets Exit Unit - Flights - From Homepage'
		else au.name 
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
	c.precheck_eligibility_type,
	display_format
