select
	entities.name as "Advertiser Name",
	'Flights' as "Product Category Type",
	(case entities.advertiser_category_type
		when 'AIRLINE_DOMESTIC' then 'Domestic Airline'
		when 'AIRLINE_INTERNATIONAL' then 'International Airline'
		when 'HOTEL_CHAIN' then 'Hotel Chain'
		when 'META' then 'Meta'
		when 'TIER_1' then 'Tier 1'
		when 'TIER_2' then 'Tier 2'
		when 'OTHER' then 'Other'
		else entities.advertiser_category_type
	end) as "Advertiser Segment",
	ad_groups.name as "Ad Group",
	campaigns.name as "Campaign",
	acapra.date_in_et as Date,
	(case sites.name
		when 'CHEAPTICKETS' then 'CheapTickets'
		when 'EXPEDIA' then 'Expedia'
		when 'ORBITZ_GLOBAL' then 'Orbitz'
		when 'VAYAMA' then 'Vayama'
		when 'BUDGETAIR' then 'BudgetAir'
		when 'HOTWIRE' then 'Hotwire'
		when 'BOOKIT' then 'Bookit'
		when 'TRIPDOTCOM' then 'Trip.com'
		else sites.name
	end) as Site,
	sum(acapra.impression_count) as Impressions,
	sum(acapra.click_count) as Clicks,
	sum(acapra.actual_cpc_sum) as Spend,
	sum(acapra.click_conversion_count) as "Click Conversions",
	sum(acapra.exposed_conversion_count) as "View Conversions",
	sum(acapra.click_conversion_value_sum) as "Click Revenue",
	sum(acapra.exposed_conversion_value_sum) as "Exposed Revenue",
	sum(acapra.auction_position_sum) as "Auction Position Sum"
from intent_media_production.air_ct_advertiser_performance_report_aggregations acapra
left join intent_media_production.entities on acapra.advertiser_id = entities.id
left join intent_media_production.ad_groups on acapra.ad_group_id = ad_groups.id
left join intent_media_production.campaigns on ad_groups.campaign_id = campaigns.id
left join intent_media_production.ad_units on acapra.ad_unit_id = ad_units.id
left join intent_media_production.sites on ad_units.site_id = sites.id
where acapra.date_in_et < date(current_timestamp at timezone 'America/New_York')
group by entities.name, entities.advertiser_category_type, ad_groups.name, campaigns.name, acapra.date_in_et, sites.name

union


select
	entities.name as "Advertiser Name",
	'Hotels' as "Product Category Type",
	(case entities.advertiser_category_type
		when 'AIRLINE_DOMESTIC' then 'Domestic Airline'
		when 'AIRLINE_INTERNATIONAL' then 'International Airline'
		when 'HOTEL_CHAIN' then 'Hotel Chain'
		when 'META' then 'Meta'
		when 'TIER_1' then 'Tier 1'
		when 'TIER_2' then 'Tier 2'
		when 'OTHER' then 'Other'
		else entities.advertiser_category_type
	end) as "Advertiser Segment",
	ad_groups.name as "Ad Group",
	campaigns.name as "Campaign",
	acapra.date_in_et as Date,
	(case sites.name
		when 'CHEAPTICKETS' then 'CheapTickets'
		when 'EXPEDIA' then 'Expedia'
		when 'ORBITZ_GLOBAL' then 'Orbitz'
		when 'VAYAMA' then 'Vayama'
		when 'BUDGETAIR' then 'BudgetAir'
		when 'HOTWIRE' then 'Hotwire'
		when 'BOOKIT' then 'Bookit'
		when 'TRIPDOTCOM' then 'Trip.com'
		else sites.name
	end) as Site,
	sum(acapra.impression_count) as Impressions,
	sum(acapra.click_count) as Clicks,
	sum(acapra.actual_cpc_sum) as Spend,
	sum(acapra.click_conversion_count) as "Click Conversions",
	sum(acapra.exposed_conversion_count) as "View Conversions",
	sum(acapra.click_conversion_value_sum) as "Click Revenue",
	sum(acapra.exposed_conversion_value_sum) as "Exposed Revenue",
	sum(acapra.auction_position_sum) as "Auction Position Sum"
from intent_media_production.hotel_ct_advertiser_performance_report_aggregations acapra
left join intent_media_production.entities on acapra.advertiser_id = entities.id
left join intent_media_production.ad_groups on acapra.ad_group_id = ad_groups.id
left join intent_media_production.campaigns on ad_groups.campaign_id = campaigns.id
left join intent_media_production.ad_units on acapra.ad_unit_id = ad_units.id
left join intent_media_production.sites on ad_units.site_id = sites.id
where acapra.date_in_et < date(current_timestamp at timezone 'America/New_York')
group by entities.name, entities.advertiser_category_type, ad_groups.name, campaigns.name, acapra.date_in_et, sites.name
