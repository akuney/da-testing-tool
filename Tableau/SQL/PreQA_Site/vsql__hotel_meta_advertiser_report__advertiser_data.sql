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
		when 'OTA_BUDGET' then 'OTA Budget'
		when 'OTHER' then 'Other'
		else entities.advertiser_category_type
	end) as "Advertiser Segment",
	ad_groups.name as "Ad Group",
	campaigns.name as "Campaign",
	acapra.date_in_et as Date,
	sites.display_name as Site,
	cast(NULL as float) as "Eligible Ad Calls",
	cast(NULL as float) as "Non-Learning Impressions",
	cast(NULL as float) as "Filtered Ad Count",
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
group by entities.name, entities.advertiser_category_type, ad_groups.name, campaigns.name, acapra.date_in_et, sites.display_name

union

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
		when 'OTA_BUDGET' then 'OTA Budget'
		when 'OTHER' then 'Other'
		else entities.advertiser_category_type
	end) as "Advertiser Segment",
	ad_groups.name as "Ad Group",
	campaigns.name as "Campaign",
	acapra.date_in_et as Date,
	'Total' as Site,
	performance."Eligible Ad Calls" as "Eligible Ad Calls",
	performance."Non-Learning Impressions" as "Non-Learning Impressions",
	performance."Filtered Ad Count" as "Filtered Ad Count",
	sum(acapra.impression_count) as Impressions,
	sum(acapra.click_count) as Clicks,
	sum(acapra.actual_cpc_sum) as Spend,
	sum(acapra.click_conversion_count) as "Click Conversions",
	sum(acapra.exposed_conversion_count) as "View Conversions",
	sum(acapra.click_conversion_value_sum) as "Click Revenue",
	sum(acapra.exposed_conversion_value_sum) as "Exposed Revenue",
	sum(acapra.auction_position_sum) as "Auction Position Sum"
from intent_media_production.air_ct_advertiser_performance_report_aggregations acapra
right join (select
			aggregation_level_date_in_et date_in_et,
			advertiser_id,
			ad_group_id,
			campaign_id,
			sum(acisra.filtered_ad_count+acisra.impression_count) as "Eligible Ad Calls",
			sum(acisra.impression_count) as "Non-Learning Impressions",
			sum(acisra.filtered_ad_count) as "Filtered Ad Count"
		from intent_media_production.air_ct_impression_share_report_aggregations acisra
		group by aggregation_level_date_in_et, advertiser_id, ad_group_id, campaign_id) performance
on acapra.date_in_et = performance.date_in_et 
	and acapra.advertiser_id = performance.advertiser_id
	and acapra.ad_group_id = performance.ad_group_id
	and acapra.campaign_id = performance.campaign_id
left join intent_media_production.entities on acapra.advertiser_id = entities.id
left join intent_media_production.ad_groups on acapra.ad_group_id = ad_groups.id
left join intent_media_production.campaigns on ad_groups.campaign_id = campaigns.id
where acapra.date_in_et < date(current_timestamp at timezone 'America/New_York')
group by entities.name, entities.advertiser_category_type, ad_groups.name, campaigns.name, acapra.date_in_et, performance."Eligible Ad Calls", performance."Non-Learning Impressions", performance."Filtered Ad Count"

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
		when 'OTA_BUDGET' then 'OTA Budget'
		when 'OTHER' then 'Other'
		else entities.advertiser_category_type
	end) as "Advertiser Segment",
	ad_groups.name as "Ad Group",
	campaigns.name as "Campaign",
	acapra.date_in_et as Date,
	sites.display_name as Site,
	cast(NULL as float) as "Eligible Ad Calls",
	cast(NULL as float) as "Non-Learning Impressions",
	cast(NULL as float) as "Filtered Ad Count",
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
group by entities.name, entities.advertiser_category_type, ad_groups.name, campaigns.name, acapra.date_in_et, sites.display_name

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
		when 'OTA_BUDGET' then 'OTA Budget'
		when 'OTHER' then 'Other'
		else entities.advertiser_category_type
	end) as "Advertiser Segment",
	ad_groups.name as "Ad Group",
	campaigns.name as "Campaign",
	acapra.date_in_et as Date,
	'Total' as Site,
	performance."Eligible Ad Calls" as "Eligible Ad Calls",
	performance."Non-Learning Impressions" as "Non-Learning Impressions",
	performance."Filtered Ad Count" as "Filtered Ad Count",
	sum(acapra.impression_count) as Impressions,
	sum(acapra.click_count) as Clicks,
	sum(acapra.actual_cpc_sum) as Spend,
	sum(acapra.click_conversion_count) as "Click Conversions",
	sum(acapra.exposed_conversion_count) as "View Conversions",
	sum(acapra.click_conversion_value_sum) as "Click Revenue",
	sum(acapra.exposed_conversion_value_sum) as "Exposed Revenue",
	sum(acapra.auction_position_sum) as "Auction Position Sum"
from intent_media_production.hotel_ct_advertiser_performance_report_aggregations acapra
right join (select
			aggregation_level_date_in_et date_in_et,
			advertiser_id,
			ad_group_id,
			campaign_id,
			sum(acisra.filtered_ad_count+acisra.impression_count) as "Eligible Ad Calls",
			sum(acisra.impression_count) as "Non-Learning Impressions",
			sum(acisra.filtered_ad_count) as "Filtered Ad Count"
		from intent_media_production.hotel_ct_impression_share_report_aggregations acisra
		group by aggregation_level_date_in_et, advertiser_id, ad_group_id, campaign_id) performance
on acapra.date_in_et = performance.date_in_et 
	and acapra.advertiser_id = performance.advertiser_id
	and acapra.ad_group_id = performance.ad_group_id
	and acapra.campaign_id = performance.campaign_id
left join intent_media_production.entities on acapra.advertiser_id = entities.id
left join intent_media_production.ad_groups on acapra.ad_group_id = ad_groups.id
left join intent_media_production.campaigns on ad_groups.campaign_id = campaigns.id
where acapra.date_in_et < date(current_timestamp at timezone 'America/New_York')
group by entities.name, entities.advertiser_category_type, ad_groups.name, campaigns.name, acapra.date_in_et, performance."Eligible Ad Calls", performance."Non-Learning Impressions", performance."Filtered Ad Count"

union

select
	entities.name as "Advertiser Name",
	'Meta' as "Product Category Type",
	(case entities.advertiser_category_type
		when 'AIRLINE_DOMESTIC' then 'Domestic Airline'
		when 'AIRLINE_INTERNATIONAL' then 'International Airline'
		when 'HOTEL_CHAIN' then 'Hotel Chain'
		when 'META' then 'Meta'
		when 'TIER_1' then 'Tier 1'
		when 'TIER_2' then 'Tier 2'
		when 'OTA_BUDGET' then 'OTA Budget'
		when 'OTHER' then 'Other'
		else entities.advertiser_category_type
	end) as "Advertiser Segment",
	ad_groups.name as "Ad Group",
	campaigns.name as "Campaign",
	acapra.date_in_et as Date,
	sites.display_name as Site,
	sum(acapra.filtered_ad_count + acapra.impression_count) as "Eligible Ad Calls",
	cast(null as int) as "Non-Learning Impressions",
	sum(acapra.filtered_ad_count) as "Filtered Ad Count",
	sum(acapra.impression_count) as Impressions,
	sum(acapra.click_count) as Clicks,
	sum(acapra.actual_cpc_sum) as Spend,
	cast(null as int) as "Click Conversions",
	cast(null as int) as "View Conversions",
	cast(null as int) as "Click Revenue",
	cast(null as int) as "Exposed Revenue",
	sum(acapra.auction_position_sum) as "Auction Position Sum"
from intent_media_production.hotel_meta_advertiser_performance_aggregations acapra
left join intent_media_production.entities on acapra.advertiser_id = entities.id
left join intent_media_production.ad_groups on acapra.ad_group_id = ad_groups.id
left join intent_media_production.campaigns on ad_groups.campaign_id = campaigns.id
left join intent_media_production.ad_units on acapra.ad_unit_id = ad_units.id
left join intent_media_production.sites on ad_units.site_id = sites.id
where acapra.date_in_et < date(current_timestamp at timezone 'America/New_York')
group by entities.name, entities.advertiser_category_type, ad_groups.name, campaigns.name, acapra.date_in_et, sites.display_name

union

select
	entities.name as "Advertiser Name",
	'Meta' as "Product Category Type",
	(case entities.advertiser_category_type
		when 'AIRLINE_DOMESTIC' then 'Domestic Airline'
		when 'AIRLINE_INTERNATIONAL' then 'International Airline'
		when 'HOTEL_CHAIN' then 'Hotel Chain'
		when 'META' then 'Meta'
		when 'TIER_1' then 'Tier 1'
		when 'TIER_2' then 'Tier 2'
		when 'OTA_BUDGET' then 'OTA Budget'
		when 'OTHER' then 'Other'
		else entities.advertiser_category_type
	end) as "Advertiser Segment",
	ad_groups.name as "Ad Group",
	campaigns.name as "Campaign",
	acapra.date_in_et as Date,
	'Total' as Site,
	sum(acapra.filtered_ad_count + acapra.impression_count) as "Eligible Ad Calls",
	cast(null as int) as "Non-Learning Impressions",
	sum(acapra.filtered_ad_count) as "Filtered Ad Count",
	sum(acapra.impression_count) as Impressions,
	sum(acapra.click_count) as Clicks,
	sum(acapra.actual_cpc_sum) as Spend,
	cast(null as int) as "Click Conversions",
	cast(null as int) as "View Conversions",
	cast(null as int) as "Click Revenue",
	cast(null as int) as "Exposed Revenue",
	sum(acapra.auction_position_sum) as "Auction Position Sum"
from intent_media_production.hotel_meta_advertiser_performance_aggregations acapra
left join intent_media_production.entities on acapra.advertiser_id = entities.id
left join intent_media_production.ad_groups on acapra.ad_group_id = ad_groups.id
left join intent_media_production.campaigns on ad_groups.campaign_id = campaigns.id
where acapra.date_in_et < date(current_timestamp at timezone 'America/New_York')
group by entities.name, entities.advertiser_category_type, ad_groups.name, campaigns.name, acapra.date_in_et


