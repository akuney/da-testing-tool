select
	entities.name as "Advertiser Name",
    entities.id as "Advertiser ID",
	'Flights' as "Product Category Type",
	(case entities.advertiser_category_type
		when 'AIRLINE_DOMESTIC' then 'Domestic Airline'
		when 'AIRLINE_INTERNATIONAL' then 'International Airline'
		when 'HOTEL_CHAIN' then 'Hotel Chain'
		when 'META' then 'PPA'
		when 'TIER_1' then 'Tier 1'
		when 'TIER_2' then 'Tier 2'
		when 'OTA_BUDGET' then 'OTA Budget'
		when 'OTHER' then 'Other'
		else entities.advertiser_category_type
	end) as "Advertiser Segment",
	ad_groups.name as "Ad Group",
	campaigns.name as "Campaign",
	campaigns.precheck_eligibility_type as "Precheck Type",
	acapra.date_in_et as Date,
	'Total' as Site,
	performance."Eligible Ad Calls" as "Eligible Ad Calls",
	performance."Non-Learning Impressions" as "Non-Learning Impressions",
	performance."Filtered Ad Count" as "Filtered Ad Count",
        performance."Filtered Ad for Budget Count",
        performance."Filtered Ad for Bid Count",
        performance."Filtered Ad for Click Blackout",
	sum(acapra.impression_count) as Impressions,
	sum(acapra.click_count) as Clicks,
	sum(acapra.actual_cpc_sum) as Spend,
	sum(acapra.click_conversion_count) as "Click Conversions",
	sum(acapra.exposed_conversion_count) as "View Conversions",
	sum(acapra.click_conversion_value_sum) as "Click Revenue",
	sum(acapra.exposed_conversion_value_sum) as "Exposed Revenue",
	sum(acapra.auction_position_sum) as "Auction Position Sum"
from intent_media_production.air_ct_advertiser_performance_report_aggregations acapra
right join 
                (select
			aggregation_level_date_in_et date_in_et,
			advertiser_id,
			ad_group_id,
			campaign_id,
			sum(acisra.filtered_ad_count+acisra.impression_count) as "Eligible Ad Calls",
			sum(acisra.impression_count) as "Non-Learning Impressions",
			sum(acisra.filtered_ad_count) as "Filtered Ad Count",
                        sum(acisra.filtered_ad_for_budget_count) as "Filtered Ad for Budget Count",
                        sum(acisra.filtered_ad_for_bid_count) as "Filtered Ad for Bid Count",
                        sum(acisra.filtered_ad_for_click_blackout_count) as "Filtered Ad for Click Blackout"
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
group by 
    entities.name, 
    entities.id,
    entities.advertiser_category_type, 
    ad_groups.name, 
    campaigns.name, 
    acapra.date_in_et, 
    performance."Eligible Ad Calls", 
    performance."Non-Learning Impressions", 
    performance."Filtered Ad Count",
    performance."Filtered Ad for Budget Count",
    performance."Filtered Ad for Bid Count",
    performance."Filtered Ad for Click Blackout",
    campaigns.precheck_eligibility_type

union

-------------------------------------HOTEL-------------------------------
select
	entities.name as "Advertiser Name",
    entities.id as "Advertiser ID",
	'Hotels' as "Product Category Type",
	(case entities.advertiser_category_type
		when 'AIRLINE_DOMESTIC' then 'Domestic Airline'
		when 'AIRLINE_INTERNATIONAL' then 'International Airline'
		when 'HOTEL_CHAIN' then 'Hotel Chain'
		when 'META' then 'PPA'
		when 'TIER_1' then 'Tier 1'
		when 'TIER_2' then 'Tier 2'
		when 'OTA_BUDGET' then 'OTA Budget'
		when 'OTHER' then 'Other'
		else entities.advertiser_category_type
	end) as "Advertiser Segment",
	ad_groups.name as "Ad Group",
	campaigns.name as "Campaign",
	campaigns.precheck_eligibility_type as "Precheck Type",
	acapra.date_in_et as Date,
	'Total' as Site,
	performance."Eligible Ad Calls" as "Eligible Ad Calls",
	performance."Non-Learning Impressions" as "Non-Learning Impressions",
	performance."Filtered Ad Count" as "Filtered Ad Count",
    performance."Filtered Ad for Budget Count",
    performance."Filtered Ad for Bid Count",
    performance."Filtered Ad for Click Blackout",
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
			sum(acisra.filtered_ad_count) as "Filtered Ad Count",
            sum(acisra.filtered_ad_for_budget_count) as "Filtered Ad for Budget Count",
            sum(acisra.filtered_ad_for_bid_count) as "Filtered Ad for Bid Count",
            sum(acisra.filtered_ad_for_click_blackout_count) as "Filtered Ad for Click Blackout"
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
group by 
    entities.name, 
    entities.id,
    entities.advertiser_category_type, 
    ad_groups.name, 
    campaigns.name, 
    acapra.date_in_et, 
    performance."Eligible Ad Calls", 
    performance."Non-Learning Impressions", 
    performance."Filtered Ad Count",
    performance."Filtered Ad for Budget Count",
    performance."Filtered Ad for Bid Count",
    performance."Filtered Ad for Click Blackout",
    campaigns.precheck_eligibility_type 
    
union


--------------------------------------META---------------------------------------

select
	entities.name as "Advertiser Name",
    entities.id as "Advertiser ID",
	'PPA' as "Product Category Type",
	(case entities.advertiser_category_type
		when 'AIRLINE_DOMESTIC' then 'Domestic Airline'
		when 'AIRLINE_INTERNATIONAL' then 'International Airline'
		when 'HOTEL_CHAIN' then 'Hotel Chain'
		when 'META' then 'PPA'
		when 'TIER_1' then 'Tier 1'
		when 'TIER_2' then 'Tier 2'
		when 'OTA_BUDGET' then 'OTA Budget'
		when 'OTHER' then 'Other'
		else entities.advertiser_category_type
	end) as "Advertiser Segment",
	ad_groups.name as "Ad Group",
	campaigns.name as "Campaign",
	campaigns.precheck_eligibility_type as "Precheck Type",
	acapra.date_in_et as Date,
	'Total' as Site,
	sum(acapra.filtered_ad_count + acapra.impression_count) as "Eligible Ad Calls",
	cast(null as int) as "Non-Learning Impressions",
	sum(acapra.filtered_ad_count) as "Filtered Ad Count",
    cast(null as int) as "Filtered Ad for Budget Count",
    cast(null as int) as "Filtered Ad for Bid Count",
    cast(null as int) as "Filtered Ad for Click Blackout",
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
group by 
    entities.name, 
    entities.id,
    entities.advertiser_category_type, 
    ad_groups.name, 
    campaigns.name, 
    acapra.date_in_et,
    campaigns.precheck_eligibility_type