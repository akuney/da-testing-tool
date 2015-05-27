select
	entities.name as "Advertiser Name",
	'Flights' as "Product Category Type",
	ad_groups.name as "Ad Group",
	campaigns.name as "Campaign",
	acisra.aggregation_level_date_in_et as Date,
	sum(acisra.filtered_ad_count) as "Filtered Ad Count",
	sum(acisra.impression_count) as Impressions,
	sum(acisra.filtered_ad_for_budget_count) as "Filtered Ad for Budget",
	sum(acisra.filtered_ad_for_bid_count) as "Filtered Ad for Bid Count",
	sum(acisra.filtered_ad_count) + sum(impression_count) as "Eligible Ad Count",
	max(max_bid_increment_needed_to_participate) as "Max Bid Increment Needed to Participate",
	sum(daily_budget_needed_to_participate) as "Daily Budget Needed to Participate",
	max(max_bid_increment_needed_for_position_one) as "Max Bid Increment Needed for Position One",
	sum(daily_budget_needed_for_position_one) as "Daily Budget Needed for Position One"
from intent_media_production.air_ct_impression_share_report_aggregations acisra
left join intent_media_production.entities on acisra.advertiser_id = entities.id
left join intent_media_production.ad_groups on acisra.ad_group_id = ad_groups.id
left join intent_media_production.campaigns on ad_groups.campaign_id = campaigns.id
where aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York')
group by
	entities.name,
	ad_groups.name,
	campaigns.name,
	acisra.aggregation_level_date_in_et


union

select
	entities.name as "Advertiser Name",
	'Hotels' as "Product Category Type",
	ad_groups.name as "Ad Group",
	campaigns.name as "Campaign",
	acisra.aggregation_level_date_in_et as Date,
	sum(acisra.filtered_ad_count) as "Filtered Ad Count",
	sum(acisra.impression_count) as Impressions,
	sum(acisra.filtered_ad_for_budget_count) as "Filtered Ad for Budget",
	sum(acisra.filtered_ad_for_bid_count) as "Filtered Ad for Bid Count",
	sum(acisra.filtered_ad_count) + sum(impression_count) as "Eligible Ad Count",
	max(max_bid_increment_needed_to_participate) as "Max Bid Increment Needed to Participate",
	sum(daily_budget_needed_to_participate) as "Daily Budget Needed to Participate",
	max(max_bid_increment_needed_for_position_one) as "Max Bid Increment Needed for Position One",
	sum(daily_budget_needed_for_position_one) as "Daily Budget Needed for Position One"
from intent_media_production.hotel_ct_impression_share_report_aggregations acisra
left join intent_media_production.entities on acisra.advertiser_id = entities.id
left join intent_media_production.ad_groups on acisra.ad_group_id = ad_groups.id
left join intent_media_production.campaigns on ad_groups.campaign_id = campaigns.id
where aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York')
group by
	entities.name,
	ad_groups.name,
	campaigns.name,
	acisra.aggregation_level_date_in_et