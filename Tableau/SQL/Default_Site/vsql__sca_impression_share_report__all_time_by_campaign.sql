select
	'Flights' as "Product Category Type",
	acisra.aggregation_level_date_in_et as Date,
	e.name as "Advertiser Name",
	c.name as Campaign,
	c.precheck_eligibility_type as "Precheck Type",
	c.display_format as "Display Type",
	sum(acisra.filtered_ad_count) as "Filtered Ad Count",
	sum(acisra.impression_count) as Impressions,
	sum(acisra.filtered_ad_for_budget_count) as "Filtered Ad for Budget",
	sum(acisra.filtered_ad_for_bid_count) as "Filtered Ad for Bid Count",
	sum(acisra.filtered_ad_for_click_blackout_count) as "Filtered Ad for Click Blackout",
	sum(acisra.filtered_ad_count + acisra.impression_count) as "Eligible Ad Count",
	max(max_bid_increment_needed_to_participate) as "Max Bid Increment Needed to Participate",
	sum(daily_budget_needed_to_participate) as "Daily Budget Needed to Participate",
	max(max_bid_increment_needed_for_position_one) as "Max Bid Increment Needed for Position One",
	sum(daily_budget_needed_for_position_one) as "Daily Budget Needed for Position One"
from intent_media_production.air_ct_impression_share_report_aggregations acisra
left join intent_media_production.entities e on acisra.advertiser_id = e.id
left join intent_media_production.ad_groups ag on acisra.ad_group_id = ag.id
left join intent_media_production.campaigns c on ag.campaign_id = c.id
where aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York')
group by
	acisra.aggregation_level_date_in_et,
	e.name,
	c.name,
	c.precheck_eligibility_type,
	c.display_format

union

select
	'Hotels' as "Product Category Type",
	hcisra.aggregation_level_date_in_et as Date,
	e.name as "Advertiser Name",
	c.name as Campaign,
	c.precheck_eligibility_type as "Precheck Type",
	c.display_format as "Display Type",
	sum(hcisra.filtered_ad_count) as "Filtered Ad Count",
	sum(hcisra.impression_count) as Impressions,
	sum(hcisra.filtered_ad_for_budget_count) as "Filtered Ad for Budget",
	sum(hcisra.filtered_ad_for_bid_count) as "Filtered Ad for Bid Count",
	sum(hcisra.filtered_ad_for_click_blackout_count) as "Filtered Ad for Click Blackout",
	sum(hcisra.filtered_ad_count + hcisra.impression_count) as "Eligible Ad Count",
	max(max_bid_increment_needed_to_participate) as "Max Bid Increment Needed to Participate",
	sum(daily_budget_needed_to_participate) as "Daily Budget Needed to Participate",
	max(max_bid_increment_needed_for_position_one) as "Max Bid Increment Needed for Position One",
	sum(daily_budget_needed_for_position_one) as "Daily Budget Needed for Position One"
from intent_media_production.hotel_ct_impression_share_report_aggregations hcisra
left join intent_media_production.entities e on hcisra.advertiser_id = e.id
left join intent_media_production.ad_groups ag on hcisra.ad_group_id = ag.id
left join intent_media_production.campaigns c on ag.campaign_id = c.id
where aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York')
group by
	hcisra.aggregation_level_date_in_et,
	e.name,
	c.name,
	c.precheck_eligibility_type,
	c.display_format
