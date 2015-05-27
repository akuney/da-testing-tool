-- lines commented out because of error in table
select
	appa.date_in_et as Date,
	appa.advertiser_id as "Advertiser ID", 
	appa.campaign_id as "Campaign ID", 
	appa.ad_group_id as "Ad Group ID", 
	appa.air_ct_ad_copy_id as "Ad Copy ID", 
	appa.air_ct_creative_id as "Creative ID",
	e.name as "Advertiser Name",
	c.name as Campaign,
	c.display_format as "Display Type",
	ag.name as "Ad Group",
	ac.text as "Ad Copy",
	cr.name as Logo,
	cs.creative_size_url as "Creative URL",
	sum(impression_count) as Impressions,
	sum(click_count) as Clicks,
	sum(actual_cpc_sum) as Cost,
	c.precheck_eligibility_type as "Precheck Type"
-- 	sum(click_conversion_count) as Conversions,
-- 	sum(click_conversion_value_sum) as Revenue
from intent_media_production.air_ct_auction_position_performance_aggregations appa
left join intent_media_production.entities e on e.id = appa.advertiser_id
left join intent_media_production.campaigns c on c.id = appa.campaign_id
left join intent_media_production.ad_groups ag on ag.id = appa.ad_group_id
left join intent_media_production.ad_copies ac on ac.id = appa.air_ct_ad_copy_id
left join intent_media_production.creatives cr on cr.id = appa.air_ct_creative_id
left join intent_media_production.creative_sizes cs on cs.creative_id = appa.air_ct_creative_id
where date_in_et < date(current_timestamp at timezone 'America/New_York')
and cs.creative_size_type = 'LARGE'
group by
	appa.date_in_et,
	appa.advertiser_id, 
	appa.campaign_id, 
	appa.ad_group_id, 
	appa.air_ct_ad_copy_id, 
	appa.air_ct_creative_id,
	e.name,
	c.name,
	c.display_format,
	ag.name,
	ac.text,
	cr.name,
	cs.creative_size_url,
        c.precheck_eligibility_type