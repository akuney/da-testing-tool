select
	'Flights' as "Product Category",
	date_in_et as Date,
	e.name as "Advertiser Name",
	e.id as "Advertiser ID",
	campaigns.name as Campaign,
	campaigns.tracking_code as "Tracking Code",
	ad_groups.name as "Ad Group",
	(case when campaigns.paused = 0 then 'Active' else 'Paused' end) as Status,
	sum(impression_count) as Impressions,
	sum(click_count) as Clicks,
	sum(click_conversion_count) as "Click Conversions",
	sum(actual_cpc_sum) as Spend,
	sum(auction_position_sum) as "Auction Position Sum",
	sum(click_conversion_value_sum) as "Click Revenue",
	sum(exposed_conversion_count) as "Exposed Conversions",
	sum(exposed_conversion_value_sum) as "Exposed Revenue"
from intent_media_production.air_ct_advertiser_performance_report_aggregations acapra
left join intent_media_production.campaigns on campaigns.id = acapra.campaign_id
left join intent_media_production.ad_groups on ad_groups.id = acapra.ad_group_id
left join intent_media_production.entities e on e.id = acapra.advertiser_id
where campaigns.id not in (4189,4190,4191,4192,4193)
and date_in_et < date(current_timestamp at timezone 'America/New_York')
group by
	date_in_et,
	e.name,
	e.id,
	campaigns.name,
	campaigns.tracking_code,
	ad_groups.name,
	(case when campaigns.paused = 0 then 'Active' else 'Paused' end)
	
union

select
	'Hotels' as "Product Category",
	date_in_et as Date,
	e.name as "Advertiser Name",
	e.id as "Advertiser ID",
	campaigns.name as Campaign,
	campaigns.tracking_code as "Tracking Code",
	ad_groups.name as "Ad Group",
	(case when campaigns.paused = 0 then 'Active' else 'Paused' end) as Status,
	sum(impression_count) as Impressions,
	sum(click_count) as Clicks,
	sum(click_conversion_count) as "Click Conversions",
	sum(actual_cpc_sum) as Spend,
	sum(auction_position_sum) as "Auction Position Sum",
	sum(click_conversion_value_sum) as "Click Revenue",
	sum(exposed_conversion_count) as "Exposed Conversions",
	sum(exposed_conversion_value_sum) as "Exposed Revenue"
from intent_media_production.hotel_ct_advertiser_performance_report_aggregations acapra
left join intent_media_production.campaigns on campaigns.id = acapra.campaign_id
left join intent_media_production.ad_groups on ad_groups.id = acapra.ad_group_id
left join intent_media_production.entities e on e.id = acapra.advertiser_id
where campaigns.id not in (4189,4190,4191,4192,4193)
and date_in_et < date(current_timestamp at timezone 'America/New_York')
group by
	date_in_et,
	e.name,
	e.id,
	campaigns.name,
	campaigns.tracking_code,
	ad_groups.name,
	(case when campaigns.paused = 0 then 'Active' else 'Paused' end)