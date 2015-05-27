select
	entities.name as "Advertiser Name",
	'Flights' as "Product Category Type",
	ad_groups.name as "Ad Group",
	campaigns.name as "Campaign",
	acisra.aggregation_level_date_in_et as Date,
	sum(acisra.impression_count) as Impressions,
	sum(acisra.filtered_ad_count) + sum(acisra.impression_count) as "Eligible Ad Count"
from intent_media_production.air_ct_impression_share_report_aggregations acisra
left join intent_media_production.entities on acisra.advertiser_id = entities.id
left join intent_media_production.ad_groups on acisra.ad_group_id = ad_groups.id
left join intent_media_production.campaigns on ad_groups.campaign_id = campaigns.id
where aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York')
and acisra.advertiser_id in (61224, 93063, 106574)
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
	hcisra.aggregation_level_date_in_et as Date,
	sum(hcisra.impression_count) as Impressions,
	sum(hcisra.filtered_ad_count) + sum(hcisra.impression_count) as "Eligible Ad Count"
from intent_media_production.hotel_ct_impression_share_report_aggregations hcisra
left join intent_media_production.entities on hcisra.advertiser_id = entities.id
left join intent_media_production.ad_groups on hcisra.ad_group_id = ad_groups.id
left join intent_media_production.campaigns on ad_groups.campaign_id = campaigns.id
where aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York')
and hcisra.advertiser_id in (61224, 93063, 106574)
group by
	entities.name,
	ad_groups.name,
	campaigns.name,
	hcisra.aggregation_level_date_in_et
	
	
union 

select
	entities.name as "Advertiser Name",
	'Meta' as "Product Category Type",
	ad_groups.name as "Ad Group",
	campaigns.name as "Campaign",
	date_in_et as Date,
	sum(hmapa.impression_count) as Impressions,
	sum(hmapa.filtered_ad_count) + sum(hmapa.impression_count) as "Eligible Ad Count"
from intent_media_production.hotel_meta_advertiser_performance_aggregations hmapa
left join intent_media_production.entities on hmapa.advertiser_id = entities.id
left join intent_media_production.ad_groups on hmapa.ad_group_id = ad_groups.id
left join intent_media_production.campaigns on ad_groups.campaign_id = campaigns.id
where hmapa.date_in_et < date(current_timestamp at timezone 'America/New_York')
and hmapa.advertiser_id in (148708, 155752)
group by
	entities.name,
	ad_groups.name,
	campaigns.name,
	date_in_et