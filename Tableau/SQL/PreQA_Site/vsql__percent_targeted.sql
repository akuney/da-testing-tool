select
	date_in_et as Date,
	advertiser_id as "Advertiser ID",
	e.name as "Advertiser",
	sum(served_ad_call_count) as "Served Ad Calls",
	sum(impression_count) as Impressions,
	sum(filtered_ad_count) as "Filtered Ad Count"
from intent_media_production.air_ct_advertiser_od_opportunity_aggregations acaooa
left join intent_media_production.entities e on e.id = acaooa.advertiser_id
where date_in_et >= (date(current_timestamp at timezone 'America/New_York') - interval '30 days')
	and date_in_et < date(current_timestamp at timezone 'America/New_York')
        and e.active = 1
        and e.entity_type = 'AftAdvertiser'
group by
	date_in_et,
	advertiser_id,
	e.name