select
	dimensions.*,
	ag.name as "Ad Group",
	e.name as "Advertiser Name",
	e.id as "Advertiser ID",
	c.name as Campaign,
	c.id as "Campaign ID",
	c.tracking_code as "Tracking Code",
	(case when c.paused = 0 then 'Active' else 'Paused' end) as "Campaign Status",
	performance.Impressions,
	performance.Clicks,
	performance."Click Conversions",
	performance.Spend,
	performance."Auction Position Sum",
	performance."Click Revenue",
	performance."Exposed Conversions",
	performance."Exposed Revenue"

	
from
	
(select
	date_in_et as Date,
	all_ad_groups.id as "Ad Group ID",
	all_ad_groups."Product Category"
from
(select
	distinct(date_in_et) as date_in_et
from intent_media_production.air_ct_advertiser_performance_report_aggregations
where advertiser_id in (61224, 93063, 106574)
and date_in_et < date(current_timestamp at timezone 'America/New_York')) dates,
(select
	ag.id,
	(case
		when ag.id in (select 
							ad_group_id 
						from intent_media_production.air_ct_advertiser_performance_report_aggregations
						where advertiser_id in (61224, 93063, 106574))
			then 'Flights'
		when ag.id in (select 
							ad_group_id 
						from intent_media_production.hotel_ct_advertiser_performance_report_aggregations
						where advertiser_id in (61224, 93063, 106574))
			then 'Hotels'
		when ag.id in (select
							ad_group_id
						from intent_media_production.hotel_meta_advertiser_performance_aggregations
						where advertiser_id in (148708, 155752))
			then 'Meta'
	end) as "Product Category"
from intent_media_production.ad_groups ag
left join intent_media_production.campaigns c on c.id = ag.campaign_id
left join intent_media_production.entities e on e.id = c.advertiser_id
where c.deleted = 0
and e.id in (61224, 93063, 106574, 148708, 155752)) all_ad_groups) dimensions


left join

(select
	date_in_et as Date,
	ad_group_id as "Ad Group ID",
	sum(impression_count) as Impressions,
	sum(click_count) as Clicks,
	sum(actual_cpc_sum) as Spend,
	sum(auction_position_sum) as "Auction Position Sum",
	sum(click_conversion_count) as "Click Conversions",
	sum(click_conversion_value_sum) as "Click Revenue",
	sum(exposed_conversion_count) as "Exposed Conversions",
	sum(exposed_conversion_value_sum) as "Exposed Revenue"
from intent_media_production.air_ct_advertiser_performance_report_aggregations
where advertiser_id in (61224, 93063, 106574)
and date_in_et < date(current_timestamp at timezone 'America/New_York')
group by
	date_in_et,
	ad_group_id
	
union

select
	date_in_et as Date,
	ad_group_id as "Ad Group ID",
	sum(impression_count) as Impressions,
	sum(click_count) as Clicks,
	sum(actual_cpc_sum) as Spend,
	sum(auction_position_sum) as "Auction Position Sum",
	sum(click_conversion_count) as "Click Conversions",
	sum(click_conversion_value_sum) as "Click Revenue",
	sum(exposed_conversion_count) as "Exposed Conversions",
	sum(exposed_conversion_value_sum) as "Exposed Revenue"
from intent_media_production.hotel_ct_advertiser_performance_report_aggregations
where advertiser_id in (61224, 93063, 106574)
and date_in_et < date(current_timestamp at timezone 'America/New_York')
group by
	date_in_et,
	ad_group_id
	
union

select
	date_in_et as Date,
	ad_group_id as "Ad Group ID",
	sum(hmapa.impression_count) as Impressions,
	sum(hmapa.click_count) as Clicks,
	sum(hmapa.actual_cpc_sum) as Spend,
	sum(hmapa.auction_position_sum) as "Auction Position Sum",
	cast(null as int) as "Click Conversions",
	cast(null as int) as "Click Revenue",
	cast(null as int) as "Exposed Conversions",
	cast(null as int) as "Exposed Revenue"
from intent_media_production.hotel_meta_advertiser_performance_aggregations hmapa
where hmapa.date_in_et < date(current_timestamp at timezone 'America/New_York')
and advertiser_id in (148708, 155752)
group by
	date_in_et,
	ad_group_id	
	
	) performance
	
on dimensions.Date = performance.Date
and dimensions."Ad Group ID" = performance."Ad Group ID"

left join intent_media_production.ad_groups ag on ag.id = dimensions."Ad Group ID"
left join intent_media_production.campaigns c on c.id = ag.campaign_id
left join intent_media_production.entities e on e.id = c.advertiser_id

where dimensions."Product Category" is not null