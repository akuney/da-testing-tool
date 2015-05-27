select
	'Flights' as "Product Category Type",
	c.precheck_eligibility_type as "Precheck Type",
	e.name as "Advertiser",
	e.alternate_reporting_currency as "Local Currency",
	performance.*,
	revenue_metrics.Impressions,
	revenue_metrics.Clicks,
	revenue_metrics.Spend,
	revenue_metrics."Click Conversions",
	revenue_metrics."View Conversions",
	revenue_metrics."Click Revenue",
	revenue_metrics."Exposed Revenue",
	revenue_metrics."Auction Position Sum"
from
(
	select
		aggregation_level_date_in_et as Date,
		campaign_id as "Campaign ID",
		advertiser_id as "Advertiser ID",
		sum(impression_count + filtered_ad_count) as "Pages Served"
	from intent_media_production.air_ct_impression_share_report_aggregations
	group by
	  aggregation_level_date_in_et,
	  campaign_id,
	  advertiser_id
) performance
left join
(
	select
		acapra.date_in_et as Date,
		acapra.campaign_id as "Campaign ID",
		acapra.advertiser_id as "Advertiser ID",
		sum(acapra.impression_count) as Impressions,
		sum(acapra.click_count) as Clicks,
		sum(acapra.actual_cpc_sum) as Spend,
		sum(acapra.click_conversion_count) as "Click Conversions",
		sum(acapra.exposed_conversion_count) as "View Conversions",
		sum(acapra.click_conversion_value_sum) as "Click Revenue",
		sum(acapra.exposed_conversion_value_sum) as "Exposed Revenue",
		sum(acapra.auction_position_sum) as "Auction Position Sum"
	from intent_media_production.air_ct_advertiser_performance_report_aggregations acapra
	group by
	  acapra."date_in_et",
	  acapra.campaign_id,
	  acapra."advertiser_id"
) revenue_metrics
on performance.Date = revenue_metrics.Date
  and performance."Campaign ID" = revenue_metrics."Campaign ID"
  and performance."Advertiser ID" = revenue_metrics."Advertiser ID"
left join intent_media_production.entities e on performance."Advertiser ID" = e.id
left join intent_media_production.campaigns c on performance."Campaign ID" = c.id

union

select
	'Hotels' as "Product Category Type",
	c.precheck_eligibility_type as "Precheck Type",
	e.name as "Advertiser",
	e.alternate_reporting_currency as "Local Currency",
	performance.*,
	revenue_metrics.Impressions,
	revenue_metrics.Clicks,
	revenue_metrics.Spend,
	revenue_metrics."Click Conversions",
	revenue_metrics."View Conversions",
	revenue_metrics."Click Revenue",
	revenue_metrics."Exposed Revenue",
	revenue_metrics."Auction Position Sum"
from
(
	select
		aggregation_level_date_in_et as Date,
		campaign_id as "Campaign ID",
		advertiser_id as "Advertiser ID",
		sum(impression_count + filtered_ad_count) as "Pages Served"
	from intent_media_production.hotel_ct_impression_share_report_aggregations
	group by
	  aggregation_level_date_in_et,
	  campaign_id,
	  advertiser_id
) performance
left join
(
	select
		acapra.date_in_et as Date,
		acapra.campaign_id as "Campaign ID",
		acapra.advertiser_id as "Advertiser ID",
		sum(acapra.impression_count) as Impressions,
		sum(acapra.click_count) as Clicks,
		sum(acapra.actual_cpc_sum) as Spend,
		sum(acapra.click_conversion_count) as "Click Conversions",
		sum(acapra.exposed_conversion_count) as "View Conversions",
		sum(acapra.click_conversion_value_sum) as "Click Revenue",
		sum(acapra.exposed_conversion_value_sum) as "Exposed Revenue",
		sum(acapra.auction_position_sum) as "Auction Position Sum"
	from intent_media_production.hotel_ct_advertiser_performance_report_aggregations acapra
	group by
	  acapra."date_in_et",
	  acapra.campaign_id,
	  acapra."advertiser_id"
) revenue_metrics
on performance.Date = revenue_metrics.Date
  and performance."Campaign ID" = revenue_metrics."Campaign ID"
  and performance."Advertiser ID" = revenue_metrics."Advertiser ID"
left join intent_media_production.entities e on performance."Advertiser ID" = e.id
left join intent_media_production.campaigns c on performance."Campaign ID" = c.id

union

select
	'PPA' as "Product Category Type",
	campaigns.precheck_eligibility_type as "Precheck Type",
	entities.name as "Advertiser",
	entities.alternate_reporting_currency as "Local Currency",
	acapra.date_in_et as Date,
	campaigns.id as "Campaign ID",
	entities.id as "Advertiser ID",
	sum(acapra.filtered_ad_count + acapra.impression_count) as "Pages Served",
	sum(acapra.impression_count) as Impressions,
	sum(acapra.click_count) as Clicks,
	sum(acapra.actual_cpc_sum) as Spend,
	cast(null as int) as "Click Conversions",
	cast(null as int) as "View Conversions",
	cast(null as float) as "Click Revenue",
	cast(null as float) as "Exposed Revenue",
	sum(acapra.auction_position_sum) as "Auction Position Sum"
from intent_media_production.hotel_meta_advertiser_performance_aggregations acapra
left join intent_media_production.entities on acapra.advertiser_id = entities.id
left join intent_media_production.ad_groups on acapra.ad_group_id = ad_groups.id
left join intent_media_production.campaigns on ad_groups.campaign_id = campaigns.id
where acapra.date_in_et < date(current_timestamp at timezone 'America/New_York')
  and acapra.date_in_et > '2014-01-14'
group by
  campaigns.precheck_eligibility_type,
  entities.name,
  entities.alternate_reporting_currency,
  acapra.date_in_et,
  campaigns.id,
  entities.id