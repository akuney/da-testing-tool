select
	'Flights' as "Product Category",
	acappa.date_in_et as Date,
	ad_units.name as "Ad Unit",
	entities.name as "Advertiser Name",
	ad_groups.name as "Ad Group",
	campaigns.name as Campaign,
	campaigns.display_format as "Display Type",
	ad_groups.product_category_type AS "Product Category Type",
	acappa.auction_position as "Auction Position",
	sum(impression_count) as "Impressions",
	sum(click_count) as Clicks,
	sum(click_conversion_count) as "Conversions",
	sum(actual_cpc_sum) as Cost,
	sum(click_conversion_value_sum) as "Revenue",
	sum(exposed_conversion_count) as "Exposed Conversions",
	sum(exposed_conversion_value_sum) as "Exposed Revenue",
	campaigns.precheck_eligibility_type as "Precheck Type"
from intent_media_production.air_ct_auction_position_performance_aggregations acappa
left join intent_media_production.ad_units on ad_units.id = acappa.ad_unit_id
left join intent_media_production.entities on entities.id = acappa.advertiser_id
left join intent_media_production.ad_groups on ad_groups.id = acappa.ad_group_id
left join intent_media_production.campaigns on campaigns.id = acappa.campaign_id
where acappa.date_in_et < date(current_timestamp at timezone 'America/New_York')
group by
  acappa.date_in_et,
  ad_units.name,
  entities.name,
  ad_groups.name,
  campaigns.name,
  campaigns.display_format,
  ad_groups.product_category_type,
  acappa.auction_position,
  campaigns.precheck_eligibility_type

union

select
	'Hotels' as "Product Category",
	happa.date_in_et as Date,
	ad_units.name as "Ad Unit",
	entities.name as "Advertiser Name",
	ad_groups.name as "Ad Group",
	campaigns.name as Campaign,
	campaigns.display_format as "Display Type",
	ad_groups.product_category_type AS "Product Category Type",
	happa.auction_position as "Auction Position",
	sum(impression_count) as "Impressions",
	sum(click_count) as Clicks,
	sum(click_conversion_count) as "Conversions",
	sum(actual_cpc_sum) as Cost,
	sum(click_conversion_value_sum) as "Revenue",
	sum(exposed_conversion_count) as "Exposed Conversions",
	sum(exposed_conversion_value_sum) as "Exposed Revenue",
	campaigns.precheck_eligibility_type as "Precheck Type"
from intent_media_production.hotel_ct_auction_position_performance_aggregations happa
left join intent_media_production.ad_units on ad_units.id = happa.ad_unit_id
left join intent_media_production.entities on entities.id = happa.advertiser_id
left join intent_media_production.ad_groups on ad_groups.id = happa.ad_group_id
left join intent_media_production.campaigns on campaigns.id = happa.campaign_id
where happa.date_in_et < date(current_timestamp at timezone 'America/New_York')
group by
  happa.date_in_et,
  ad_units.name,
  entities.name,
  ad_groups.name,
  campaigns.name,
  campaigns.display_format,
  ad_groups.product_category_type,
  happa.auction_position,
  campaigns.precheck_eligibility_type

