DROP TABLE IF EXISTS intent_media_sandbox_production.sp_adv_attrition_advertiser_performance_aggregations;
CREATE TABLE intent_media_sandbox_production.sp_adv_attrition_advertiser_performance_aggregations AS
select
		isra.aggregation_level_date_in_et as Date,
		e.ssn_channel_type as "SSN Channel Type",
		e.last_auction_participation as "Last Auction Participation",
		e.name as "Advertiser Name",
		isra.advertiser_id as "Advertiser ID",
		hpa.hotel_property_id as "Hotel Property ID",
		imhpm.intent_media_market_id as "Market ID",
		ifnull(imm.name , 'Other') as "Market Name",
		ifnull(imm.report_segment, 'Other') as "Segment Name",
		z.can_serve_ads as "Can Serve Ads",
		(case isra.advance_purchase_range_type
			when 'WEEKDAY_TRAVEL_LESS_THAN_OR_EQUAL_TO_21_DAYS' then 'Weekdays within 21 Days'
			when 'WEEKEND_TRAVEL_LESS_THAN_OR_EQUAL_TO_21_DAYS' then 'Weekends within 21 Days'
			when 'WEEKDAY_TRAVEL_GREATER_THAN_21_DAYS' then 'Weekdays 22+ Days Away'
			when 'WEEKEND_TRAVEL_GREATER_THAN_21_DAYS' then 'Weekends 22+ Days Away'
			when 'DATELESS' then 'Dateless'
			else isra.advance_purchase_range_type
		end) as "Travel Window",
		atwra.click_count as "Clicks",
		atwra.click_conversion_count as "Click Conversions",
		atwra.actual_cpc_sum as Spend,
		atwra.click_conversion_value_sum as "Click Conversion Value Sum",
		atwra.exposed_conversion_count as "Exposed Conversion Count",
		atwra.exposed_conversion_value_sum as "Exposed Conversion Value Sum",
		atwra.click_room_nights_sum as "Click Room Nights Sum",
		atwra.exposed_room_nights_sum as "Exposed Room Nights Sum",
		sum(isra.impression_count) as "Impressions",
		sum(isra.filtered_ad_count) as "Filtered Ads",
		sum(isra.filtered_ad_for_budget_count) as "Filtered Ads (Budget)",
		sum(isra.filtered_ad_for_bid_count) as "Filtered Ads (Bid)",
		sum(isra.filtered_ad_for_hotel_unavailable_count) as "Filtered Ads (Hotel Unavailable)"
	from intent_media_production.impression_share_report_aggregations isra
	left join intent_media_production.entities e on e.id = isra.advertiser_id
	left join intent_media_production.hotel_property_advertisers hpa on hpa.hotel_ssr_advertiser_id = e.id
	left join intent_media_production.intent_media_hotel_properties_markets imhpm on imhpm.hotel_property_id = hpa.hotel_property_id
	left join intent_media_production.intent_media_markets imm on imm.id = imhpm.intent_media_market_id
	left join intent_media_production.z_hotel_ssr_advertiser_status z on isra.advertiser_id = z.advertiser_id
	left join
		(select
			date(aggregation_level at timezone 'UTC' at timezone 'America/New_York') as date_in_et,
			advertiser_id,
			advance_purchase_range_type,
			sum(impression_count) as impression_count,
			sum(click_count) as click_count,
			sum(click_conversion_count) as click_conversion_count,
			sum(actual_cpc_sum) as actual_cpc_sum,
			sum(click_conversion_value_sum) as click_conversion_value_sum,
			sum(exposed_conversion_count) as exposed_conversion_count,
			sum(exposed_conversion_value_sum) as exposed_conversion_value_sum,
			sum(click_room_nights_sum) as click_room_nights_sum,
			sum(exposed_room_nights_sum) as exposed_room_nights_sum
		from intent_media_production.advertiser_travel_window_report_aggregations
		where date(aggregation_level at timezone 'UTC' at timezone 'America/New_York') <  date(current_timestamp at timezone 'UTC' at timezone 'America/New_York')
		group by date(aggregation_level at timezone 'UTC' at timezone 'America/New_York'), advertiser_id, advance_purchase_range_type) atwra
		on atwra.advertiser_id = isra.advertiser_id
			and atwra.advance_purchase_range_type = isra.advance_purchase_range_type
			and  atwra.date_in_et = isra.aggregation_level_date_in_et
	where isra.aggregation_level_date_in_et < date(current_timestamp at timezone 'UTC' at timezone 'America/New_York')
		and e.active = 1
	group by isra.aggregation_level_date_in_et,
		e.name,
		e.ssn_channel_type,
		e.last_auction_participation,
		isra.advertiser_id,
		hpa.hotel_property_id,
		imhpm.intent_media_market_id,
		ifnull(imm.report_segment, 'Other'),
		ifnull(imm.name , 'Other'),
		z.can_serve_ads,
		isra.advance_purchase_range_type,
		atwra.impression_count,
		atwra.click_count,
		atwra.click_conversion_count,
		atwra.actual_cpc_sum,
		atwra.click_conversion_value_sum,
		atwra.exposed_conversion_count,
		atwra.exposed_conversion_value_sum,
		atwra.click_room_nights_sum,
		atwra.exposed_room_nights_sum;


DROP TABLE IF EXISTS intent_media_sandbox_production.sp_adv_attrition_advertiser_performance_by_month;
CREATE TABLE intent_media_sandbox_production.sp_adv_attrition_advertiser_performance_by_month AS
SELECT
  MONTH(Date) as month,
  YEAR(Date) as year,
  "Advertiser ID" as advertiser_id,
  sum("Impressions") as impressions,
  ifnull(sum("Clicks"), 0) as clicks,
  ifnull(sum("Click Conversions"), 0) as clicked_conversions,
  ifnull(sum("Exposed Conversion Count"), 0) as exposed_conversions,
  ifnull(sum("Click Room Nights Sum"), 0) as clicked_room_nights,
  ifnull(sum("Exposed Room Nights Sum"), 0) as exposed_room_nights,
  ifnull(sum("Click Conversion Value Sum"), 0) as clicked_conversion_value,
  ifnull(sum("Exposed Conversion Value Sum"), 0) as exposed_conversion_value,
  ifnull(sum(Spend), 0) as spend,
  ifnull(sum("Clicks"), 0)/(sum("Impressions")+1) as ctr_smoothed,
  ifnull(sum("Click Conversion Value Sum"), 0)/(ifnull(sum(Spend), 0) + 1) as roi_smoothed
  FROM intent_media_sandbox_production.sp_adv_attrition_advertiser_performance_aggregations
GROUP BY 1,2,3;

DROP TABLE IF EXISTS intent_media_sandbox_production.sp_adv_attrition_advertiser_performance_comp_prior_month;
CREATE TABLE intent_media_sandbox_production.sp_adv_attrition_advertiser_performance_comp_prior_month AS
SELECT
  t2.*,
  t2.impressions/(t1.impressions+1) as impression_ratio_smoothed,
  t2.clicks/(t1.clicks+1) as click_ratio_smoothed,
  t2.clicked_conversions/(t1.clicked_conversions+1) as clicked_conversion_ratio_smoothed,
  t2.spend/(t1.spend+1) as spend_ratio_smoothed,
  t2.ctr_smoothed/(t1.ctr_smoothed+1) as ctr_ratio_smoothed,
  t2.roi_smoothed/(t1.roi_smoothed+1) as roi_ratio_smoothed
FROM intent_media_sandbox_production.sp_adv_attrition_advertiser_performance_by_month t2
JOIN intent_media_sandbox_production.sp_adv_attrition_advertiser_performance_by_month t1
ON t1.advertiser_id=t2.advertiser_id AND ((t1.month = t2.month - 1 AND t1.year = t2.year)
                                 OR (t1.month = t2.month + 11 AND t1.year = t2.year - 1));