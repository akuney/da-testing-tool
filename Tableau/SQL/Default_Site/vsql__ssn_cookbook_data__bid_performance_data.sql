select
	atwra."Date",
	atwra."Advertiser ID",
	atwra."Travel Window",
	atwra."Impressions",
	atwra."Clicks",
	atwra."Click Conversions",
	atwra."Spend",
	atwra."Auction Position Sum",
	atwra."Click Revenue",
	atwra."Exposed Conversions",
	atwra."Exposed Revenue",
	atwra."Click Room Nights",
	atwra."Exposed Room Nights",
	isra."Filtered Total",
	isra."Filtered for Budget",
	isra."Filtered for Bid",
	isra."Filtered for Hotel Unavailable",
	hpa.hotel_property_id as "Hotel Property ID"
from (select
		date(aggregation_level) as "Date",
		advertiser_id as "Advertiser ID",
		(case advance_purchase_range_type
			when 'WEEKDAY_TRAVEL_LESS_THAN_OR_EQUAL_TO_21_DAYS' then 'Weekdays within 21 Days'
			when 'WEEKEND_TRAVEL_LESS_THAN_OR_EQUAL_TO_21_DAYS' then 'Weekends within 21 Days'
			when 'WEEKDAY_TRAVEL_GREATER_THAN_21_DAYS' then 'Weekdays 22+ Days Away'
			when 'WEEKEND_TRAVEL_GREATER_THAN_21_DAYS' then 'Weekends 22+ Days Away'
			when 'DATELESS' then 'Dateless'
			else advance_purchase_range_type
		end) as "Travel Window",
		sum(impression_count) as Impressions,
		sum(click_count) as Clicks,
		sum(click_conversion_count) as "Click Conversions",
		sum(actual_cpc_sum) as Spend,
		sum(auction_position_sum) as "Auction Position Sum",
		sum(click_conversion_value_sum) as "Click Revenue",
		sum(exposed_conversion_count) as "Exposed Conversions",
		sum(exposed_conversion_value_sum) as "Exposed Revenue",
		sum(click_room_nights_sum) as "Click Room Nights",
		sum(exposed_room_nights_sum) as "Exposed Room Nights"
	from intent_media_production.advertiser_travel_window_report_aggregations
	group by date(aggregation_level), advertiser_id, advance_purchase_range_type
	) atwra
left join (select
		date(aggregation_level) as "Date",
		advertiser_id AS "Advertiser ID",
		(case advance_purchase_range_type
			when 'WEEKDAY_TRAVEL_LESS_THAN_OR_EQUAL_TO_21_DAYS' then 'Weekdays within 21 Days'
			when 'WEEKEND_TRAVEL_LESS_THAN_OR_EQUAL_TO_21_DAYS' then 'Weekends within 21 Days'
			when 'WEEKDAY_TRAVEL_GREATER_THAN_21_DAYS' then 'Weekdays 22+ Days Away'
			when 'WEEKEND_TRAVEL_GREATER_THAN_21_DAYS' then 'Weekends 22+ Days Away'
			when 'DATELESS' then 'Dateless'
			else advance_purchase_range_type
		end) as "Travel Window",
		sum(filtered_ad_count) as "Filtered Total",
		sum(filtered_ad_for_budget_count) as "Filtered for Budget",
		sum(filtered_ad_for_bid_count) as "Filtered for Bid",
		sum(filtered_ad_for_hotel_unavailable_count) as "Filtered for Hotel Unavailable"
		from intent_media_production.impression_share_report_aggregations
		group by date(aggregation_level), advertiser_id, advance_purchase_range_type
		) isra
on (isra."Date"=atwra."Date" and isra."Advertiser ID"=atwra."Advertiser ID" and isra."Travel Window"=atwra."Travel Window")
left join intent_media_production.hotel_property_advertisers hpa on hpa.hotel_ssr_advertiser_id = isra."Advertiser ID"