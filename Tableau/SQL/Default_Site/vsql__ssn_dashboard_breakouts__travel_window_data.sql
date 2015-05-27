SELECT
	DATE(t.aggregation_level at timezone 'America/New_York') AS Date,
	t.advertiser_id AS "Advertiser ID",
	t.advance_purchase_range_type AS "Advanced Purchase Range Type",
	sum(impression_count) AS "Impression Count",
	sum(click_count) AS "Clicks",
	sum(click_conversion_count) As "Clicked Conversions",
	sum(actual_cpc_sum) AS "Spend",
	sum(auction_position_sum) AS "Auction Position Sum",
	sum(click_conversion_value_sum) AS "Clicked Conversion Value Sum",
	sum(click_room_nights_sum) AS "Clicked Room Nights",
	sum(exposed_conversion_count) AS "Exposed Conversion",
	sum(exposed_conversion_value_sum) AS "Exposed Conversion Value Sum",
	sum(exposed_room_nights_sum) AS "Exposed Room Nights",
	entities.name AS Advertiser,
	u.email "User Email",
	u.first_name AS "First Name",
	u.last_name AS "Last Name"
FROM 
(
select
    advertiser_id,
    aggregation_level,
	advance_purchase_range_type,
	ad_unit_id,
	impression_count,
	click_count,
	click_conversion_count ,
	actual_cpc_sum ,
	auction_position_sum,
	click_conversion_value_sum,
	click_room_nights_sum ,
	exposed_conversion_count,
	exposed_conversion_value_sum,
	exposed_room_nights_sum 
	from intent_media_production.advertiser_travel_window_report_aggregations
	where aggregation_level >= DATE((CURRENT_DATE - interval '30 day') at timezone 'UTC')
    AND aggregation_level < DATE(CURRENT_DATE at timezone 'UTC')
) t
LEFT JOIN intent_media_production.entities ON entities.id = t.advertiser_id
LEFT JOIN intent_media_production.users u on u.id = entities.created_by_user_id
GROUP BY advertiser_id, Advertiser, "User Email", "First Name", "Last Name", DATE(t.aggregation_level at timezone 'America/New_York'), advance_purchase_range_type