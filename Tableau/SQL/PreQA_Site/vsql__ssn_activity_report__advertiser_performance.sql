select
	users."User Name",
	users."User Email",
	users."Is Primary User",
	users."Distinct Hotels",
	users."Strategic Account Type",
	users."Known Rotator Type",
	users."Phone Number",
	users."First Auction Participation",
	performance.*,
	
	CASE WHEN (performance."Click Room Nights Sum" is not null and performance."Click Room Nights Sum" !=0)
	THEN COALESCE(performance.Clicks,0.0)/performance."Click Room Nights Sum" 
	ELSE 0 end as "Clicks To Booked Room Night"  /* Added  by SR for Pivotal story: 83344234*/
	
from
(
		select
			entities_to_users.*,
			(case when entities_to_users."User ID" = primary_users.primary_user then 1 else 0 end) as "Is Primary User",
			hotel_count."Distinct Hotels",
			hotel_count."Strategic Account Type",
			hotel_count."Known Rotator Type"
		from
		(
				select
					e.id as "Advertiser ID",
					e.telephone as "Phone Number",
					(u.first_name || ' ' || u.last_name) as "User Name",
					u.email as "User Email",
					u.id as "User ID",
					e.first_auction_participation as "First Auction Participation"
				from intent_media_production.entities e
				inner join intent_media_production.memberships m on m.entity_id = e.id
				inner join intent_media_production.users u on u.id = m.user_id 
				where entity_type = 'HotelSsrAdvertiser'
					and e.active = 1
					and e.first_auction_participation is not null
					and m.active = 1
		) entities_to_users
		left join 
		(
			select 
				entity_id as entity_id, 
				min(user_id) as primary_user 
			from intent_media_production.memberships
			where active = 1
			group by 
				entity_id
		) primary_users 
		on entities_to_users."Advertiser ID" = primary_users.entity_id
		left join
		(
			select
				u.email "User Email",
				(case when u.strategic_account = 1 then 'Strategic Accounts' else 'Other Accounts' end) as "Strategic Account Type",
				(case when u.known_property_rotator = 1 then 'Known Rotators' else 'Other Accounts' end) as "Known Rotator Type",
				count(e.name) as "Distinct Hotels"
			from intent_media_production.users u
			left join intent_media_production.memberships m on m.user_id = u.id
			left join intent_media_production.entities e on e.id = m.entity_id
			where e.entity_type = 'HotelSsrAdvertiser'
				and e.active = 1
				and e.first_auction_participation is not null
				and m.active = 1
			group by
				u.email,
				u.strategic_account, 
				u.known_property_rotator
		) hotel_count
		on entities_to_users."User Email" = hotel_count."User Email"
) users
left join 
(
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
		min(atwra.click_count) as "Clicks",
		min(atwra.click_conversion_count) as "Click Conversions",
		min(atwra.actual_cpc_sum) as Spend,
		min(atwra.click_conversion_value_sum) as "Click Conversion Value Sum",
		min(atwra.exposed_conversion_count) as "Exposed Conversion Count",
		min(atwra.exposed_conversion_value_sum) as "Exposed Conversion Value Sum",
		min(atwra.click_room_nights_sum) as "Click Room Nights Sum",
		min(atwra.exposed_room_nights_sum) as "Exposed Room Nights Sum",
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
	(
		select
			date(aggregation_level at timezone 'UTC' at timezone 'America/New_York') as date_in_et,
			advertiser_id,
			advance_purchase_range_type,
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
		group by 
			date(aggregation_level at timezone 'UTC' at timezone 'America/New_York'), 
			advertiser_id, 
			advance_purchase_range_type
	) atwra
	on atwra.advertiser_id = isra.advertiser_id 
	and atwra.advance_purchase_range_type = isra.advance_purchase_range_type
	and  atwra.date_in_et = isra.aggregation_level_date_in_et
	where isra.aggregation_level_date_in_et < date(current_timestamp at timezone 'UTC' at timezone 'America/New_York')
		and e.active = 1
	group by 
		isra.aggregation_level_date_in_et, 
		e.name, 
		e.ssn_channel_type,
		e.last_auction_participation,
		isra.advertiser_id,
		hpa.hotel_property_id,
		imhpm.intent_media_market_id,
		ifnull(imm.report_segment, 'Other'),
		ifnull(imm.name , 'Other'),
		z.can_serve_ads,
		isra.advance_purchase_range_type
) performance
on users."Advertiser ID" = performance."Advertiser ID"