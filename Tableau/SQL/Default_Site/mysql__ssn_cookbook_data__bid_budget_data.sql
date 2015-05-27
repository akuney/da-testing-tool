select 
	c.advertiser_id as `Advertiser ID`, 
	e.name as `Advertiser Name`,
	concat(u.first_name," ",u.last_name) as `User Name`,
	u.email as `User Email`,
	z.can_serve_ads as `Can Serve Ads`, 
	(case ci.value
		when 'WEEKDAY_TRAVEL_LESS_THAN_OR_EQUAL_TO_21_DAYS' then 'Weekdays within 21 Days'
		when 'WEEKEND_TRAVEL_LESS_THAN_OR_EQUAL_TO_21_DAYS' then 'Weekends within 21 Days'
		when 'WEEKDAY_TRAVEL_GREATER_THAN_21_DAYS' then 'Weekdays 22+ Days Away'
		when 'WEEKEND_TRAVEL_GREATER_THAN_21_DAYS' then 'Weekends 22+ Days Away'
		when 'DATELESS' then 'Dateless'
		else ci.value
	end) as `Travel Window`,
	it.bid_override as Bid, 
	c.budget_type as `Budget Type`, 
	(case c.daily_budget 
		when 'DAILY' then 'Daily'
		when 'MONTHLY' then 'Monthly'
		else c.daily_budget
	end) as `Daily Budget`,
	c.available_asap_budget as `Available ASAP Budget`
from intent_targets it 
join ad_groups ag on ag.id = it.ad_group_id
join campaigns c on c.id = ag.campaign_id
join comparable_intents ci on ci.id = it.intent_id
join entities e on e.id = c.advertiser_id
join users u on u.id = e.created_by_user_id
join `z_hotel_ssr_advertiser_status` z on z.advertiser_id = c.advertiser_id
where it.intent_id in (1,2,3,4,5)