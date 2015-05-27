SELECT
	e.id AS `Advertiser ID`,
	e.name AS Advertiser,
	z.can_serve_ads AS `Currently Active`,
	e.first_auction_participation AS `First Auction Participation`,
	e.last_auction_participation AS `Latest Auction Participation`,
	c.budget_type AS `Budget Type`,
	c.daily_budget AS `Daily Budget`,
	u.email AS `Email`,
	u.first_name AS `First Name`,
	u.last_name AS `Last Name`
FROM entities e
JOIN campaigns c ON e.id = c.advertiser_id
JOIN users u ON e.created_by_user_id = u.id
JOIN z_hotel_ssr_advertiser_status z ON z.advertiser_id = e.id
WHERE e.entity_type = "HotelSsrAdvertiser" AND e.first_auction_participation IS NOT NULL