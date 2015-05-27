-- Total individual sites
-- these queries are for the active hotels statistics on the Key Performance Metrics by Market tab
SELECT
		d.max_date as week_ending_date,
		(CASE participating_advertisers.site 
		   WHEN 'ORBITZ_CLASSIC' THEN 'Orbitz'
		   WHEN 'ORBITZ_GLOBAL' THEN 'Orbitz'
		   WHEN 'CHEAPTICKETS' THEN 'CheapTickets'
		   WHEN 'EBOOKERS' THEN 'ebookers'
		   WHEN 'TRAVELOCITY' THEN 'Travelocity'
		   ELSE participating_advertisers.site
		END) AS site,
		intent_media_markets.name as market,
		COUNT(DISTINCT participating_advertisers.advertiser_id, participating_advertisers.site, intent_media_markets.id) AS active_hotels
	FROM (SELECT MAX(date) AS max_date FROM hotel_ssr_daily_dashboard_datasets) d, participating_advertisers
	LEFT JOIN intent_media_markets_publisher_markets
		ON participating_advertisers.market_id = intent_media_markets_publisher_markets.market_id
	LEFT JOIN intent_media_markets
		ON intent_media_markets_publisher_markets.intent_media_market_id = intent_media_markets.id
	WHERE aggregation_level_date_in_et 
		BETWEEN (SELECT DATE_ADD(MAX(date), INTERVAL -6 DAY) FROM hotel_ssr_daily_dashboard_datasets) 
		AND (SELECT MAX(date) FROM hotel_ssr_daily_dashboard_datasets) 
	GROUP BY participating_advertisers.site, intent_media_markets.id
	UNION
	-- A. ii. b. pull data grouped by site and totaled over market
	SELECT
		d.max_date as week_ending_date,
		(CASE participating_advertisers.site 
		   WHEN 'ORBITZ_CLASSIC' THEN 'Orbitz'
		   WHEN 'ORBITZ_GLOBAL' THEN 'Orbitz'
		   WHEN 'CHEAPTICKETS' THEN 'CheapTickets'
		   WHEN 'EBOOKERS' THEN 'ebookers'
		   WHEN 'TRAVELOCITY' THEN 'Travelocity'
		   ELSE participating_advertisers.site
		END) AS site,
		"Total" as market,
		COUNT(DISTINCT participating_advertisers.advertiser_id, participating_advertisers.site) AS active_hotels
	FROM (SELECT MAX(date) AS max_date FROM hotel_ssr_daily_dashboard_datasets) d, participating_advertisers
	WHERE aggregation_level_date_in_et 
		BETWEEN (SELECT DATE_ADD(MAX(date), INTERVAL -6 DAY) FROM hotel_ssr_daily_dashboard_datasets) 
		AND (SELECT MAX(date) FROM hotel_ssr_daily_dashboard_datasets) 
	GROUP BY participating_advertisers.site;

-- OWW weekly total
SELECT
		d.max_date as week_ending_date,
		"OWW" AS site,
		intent_media_markets.name as market,
		COUNT(DISTINCT participating_advertisers.advertiser_id,intent_media_markets.id) AS active_hotels
	FROM (SELECT MAX(date) AS max_date FROM hotel_ssr_daily_dashboard_datasets) d, participating_advertisers
	LEFT JOIN intent_media_markets_publisher_markets
		ON participating_advertisers.market_id = intent_media_markets_publisher_markets.market_id
	LEFT JOIN intent_media_markets
		ON intent_media_markets_publisher_markets.intent_media_market_id = intent_media_markets.id
	WHERE 
		participating_advertisers.site IN ('ORBITZ_CLASSIC','ORBITZ_GLOBAL','CHEAPTICKETS','EBOOKERS')
		AND aggregation_level_date_in_et 
			BETWEEN (SELECT DATE_ADD(MAX(date), INTERVAL -6 DAY) FROM hotel_ssr_daily_dashboard_datasets) 
			AND (SELECT MAX(date) FROM hotel_ssr_daily_dashboard_datasets) 
	GROUP BY intent_media_markets.id
	UNION
	-- B. ii. b. pull data for OWW totaled over market	
	SELECT
		d.max_date as week_ending_date,
		"OWW" AS site,
		"Total" as market,
		COUNT(DISTINCT participating_advertisers.advertiser_id) AS active_hotels
	FROM (SELECT MAX(date) AS max_date FROM hotel_ssr_daily_dashboard_datasets) d, participating_advertisers
	WHERE 
		participating_advertisers.site IN ('ORBITZ_CLASSIC','ORBITZ_GLOBAL','CHEAPTICKETS','EBOOKERS')
		AND aggregation_level_date_in_et 
			BETWEEN (SELECT DATE_ADD(MAX(date), INTERVAL -6 DAY) FROM hotel_ssr_daily_dashboard_datasets) 
			AND (SELECT MAX(date) FROM hotel_ssr_daily_dashboard_datasets) 
	GROUP BY d.max_date;

-- TOTAL

SELECT
		d.max_date as week_ending_date,
		"Total" AS site,
		intent_media_markets.name as market,
		COUNT(DISTINCT participating_advertisers.advertiser_id,intent_media_markets.id) AS active_hotels
	FROM (SELECT MAX(date) AS max_date FROM hotel_ssr_daily_dashboard_datasets) d, participating_advertisers
	LEFT JOIN intent_media_markets_publisher_markets
		ON participating_advertisers.market_id = intent_media_markets_publisher_markets.market_id
	LEFT JOIN intent_media_markets
		ON intent_media_markets_publisher_markets.intent_media_market_id = intent_media_markets.id
	WHERE aggregation_level_date_in_et 
			BETWEEN (SELECT DATE_ADD(MAX(date), INTERVAL -6 DAY) FROM hotel_ssr_daily_dashboard_datasets) 
			AND (SELECT MAX(date) FROM hotel_ssr_daily_dashboard_datasets) 
	GROUP BY intent_media_markets.id
	UNION
	-- C. ii. b. pull data totaled over site and market
	SELECT
		d.max_date as week_ending_date,
		"Total" AS site,
		"Total" as market,
		COUNT(DISTINCT participating_advertisers.advertiser_id) AS active_hotels
	FROM (SELECT MAX(date) AS max_date FROM hotel_ssr_daily_dashboard_datasets) d, participating_advertisers
	WHERE aggregation_level_date_in_et 
			BETWEEN (SELECT DATE_ADD(MAX(date), INTERVAL -6 DAY) FROM hotel_ssr_daily_dashboard_datasets) 
			AND (SELECT MAX(date) FROM hotel_ssr_daily_dashboard_datasets)
	GROUP BY d.max_date;	
