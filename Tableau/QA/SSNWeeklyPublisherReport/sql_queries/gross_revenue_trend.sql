-- Gross Revenue Trend

SELECT
--	DATE AS Date,
	"Total" AS Market,
	"Total" AS Segment,
	"Total" AS Publisher,
	"Total" AS Site,
	sum(pages_available) AS `Pages Available`,
	sum(pages_served) AS `Pages Served`,
	sum(impressions_available) AS `Impressions Available`,
	sum(impressions_served) AS `Impressions Served`,
	sum(Clicks) as Clicks,
	sum(Spend) as Spend,
	sum(Spend*.8*.94) as `Net Media Revenue`, -- .8*.94 for OWW, .78*.94 for TVL (up to 1M), .79*.94 for 1-2M .8*94 for 2-3M
	sum(Conversion_value_sum) AS `Conversion Value`,
	sum(pages_served)/sum(pages_available) as `Fill Rate`,
	sum(Clicks)/sum(impressions_served) AS CTR,
	sum(Spend)/sum(Clicks) AS CPC,
	sum(impressions_served)/sum(pages_served) as `Ads per page`,
	sum(Clicks)/sum(pages_served) as `page CTR`,
	sum(Spend)/sum(pages_available) as `Available ecpm`,
	sum(Spend)/sum(pages_served) as `Served ecpm`
FROM hotel_ssr_daily_dashboard_datasets
WHERE dataset = "Pubs"
and Site = 'Orbitz'
-- and Site in ('Orbitz','CheapTickets','eBookers')
and date BETWEEN (SELECT DATE_ADD(MAX(date), INTERVAL -6 DAY) FROM hotel_ssr_daily_dashboard_datasets) 
AND (SELECT MAX(date) FROM hotel_ssr_daily_dashboard_datasets)
-- GROUP BY DATE
;

-- Gross Revenue Trend

SELECT
--	DATE AS Date,
	"Total" AS Market,
	"Total" AS Segment,
	"Total" AS Publisher,
	"Total" AS Site,
	sum(pages_available) AS `Pages Available`,
	sum(pages_served) AS `Pages Served`,
	sum(impressions_available) AS `Impressions Available`,
	sum(impressions_served) AS `Impressions Served`,
	sum(Clicks) as Clicks,
	sum(Spend) as Spend,
	sum(Spend*.8*.94) as `Net Media Revenue`, -- .8*.94 for OWW, .78*.94 for TVL (up to 1M), .79*.94 for 1-2M .8*94 for 2-3M
	sum(Conversion_value_sum) AS `Conversion Value`,
	sum(pages_served)/sum(pages_available) as `Fill Rate`,
	sum(Clicks)/sum(impressions_served) AS CTR,
	sum(Spend)/sum(Clicks) AS CPC,
	sum(impressions_served)/sum(pages_served) as `Ads per page`,
	sum(Clicks)/sum(pages_served) as `page CTR`,
	sum(Spend)/sum(pages_available) as `Available ecpm`,
	sum(Spend)/sum(pages_served) as `Served ecpm`
FROM hotel_ssr_daily_dashboard_datasets
WHERE dataset = "Pubs"
and Site = 'Orbitz'
-- and Site in ('Orbitz','CheapTickets','eBookers')
and date BETWEEN (SELECT DATE_ADD(MAX(date), INTERVAL -6 DAY) FROM hotel_ssr_daily_dashboard_datasets) 
AND (SELECT MAX(date) FROM hotel_ssr_daily_dashboard_datasets)
-- GROUP BY DATE
;

-- Gross Revenue Trend

SELECT
--	DATE AS Date,
	"Total" AS Market,
	"Total" AS Segment,
	"Total" AS Publisher,
	"Total" AS Site,
	sum(pages_available) AS `Pages Available`,
	sum(pages_served) AS `Pages Served`,
	sum(impressions_available) AS `Impressions Available`,
	sum(impressions_served) AS `Impressions Served`,
	sum(Clicks) as Clicks,
	sum(Spend) as Spend,
	sum(Spend*.8*.94) as `Net Media Revenue`, -- .8*.94 for OWW, .78*.94 for TVL (up to 1M) 
	sum(Conversion_value_sum) AS `Conversion Value`,
	sum(pages_served)/sum(pages_available) as `Fill Rate`,
	sum(Clicks)/sum(impressions_served) AS CTR,
	sum(Spend)/sum(Clicks) AS CPC,
	sum(impressions_served)/sum(pages_served) as `Ads per page`,
	sum(Clicks)/sum(pages_served) as `page CTR`,
	sum(Spend)/sum(pages_available) as `Available ecpm`,
	sum(Spend)/sum(pages_served) as `Served ecpm`
FROM hotel_ssr_daily_dashboard_datasets
WHERE dataset = "Pubs"
and Site = 'Orbitz'
and date BETWEEN ('2012-07-01') AND ('2012-07-15')
-- GROUP BY DATE
;

-- Gross Revenue Trend
-- These are the statistics for Key Performance Metrics by Market

SELECT
--	DATE AS Date,
	"Total" AS Market,
	"Total" AS Segment,
	"Total" AS Publisher,
	"Total" AS Site,
	sum(pages_available) AS `Pages Available`,
	sum(pages_served) AS `Pages Served`,
	sum(impressions_available) AS `Impressions Available`,
	sum(impressions_served) AS `Impressions Served`,
	sum(Clicks) as Clicks,
	sum(Spend) as Spend,
	sum(Spend*.78*.94) as `Net Media Revenue`, -- .8*.94 for OWW, .78*.94 for TVL (up to 1M) 
	sum(Conversion_value_sum) AS `Conversion Value`,
	sum(pages_served)/sum(pages_available) as `Fill Rate`,
	sum(Clicks)/sum(impressions_served) AS CTR,
	sum(Spend)/sum(Clicks) AS CPC,
	sum(impressions_served)/sum(pages_served) as `Ads per page`,
	sum(Clicks)/sum(pages_served) as `page CTR`,
	sum(Spend)/sum(pages_available) as `Available ecpm`,
	sum(Spend)/sum(pages_served) as `Served ecpm`
FROM hotel_ssr_daily_dashboard_datasets
WHERE dataset = "Pubs"
and Site = 'Travelocity'
and date BETWEEN ('2012-01-01') AND ('2012-07-15')
-- GROUP BY DATE
;


-- Gross Revenue Trend BY market

SELECT
--	DATE AS Date,
	Market,
	Segment,
	Publisher,
	Site,
	sum(pages_available) AS `Pages Available`,
	sum(pages_served) AS `Pages Served`,
	sum(impressions_available) AS `Impressions Available`,
	sum(impressions_served) AS `Impressions Served`,
	sum(Clicks) as Clicks,
	sum(Spend) as Spend,
	sum(Conversion_value_sum) AS `Conversion Value`,
	sum(pages_served)/sum(pages_available) as `Fill Rate`,
	sum(Clicks)/sum(impressions_served) AS CTR,
	sum(Spend)/sum(Clicks) AS CPC,
	sum(impressions_served)/sum(pages_served) as `Ads per page`,
	sum(Clicks)/sum(pages_served) as `page CTR`,
	sum(Spend)/sum(pages_available) as `Available ecpm`,
	sum(Spend)/sum(pages_served) as `Served ecpm`
FROM hotel_ssr_daily_dashboard_datasets
WHERE dataset = "Pubs"
--  and Site in ('Orbitz','CheapTickets','eBookers')
and Site = 'Travelocity'
and date BETWEEN (SELECT DATE_ADD(MAX(date), INTERVAL -6 DAY) FROM hotel_ssr_daily_dashboard_datasets) 
AND (SELECT MAX(date) FROM hotel_ssr_daily_dashboard_datasets)
GROUP BY Market
;
