--  This has the Active and Sold hotels for the top of the Gross Revenue page.

SELECT active_per_day.*, sold_per_day.`Sold Hotels` FROM
  (SELECT
    date_in_et AS Date,
    SUM(is_active) AS `Active Hotels`
    FROM (
      SELECT
        DISTINCT participating_advertisers.advertiser_id, aggregation_level_date_in_et AS date_in_et,
        1 AS is_active
      FROM participating_advertisers
      WHERE aggregation_level_date_in_et >= '2011-01-01' AND aggregation_level_date_in_et < DATE(NOW()))
      
      AS active_hotels 
   GROUP BY date_in_et
    HAVING SUM(is_active) IS NOT NULL) AS active_per_day
JOIN
  (SELECT agg_date AS Date, SUM(sold) AS `Sold Hotels` FROM
    (SELECT
      e.id AS advertiser_id,
      DATE(CONVERT_TZ(e.first_auction_participation, 'UTC', 'America/New_York')) AS sold_date,
      IF(e.active = 1 AND e.first_auction_participation IS NOT NULL,1,0) AS sold
    FROM entities e where entity_type="HotelSsrAdvertiser") AS advertiser_hotel_properties
   RIGHT JOIN
     (SELECT DISTINCT aggregation_level_date_in_et AS agg_date 
     FROM advertiser_account_report_aggregations) Dates ON Dates.agg_date >= advertiser_hotel_properties.sold_date
     GROUP BY agg_date) AS sold_per_day
ON active_per_day.Date = sold_per_day.Date
