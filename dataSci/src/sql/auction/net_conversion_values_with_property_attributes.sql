-- Last query requires access to intent_media_sandbox_production.sp_publisher_hotel_properties,
--   a table that is a copy of publishers_hotel_properties from MySQL.

DROP TABLE IF EXISTS intent_media_sandbox_production.AUCSIM_ppa_auction_value;
CREATE TABLE intent_media_sandbox_production.AUCSIM_ppa_auction_value AS
  SELECT net_conversion_value, 
         hotel_property_id, 
         site_type, 
         entity_id, 
         CASE 
                WHEN advance_purchase_range_type LIKE '%WEEKEND%' THEN 1 
                ELSE 0 
         END AS weekend_travel, 
         CASE 
                WHEN advance_purchase_range_type LIKE '%LESS%' THEN 1 
                ELSE 0 
         END AS upcoming, 
         CASE WHEN market_id IS NULL THEN 0 ELSE market_id 
         END AS market_id 
  FROM   intent_media_log_data_production.conversions 
  WHERE  product_category_type = 'HOTELS' 
  AND    requested_at_date_in_et BETWEEN '2014-10-01' AND    '2014-10-31' 
  AND    net_conversion_value IS NOT NULL 
  AND    hotel_property_id IS NOT NULL 
  AND    rooms=1;


DROP TABLE IF EXISTS intent_media_sandbox_production.AUCSIM_ppa_auction_value_brand;
CREATE TABLE intent_media_sandbox_production.AUCSIM_ppa_auction_value_brand AS
  SELECT    v.*, 
            CASE WHEN p.brand_id IS NULL THEN 0 ELSE p.brand_id 
            END AS brand_id 
  FROM      intent_media_sandbox_production.AUCSIM_ppa_auction_value v 
  LEFT JOIN intent_media_production.hotel_properties p 
  ON        v.hotel_property_id = p.id;

SELECT    b.*, 
          s.star_rating 
FROM      intent_media_sandbox_production.AUCSIM_ppa_auction_value_brand b 
LEFT JOIN intent_media_sandbox_production.sp_publisher_hotel_properties s 
ON        b.hotel_property_id = s.hotel_property_id 
AND       b.entity_id = s.publisher_id;