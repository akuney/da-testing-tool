SELECT php_hp.id           AS hotel_property_id, 
       php_hp.publisher_id AS publisher_id, 
       Max(CASE 
             WHEN php_hp.star_rating IS NULL THEN 0 
             ELSE php_hp.star_rating 
           END)            AS star_rating, 
       Max(CASE 
             WHEN php_hp.brand_id IS NULL THEN 0 
             ELSE php_hp.brand_id 
           END)            AS brand_id, 
       Max(CASE 
             WHEN imhpm.intent_media_market_id IS NULL THEN 0 
             ELSE imhpm.intent_media_market_id 
           END)            AS market_id 
FROM   (SELECT hp.id, 
               php.publisher_id, 
               php.star_rating, 
               hp.brand_id 
        FROM   intent_media_sandbox_production.sp_publisher_hotel_properties php 
               LEFT JOIN intent_media_production.hotel_properties hp 
                      ON php.hotel_property_id = hp.id) php_hp 
       LEFT JOIN intent_media_production.intent_media_hotel_properties_markets 
                 imhpm 
              ON php_hp.id = imhpm.hotel_property_id 
WHERE  php_hp.id IS NOT NULL
GROUP  BY php_hp.id, 
          php_hp.publisher_id
ORDER BY php_hp.id,
         php_hp.publisher_id; 
