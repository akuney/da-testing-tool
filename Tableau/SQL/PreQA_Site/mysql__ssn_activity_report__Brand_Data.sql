SELECT
    e.id AS 'Advertiser ID',
    e.name as Advertiser_Name,
    hpa.active,
    hp.name as Hotel_Name,
    br.brand_name,
    br.chain_name
FROM
    intent_media_production.entities e
LEFT JOIN
    intent_media_production.hotel_property_advertisers hpa
ON
    e.id = hpa.hotel_ssr_advertiser_id
LEFT JOIN
    intent_media_production.hotel_properties hp
ON
    hp.id = hpa.hotel_property_id
LEFT JOIN
    intent_media_production.brands br
ON
    br.id = hp.brand_id
WHERE
    e.entity_type = 'HotelSsrAdvertiser'
AND e.active =1
AND hpa.active =1;
    
    

    

  
  

	

 
