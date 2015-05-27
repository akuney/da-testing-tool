SELECT
	it.ad_group_id AS `Ad Group ID`,
	it.tracking_code AS `Tracking Code`,
	it.tracking_code_2 AS `Tracking Code 2`,
	it.desktop_destination_url_override AS `URL Override`,
	fodp.origination_airport_code_id AS `Origination Airport Code ID`,
	fodp.destination_airport_code_id AS `Destination Airport Code ID`,
	fodp.origination_air_ct_favorite_id AS `Origination Favorite ID`,
	fodp.destination_air_ct_favorite_id AS `Destination Favorite ID`,
	hci.name AS City,
	hci.state AS State,
	CASE WHEN hco_hci.name IS NULL AND hco.name IS NULL THEN NULL ELSE concat(ifnull(hco_hci.name,''), ifnull(hco.name,'')) END AS Country
FROM intent_targets it
LEFT JOIN intent_media_production.flight_origination_destination_pairs fodp ON fodp.id = it.intent_id AND it.intent_type = 'FlightOriginationDestinationPair' 
LEFT JOIN hotel_cities hci ON hci.id = it.intent_id AND it.intent_type = 'HotelCity'
LEFT JOIN countries hco ON hco.id = it.intent_id AND it.intent_type = 'Country'
LEFT JOIN countries hco_hci ON hco_hci.id = hci.country_id
WHERE it.paused = 0
	AND it.intent_type IN ('FlightOriginationDestinationPair','HotelCity', 'Country')
	AND it.ad_group_id IN (16322, 16932, 17843, 21542, 21543, 21544, 21545, 21547, 21548)