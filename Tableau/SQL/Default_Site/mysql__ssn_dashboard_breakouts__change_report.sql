SELECT
	hsac.advertiser_id AS `Advertiser ID`,
	hsac.created_at AS `Date of Change`,
	hsac.change_type AS `Change Type`,
	hsac.old_settings AS `Old Settings`,
	hsac.new_settings AS `New Settings`,
	e.name AS Advertiser
FROM hotel_ssr_advertiser_changes hsac
JOIN entities e ON hsac.advertiser_id = e.id