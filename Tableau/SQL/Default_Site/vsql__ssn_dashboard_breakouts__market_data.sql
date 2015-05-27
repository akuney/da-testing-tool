select
	cpc_stratum as "CPC Stratum",
	impressions_served as Impressions,
	Clicks,
	Spend,
	conversion_count as "Conversion Count",
	conversion_value_sum as "Conversion Value Sum",
	exposed_conversion_count as "Exposed Conversion Count",
	exposed_conversion_value_sum as "Exposed Conversion Value Sum",
	Date,
	ifnull(imm.name,'Other') as Market,
	Advertiser,
	Site,
	user_email as "User Email",
	first_name as "First Name",
	last_name as "Last Name",
	ifnull(imm.report_segment,'Other') as Segment
from intent_media_production.hotel_ssr_daily_dashboard_datasets hsddd
left join intent_media_production.entities e on e.name = hsddd.Advertiser
left join intent_media_production.hotel_property_advertisers hpa on hpa.hotel_ssr_advertiser_id = e.id
left join intent_media_production.intent_media_hotel_properties_markets imhpm on imhpm.hotel_property_id = hpa.hotel_property_id
left join intent_media_production.intent_media_markets imm on imm.id = imhpm.intent_media_market_id
where dataset = 'Mkt' 