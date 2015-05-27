select
	ac.requested_at_date_in_et as "Date",
	'Meta' as "Product Category Type",
	case when e.id in (148708) then 'OWW' end as "POS",
	e.id as "Advertiser ID",
	e.name as "Advertiser",
	campaigns.name as "Campaign Name",
	null as "Origination Code",
	null as "Destination Code",
	null as "Is Domestic Origin",
	hci.name as "Hotel City",
	hci.state as "Hotel State",
	case when hci_hco.name is null and hco.name is null then null else ifnull(hci_hco.name,'') || ifnull(hco.name,'') end as "Hotel Country",
	i.advertiser_hotel_property_id as "Hotel Property ID",
	count(c.request_id) as "Clicks",
	count(i.request_id) as "Impressions",
	sum(i.auction_position) as "Auction Position Sum",
	sum(ifnull(c.actual_cpc,0.0)) as "Spend"
from 
	(select
		i.request_id,
		i.auction_position,
		i.campaign_id,
		i.advertiser_id,
		i.advertiser_hotel_property_id,
		i.external_id,
		i.requested_at
	from intent_media_log_data_production.impressions i
	left join intent_media_production.ad_units au on au.id = i.ad_unit_id
	where i.requested_at_date_in_et >= (date(current_timestamp at timezone 'America/New_York') - interval '8 days')
		and i.requested_at_date_in_et < date(current_timestamp at timezone 'America/New_York')
		and au.ad_type = 'META') i
left join intent_media_log_data_production.ad_calls ac on i.request_id = ac.request_id
left join intent_media_log_data_production.clicks c on c.external_impression_id = i.external_id
left join intent_media_production.entities e on e.id = i.advertiser_id
left join intent_media_production.hotel_cities hci on (ac.destination_type = 'HotelCity' and hci.id = ac.destination_code) and ac.hotel_city_id is not null
left join intent_media_production.countries hco on (ac.destination_type = 'Country' and hco.id = ac.destination_code) and ac.hotel_country_id is not null
left join intent_media_production.countries hci_hco on hci_hco.id = hci.country_id
left join intent_media_production.campaigns on campaigns.id = i.campaign_id
left join intent_media_production.ad_units au on au.id = ac.ad_unit_id
where ac.requested_at_date_in_et >= (date(current_timestamp at timezone 'America/New_York') - interval '8 days')
	and ac.requested_at_date_in_et < date(current_timestamp at timezone 'America/New_York')
	and (c.requested_at_date_in_et >= (date(current_timestamp at timezone 'America/New_York') - interval '8 days') or c.requested_at_date_in_et is null)
	and (c.requested_at_date_in_et <= date(current_timestamp at timezone 'America/New_York') or c.requested_at_date_in_et is null)
	and (c.requested_at <= (i.requested_at + interval '24 hours') or c.requested_at is null)
	and ac.ip_address_blacklisted = 0
	and (c.ip_address_blacklisted = 0 or c.ip_address_blacklisted is null)
	and (c.fraudulent = 0 or c.fraudulent is null)
	and au.ad_type = 'META'
group by
	ac.requested_at_date_in_et,
	e.id,
	e.name,
	campaigns.name,
	hci.name,
	hci.state,
	case when hci_hco.name is null and hco.name is null then null else ifnull(hci_hco.name,'') || ifnull(hco.name,'') end,
	i.advertiser_hotel_property_id