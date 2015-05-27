select
	ac.requested_at_date_in_et as Date,
	(case
		when ac.hotel_city_id is null and ac.hotel_country_id is null then 'Flights'
		else 'Hotels'
	end) as "Product Category Type",
	(case 
		when i.advertiser_id in (61224,93063) then 'OWW'
		when i.advertiser_id in (106574) then 'CTIX'
	end) as POS,	
	i.advertiser_id as "Advertiser ID",	
	e.name as "Advertiser Name",
	campaigns.name as "Campaign Name",	
	ac.origination_code as "Origination Code",
	ac.destination_code as "Destination Code",
	(case when aco.is_domestic is null then null else (case when aco.is_domestic then 1 else 0 end) end) as "Is Domestic Origin",
	hci.name as "Hotel City",
	hci.state as "Hotel State",
	(case when hci_hco.name is not null then hci_hco.name when hco.name is not null then hco.name end) as "Hotel Country",
	count(distinct(c.request_id)) as Clicks,
	count(distinct(i.request_id)) as Impressions,
	sum(i.auction_position)/count(distinct(i.request_id)) as "Auction Position Sum",
	sum(c.actual_cpc) as Spend
from intent_media_log_data_production.impressions i
left join intent_media_log_data_production.ad_calls ac on ac.request_id = i.request_id
left join intent_media_log_data_production.clicks c on c.external_impression_id = i.external_id
left join intent_media_production.ad_units au on au.id = i.ad_unit_id
left join intent_media_production.entities e on e.id = i.advertiser_id
left join intent_media_production.campaigns on campaigns.id = i.campaign_id
left join intent_media_production.airport_codes aco on aco.code = ac.origination_code
left join intent_media_production.hotel_cities hci on hci.id = ac.hotel_city_id
left join intent_media_production.countries hci_hco on hci_hco.id = hci.country_id
left join intent_media_production.countries hco on hco.id = ac.hotel_country_id
where i.requested_at_date_in_et in ('2014-03-01')
	and ac.requested_at_date_in_et in ('2014-03-01')
	and ac.ip_address_blacklisted = 0
	and i.ip_address_blacklisted = 0
	and i.advertiser_id in (61224, 93063, 106574)
	and c.ip_address_blacklisted = 0
	and c.fraudulent = 0
	and au.ad_type = 'CT'
group by
	ac.requested_at_date_in_et,
	(case
		when ac.hotel_city_id is null and ac.hotel_country_id is null then 'Flights'
		else 'Hotels'
	end),
	(case 
		when i.advertiser_id in (61224,93063) then 'OWW'
		when i.advertiser_id in (106574) then 'CTIX'
	end),	
	i.advertiser_id,	
	e.name,
	campaigns.name,	
	ac.origination_code,
	ac.destination_code,
	(case when aco.is_domestic is null then null else (case when aco.is_domestic then 1 else 0 end) end),
	hci.name,
	hci.state,
	(case when hci_hco.name is not null then hci_hco.name when hco.name is not null then hco.name end)
