select
	'Flights' as "Product Category Type",
	ad_groups.tracking_code as "Tracking Code",
	acapra.date_in_et as Date,
	ac_o.code as "Origination Code",
	ac_d.code as "Destination Code",
	acf_o.name as "Favorite Origination",
	acf_d.name as "Favorite Destination",
	null as "Hotel City",
	null as "Hotel State",
	null as "Hotel Country",
	null as "OLACID",
	sum(acapra.click_count) as Clicks,
	sum(acapra.actual_cpc_sum) as Spend
from intent_media_production.air_ct_advertiser_performance_report_aggregations acapra
left join intent_media_production.entities on acapra.advertiser_id = entities.id
left join intent_media_production.ad_groups on acapra.ad_group_id = ad_groups.id
left join intent_media_production.campaigns on ad_groups.campaign_id = campaigns.id
left join intent_media_production.ad_units on acapra.ad_unit_id = ad_units.id
left join intent_media_production.airport_codes ac_o on acapra.origination_airport_code_id = ac_o.id
left join intent_media_production.airport_codes ac_d on acapra.destination_airport_code_id = ac_d.id
left join intent_media_production.air_ct_favorites acf_o on acapra.origination_air_ct_favorite_id = acf_o.id
left join intent_media_production.air_ct_favorites acf_d on acapra.destination_air_ct_favorite_id = acf_d.id
where acapra.date_in_et >= date((current_timestamp - interval '30 days') at timezone 'America/New_York')
and acapra.date_in_et < date(current_timestamp at timezone 'America/New_York')
and acapra.advertiser_id = 59777
group by 
	ad_groups.tracking_code,
	acapra.date_in_et, 
	ac_o.code,
	ac_d.code,
	acf_o.name,
	acf_d.name

union


select
	'Hotels' as "Product Category Type",
	ad_groups.tracking_code as "Tracking Code",
	hcapra.date_in_et as Date,
	null as "Origination Code",
	null as "Destination Code",
	null as "Favorite Origination",
	null as "Favorite Destination",
	hci.name as "Hotel City",
	hci.state as "Hotel State",
	(case 
		when hcapra.hotel_destination_type = 'Country' then hco.name 
		when hcapra.hotel_destination_type = 'HotelCity' then hco_hci.name
	end) as "Hotel Country",
	substring(it.tracking_code,8) as "OLACID",
	sum(hcapra.click_count) as Clicks,
	sum(hcapra.actual_cpc_sum) as Spend
from intent_media_production.hotel_ct_advertiser_performance_report_aggregations hcapra
left join intent_media_production.entities on hcapra.advertiser_id = entities.id
left join intent_media_production.ad_groups on hcapra.ad_group_id = ad_groups.id
left join intent_media_production.campaigns on ad_groups.campaign_id = campaigns.id
left join intent_media_production.ad_units on hcapra.ad_unit_id = ad_units.id
left join intent_media_production.countries hco on (hcapra.hotel_destination_type = 'Country' and hcapra.hotel_destination_id = hco.id)
left join intent_media_production.hotel_cities hci on (hcapra.hotel_destination_type = 'HotelCity' and hcapra.hotel_destination_id = hci.id)
left join intent_media_production.countries hco_hci on (hcapra.hotel_destination_type = 'HotelCity' and hcapra.hotel_destination_id = hci.id and hci.country_id = hco_hci.id)
left join intent_media_production.intent_targets it on (ad_groups.id = it.ad_group_id and ((hci.id = it.intent_id and it.intent_type = 'HotelCity') or (hco.id = it.intent_id and it.intent_type = 'Country')))
where hcapra.date_in_et >= date((current_timestamp - interval '30 days') at timezone 'America/New_York')
and hcapra.date_in_et < date(current_timestamp at timezone 'America/New_York')
and hcapra.advertiser_id = 59777
group by
	ad_groups.tracking_code,
	hcapra.date_in_et, 
	hci.name,
	hci.state,
	(case 
		when hcapra.hotel_destination_type = 'Country' then hco.name 
		when hcapra.hotel_destination_type = 'HotelCity' then hco_hci.name
	end),
	substring(it.tracking_code,8)