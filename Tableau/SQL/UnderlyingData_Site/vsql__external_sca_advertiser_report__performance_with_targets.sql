select
	entities.name as "Advertiser Name",
	entities.id as "Advertiser ID",
	'Flights' as "Product Category Type",
	campaigns.name as "Campaign",
	campaigns.tracking_code as "Campaign Tracking Code",
	(case when campaigns.paused = 0 then 'Active' else 'Paused' end) as "Campaign Status",
	ad_groups.name as "Ad Group",
	ad_groups.id as "Ad Group ID",	
	acapra.date_in_et as Date,
	ac_o.id as "Origination Airport Code ID",
	ac_d.id as "Destination Airport Code ID",
	acf_o.id as "Origination Favorite ID",
	acf_d.id as "Destination Favorite ID",
	ac_o.code as "Origination Airport",
	ac_d.code as "Destination Airport",
	acf_o.name as "Origination Airport Favorite",
	acf_d.name as "Destination Airport Favorite",
	null as "City",
	null as "State",
	null as "Country",
	sum(acapra.impression_count) as Impressions,
	sum(acapra.click_count) as Clicks,
	sum(acapra.actual_cpc_sum) as Spend,
	sum(acapra.click_conversion_count) as "Click Conversions",
	sum(acapra.exposed_conversion_count) as "View Conversions",
	sum(acapra.click_conversion_value_sum) as "Click Revenue",
	sum(acapra.exposed_conversion_value_sum) as "Exposed Revenue",
	sum(acapra.auction_position_sum) as "Auction Position Sum"
from intent_media_production.air_ct_advertiser_performance_report_aggregations acapra
left join intent_media_production.entities on acapra.advertiser_id = entities.id
left join intent_media_production.ad_groups on acapra.ad_group_id = ad_groups.id
left join intent_media_production.campaigns on ad_groups.campaign_id = campaigns.id
left join intent_media_production.campaign_budgets cb on cb.campaign_id = campaigns.id
left join intent_media_production.ad_units on acapra.ad_unit_id = ad_units.id
left join intent_media_production.sites on ad_units.site_id = sites.id
left join intent_media_production.airport_codes ac_o on acapra.origination_airport_code_id = ac_o.id
left join intent_media_production.airport_codes ac_d on acapra.destination_airport_code_id = ac_d.id
left join intent_media_production.air_ct_favorites acf_o on acapra.origination_air_ct_favorite_id = acf_o.id
left join intent_media_production.air_ct_favorites acf_d on acapra.destination_air_ct_favorite_id = acf_d.id
where acapra.date_in_et >= date((current_timestamp - interval '31 days') at timezone 'America/New_York')
and acapra.date_in_et < date(current_timestamp at timezone 'America/New_York')
and entities.id in (122112,153784)
group by 
	entities.name,
	entities.id,
	(case when campaigns.paused = 0 then 'Active' else 'Paused' end),
	campaigns.name,
	campaigns.tracking_code,
	ad_groups.name,
	ad_groups.id,	
	acapra.date_in_et,
	ac_o.id,
	ac_d.id,
	acf_o.id,
	acf_d.id,
	ac_o.code,
	ac_d.code,
	acf_o.name,
	acf_d.name

union



select
	entities.name as "Advertiser Name",
	entities.id as "Advertiser ID",
	'Hotels' as "Product Category Type",
	campaigns.name as "Campaign",
	campaigns.tracking_code as "Campaign Tracking Code",
	(case when campaigns.paused = 0 then 'Active' else 'Paused' end) as "Campaign Status",
	ad_groups.name as "Ad Group",
	ad_groups.id as "Ad Group ID",
	hcapra.date_in_et as Date,
	cast(null as int) as "Origination Airport Code ID",
	cast(null as int) as "Destination Airport Code ID",
	cast(null as int) as "Origination Favorite ID",
	cast(null as int) as "Destination Favorite ID",
	null as "Origination Code",
	null as "Destination Code",
	null as "Origination Favorite",
	null as "Destination Favorite",
	hci.name as "City",
	hci.state as "State",
	(case 
		when hco.name is null and hco_hci.name is null then null
		else ifnull(hco_hci.name,'') || ifnull(hco.name,'')
	end) as "Country",
	sum(hcapra.impression_count) as Impressions,
	sum(hcapra.click_count) as Clicks,
	sum(hcapra.actual_cpc_sum) as Spend,
	sum(hcapra.click_conversion_count) as "Click Conversions",
	sum(hcapra.exposed_conversion_count) as "View Conversions",
	sum(hcapra.click_conversion_value_sum) as "Click Revenue",
	sum(hcapra.exposed_conversion_value_sum) as "Exposed Revenue",
	sum(hcapra.auction_position_sum) as "Auction Position Sum"
from intent_media_production.hotel_ct_advertiser_performance_report_aggregations hcapra
left join intent_media_production.entities on hcapra.advertiser_id = entities.id
left join intent_media_production.ad_groups on hcapra.ad_group_id = ad_groups.id
left join intent_media_production.campaigns on ad_groups.campaign_id = campaigns.id
left join intent_media_production.ad_units on hcapra.ad_unit_id = ad_units.id
left join intent_media_production.sites on ad_units.site_id = sites.id
left join intent_media_production.countries hco on (hcapra.hotel_destination_type = 'Country' and hcapra.hotel_destination_id = hco.id)
left join intent_media_production.hotel_cities hci on (hcapra.hotel_destination_type = 'HotelCity' and hcapra.hotel_destination_id = hci.id)
left join intent_media_production.countries hco_hci on hci.country_id = hco_hci.id
where hcapra.date_in_et >= date((current_timestamp - interval '31 days') at timezone 'America/New_York')
and hcapra.date_in_et < date(current_timestamp at timezone 'America/New_York')
and entities.id in (122112,153784)
group by 
	entities.name,
	entities.id,
	campaigns.name,
	campaigns.tracking_code,
	(case when campaigns.paused = 0 then 'Active' else 'Paused' end),
	ad_groups.name,
	ad_groups.id,
	hcapra.date_in_et,
	hci.name,
	hci.state,
	(case 
		when hco.name is null and hco_hci.name is null then null
		else ifnull(hco_hci.name,'') || ifnull(hco.name,'')
	end)
