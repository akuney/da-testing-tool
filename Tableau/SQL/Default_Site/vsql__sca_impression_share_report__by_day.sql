select
  'Flights' as "Product Category Type",
  acisra.aggregation_level_date_in_et as Date,
  e.name as Advertiser,
  c.name as Campaign,
  c.precheck_eligibility_type as "Precheck Type",
  ag.name as "Ad Group",
  ifnull(ac_o.code,'') || ifnull(acf_o.name,'') as Origination,
  ifnull(ac_d.code,'') || ifnull(acf_d.name,'') as Destination,
  ac_o.code as "Origination Code",
  ac_d.code as "Destination Code",
  acf_o.name as "Favorite Origination",
  acf_d.name as "Favorite Destination",
  null as "Hotel City",
  null as "Hotel State",
  null as "Hotel Country",
  c.display_format as "Display Type",
  sum(acisra.filtered_ad_count) as "Filtered Ad Count",
  sum(acisra.impression_count) as Impressions,
  sum(acisra.filtered_ad_for_budget_count) as "Filtered Ad for Budget",
  sum(acisra.filtered_ad_for_bid_count) as "Filtered Ad for Bid Count",
  sum(acisra.filtered_ad_for_click_blackout_count) as "Filtered Ad for Click Blackout",
  sum(acisra.filtered_ad_count + acisra.impression_count) as "Eligible Ad Count",
  max(max_bid_increment_needed_to_participate) as "Max Bid Increment Needed to Participate",
  sum(daily_budget_needed_to_participate) as "Daily Budget Needed to Participate",
  max(max_bid_increment_needed_for_position_one) as "Max Bid Increment Needed for Position One",
  sum(daily_budget_needed_for_position_one) as "Daily Budget Needed for Position One"
from intent_media_production.air_ct_impression_share_report_aggregations acisra
left join intent_media_production.entities e on acisra.advertiser_id = e.id
left join intent_media_production.ad_groups ag on acisra.ad_group_id = ag.id
left join intent_media_production.campaigns c on ag.campaign_id = c.id
left join intent_media_production.airport_codes ac_o on acisra.origination_airport_code_id = ac_o.id
left join intent_media_production.airport_codes ac_d on acisra.destination_airport_code_id = ac_d.id
left join intent_media_production.air_ct_favorites acf_o on acisra.origination_air_ct_favorite_id = acf_o.id
left join intent_media_production.air_ct_favorites acf_d on acisra.destination_air_ct_favorite_id = acf_d.id
where aggregation_level_date_in_et >= date((current_timestamp - interval '14 days') at timezone 'America/New_York')
and aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York')
group by
  acisra.aggregation_level_date_in_et,
  e.name,
  c.name,
  c.precheck_eligibility_type,
  ag.name,
  ifnull(ac_o.code,'') || ifnull(acf_o.name,''),
  ifnull(ac_d.code,'') || ifnull(acf_d.name,''),
  ac_o.code,
  ac_d.code,
  acf_o.name,
  acf_d.name,
  c.display_format

union

select
  'Hotels' as "Product Category Type",
  hcisra.aggregation_level_date_in_et as Date,
  e.name as Advertiser,
  c.name as Campaign,
  c.precheck_eligibility_type as "Precheck Type",
  ag.name as "Ad Group",
  null as Origination,
  null as Destination,
  null as "Origination Code",
  null as "Destination Code",
  null as "Favorite Origination",
  null as "Favorite Destination",
  hci.name as "Hotel City",
  hci.state as "Hotel State",
  (case when hotel_city_id is null then hco.name else hci_hco.name end) as "Hotel Country",
  c.display_format as "Display Type",
  sum(hcisra.filtered_ad_count) as "Filtered Ad Count",
  sum(hcisra.impression_count) as Impressions,
  sum(hcisra.filtered_ad_for_budget_count) as "Filtered Ad for Budget",
  sum(hcisra.filtered_ad_for_bid_count) as "Filtered Ad for Bid Count",
  sum(hcisra.filtered_ad_for_click_blackout_count) as "Filtered Ad for Click Blackout",
  sum(hcisra.filtered_ad_count + hcisra.impression_count) as "Eligible Ad Count",
  max(max_bid_increment_needed_to_participate) as "Max Bid Increment Needed to Participate",
  sum(daily_budget_needed_to_participate) as "Daily Budget Needed to Participate",
  max(max_bid_increment_needed_for_position_one) as "Max Bid Increment Needed for Position One",
  sum(daily_budget_needed_for_position_one) as "Daily Budget Needed for Position One"
from intent_media_production.hotel_ct_impression_share_report_aggregations hcisra
left join intent_media_production.entities e on hcisra.advertiser_id = e.id
left join intent_media_production.ad_groups ag on hcisra.ad_group_id = ag.id
left join intent_media_production.campaigns c on ag.campaign_id = c.id
left join intent_media_production.countries hco on hcisra.hotel_country_id = hco.id and hcisra.hotel_city_id is null
left join intent_media_production.hotel_cities hci on hcisra.hotel_city_id = hci.id
left join intent_media_production.countries hci_hco on hci_hco.id = hci.country_id
where aggregation_level_date_in_et >= date((current_timestamp - interval '14 days') at timezone 'America/New_York')
and aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York')
group by
  hcisra.aggregation_level_date_in_et,
  e.name,
  c.name,
  c.precheck_eligibility_type,
  ag.name,
  hcisra.hotel_city_id,
  hci.name,
  hci.state,
  (case when hotel_city_id is null then hco.name else hci_hco.name end),
  c.display_format
