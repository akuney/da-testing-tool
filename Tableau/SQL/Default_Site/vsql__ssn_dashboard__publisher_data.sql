/* last Change done for Hipmunk 89524306 */

select
	dimensions.*,
	data."Pages Available", 
	data."Pages Served", 
	data."Spend", 
	data."Clicks", 
	data."Impressions Served",
	data."Auction Participant Count"
from
(
  select *
  from
  (
    select
      distinct(aggregation_level_date_in_et) as Date,
      0 as Zero
    from intent_media_production.publisher_performance_report_aggregations
    where aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York')
  ) dates,
  (
    select
      (case
        when e.name = 'Orbitz' then 'OWW'
        when e.name like '%Travelport%' then 'Travelport'
        else e.name
      end) as Pub,
      s.display_name as Site,
      (case
        when e.name like '%Travelport%' then 'GDS'
        else 'OTA'
      end) as "SSN Channel Type",
      (case
        when au.name = 'Travelocity Martini Package Page' then 'Total Flight-Hotel Cross-Sell Page'
        when au.name = 'Travelocity List Page (Legacy)' then 'Total Hotel List Page'
        when au.name = 'Travelocity List Page (New Platform)' then 'Total Hotel List Page'
        when au.name in ('Apollo List Page','Galileo List Page','Worldspan List Page') then 'Total GDS List Page'
        when au.name like '%SEM%' then 'Total SEM List Page'
        when au.name like '%Hotel List Page%' then 'Total Hotel List Page'
        when au.name like '%Hotel Details Page%' then 'Total Hotel Details Page'
        when au.name like '%Trip Details Page%' then 'Total Trip Details Page'
        when au.name like '%Package%' then 'Total Packages List Page'
        when au.name like '%Hipmunk%' then 'Total Hotel List Page'  --Added for Hipmunk
        else au.name
      end) as "Type of Ad Unit",
      au.name as "Ad Unit",
      ifnull(imm.name, 'Other') as "Market Name",
      ifnull(imm.report_segment, 'Other') as "Segment",
      ifnull(imm.country, 'Other') as "Country",
      ifnull(imm.finance_region, 'Other') as "Finance Region"
    from intent_media_production.publisher_performance_report_aggregations ppra
    left join intent_media_production.ad_units au on au.id = ppra.ad_unit_id
    left join intent_media_production.sites s on s.id = au.site_id
    left join intent_media_production.entities e on e.id = s.publisher_id
    left join intent_media_production.intent_media_markets_publisher_markets immpm on immpm.market_id = ppra.market_id
    left join intent_media_production.intent_media_markets imm on imm.id = immpm.intent_media_market_id
    where
      (case
        when s.name = 'TRAVELOCITY' then aggregation_level_date_in_et >= '2011-04-01' and aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York')
        when s.name = 'HIPMUNK'   then aggregation_level_date_in_et >=  '2015-03-02' and aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York') -- Added for Hipmunk
        else aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York') and (((au.name like '%SEM%' or au.name like '%Hotel Details%') and aggregation_level_date_in_et >= '2012-05-15') or (au.name not like '%SEM%' and au.name not like '%Hotel Details%'))
      end)
    group by
      (case
        when e.name = 'Orbitz' then 'OWW'
        when e.name like '%Travelport%' then 'Travelport'
        else e.name
      end),
      s.display_name,
      (case
        when e.name like '%Travelport%' then 'GDS'
        else 'OTA'
      end),
      (case
        when au.name = 'Travelocity Martini Package Page' then 'Total Flight-Hotel Cross-Sell Page'
        when au.name = 'Travelocity List Page (Legacy)' then 'Total Hotel List Page'
        when au.name = 'Travelocity List Page (New Platform)' then 'Total Hotel List Page'
        when au.name in ('Apollo List Page','Galileo List Page','Worldspan List Page') then 'Total GDS List Page'
        when au.name like '%SEM%' then 'Total SEM List Page'
        when au.name like '%Hotel List Page%' then 'Total Hotel List Page'
        when au.name like '%Hotel Details Page%' then 'Total Hotel Details Page'
        when au.name like '%Trip Details Page%' then 'Total Trip Details Page'
        when au.name like '%Package%' then 'Total Packages List Page'
        when au.name like '%Hipmunk%' then 'Total Hotel List Page'   -- Added for hipmunk
        else au.name
      end),
      au.name,
      ifnull(imm.name, 'Other'),
      ifnull(imm.report_segment, 'Other'),
      ifnull(imm.country, 'Other'),
      ifnull(imm.finance_region, 'Other')
  ) ad_unit_names
) dimensions
left join
(
  select
	  aggregation_level_date_in_et as Date,
    (case
      when e.name = 'Orbitz' then 'OWW'
      when e.name like '%Travelport%' then 'Travelport'
      else e.name
    end) as Pub,
    s.display_name as Site,
    (case
      when e.name like '%Travelport%' then 'GDS'
      else 'OTA'
    end) as "SSN Channel Type",
    (case
      when au.name = 'Travelocity Martini Package Page' then 'Total Flight-Hotel Cross-Sell Page'
      when au.name = 'Travelocity List Page (Legacy)' then 'Total Hotel List Page'
      when au.name = 'Travelocity List Page (New Platform)' then 'Total Hotel List Page'
      when au.name in ('Apollo List Page','Galileo List Page','Worldspan List Page') then 'Total GDS List Page'
      when au.name like '%SEM%' then 'Total SEM List Page'
      when au.name like '%Hotel List Page%' then 'Total Hotel List Page'
      when au.name like '%Hotel Details Page%' then 'Total Hotel Details Page'
      when au.name like '%Trip Details Page%' then 'Total Trip Details Page'
      when au.name like '%Package%' then 'Total Packages List Page'
      when au.name like '%Hipmunk%' then 'Total Hotel List Page'  -- Added for Hipmunk
      else au.name
    end) as "Type of Ad Unit",
    au.name as "Ad Unit",
    ifnull(imm.name, 'Other') as "Market Name",
    ifnull(imm.report_segment, 'Other') as "Segment",
    ifnull(imm.country, 'Other') as "Country",
    ifnull(imm.finance_region, 'Other') as "Finance Region",
    sum(ad_call_count) as "Pages Available",
    sum(case when positions_filled > 0 then ad_call_count else 0 end) as "Pages Served",
    sum(gross_actual_cpc_sum) as "Spend",
    sum(click_count) as "Clicks",
    sum(ad_call_count * positions_filled) as "Impressions Served",
    sum(auction_participant_count) as "Auction Participant Count"
  from intent_media_production.publisher_performance_report_aggregations ppra
  left join intent_media_production.ad_units au on au.id = ppra.ad_unit_id
  left join intent_media_production.sites s on s.id = au.site_id
  left join intent_media_production.entities e on e.id = s.publisher_id
  left join intent_media_production.intent_media_markets_publisher_markets immpm on immpm.market_id = ppra.market_id
  left join intent_media_production.intent_media_markets imm on imm.id = immpm.intent_media_market_id
  where
    (case
      when s.name = 'TRAVELOCITY' then aggregation_level_date_in_et >= '2011-04-01' and aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York')
      when s.name = 'HIPMUNK'   then aggregation_level_date_in_et >=  '2015-03-02' and aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York') -- Added for Hipmunk
      else aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York') and (((au.name like '%SEM%' or au.name like '%Hotel Details%') and aggregation_level_date_in_et >= '2012-05-15') or (au.name not like '%SEM%' and au.name not like '%Hotel Details%'))
    end)
  group by
    (case
      when e.name = 'Orbitz' then 'OWW'
      when e.name like '%Travelport%' then 'Travelport'
      else e.name
    end),
    s.display_name,
    (case
      when e.name like '%Travelport%' then 'GDS'
      else 'OTA'
    end),
    (case
      when au.name = 'Travelocity Martini Package Page' then 'Total Flight-Hotel Cross-Sell Page'
      when au.name = 'Travelocity List Page (Legacy)' then 'Total Hotel List Page'
      when au.name = 'Travelocity List Page (New Platform)' then 'Total Hotel List Page'
      when au.name in ('Apollo List Page','Galileo List Page','Worldspan List Page') then 'Total GDS List Page'
      when au.name like '%SEM%' then 'Total SEM List Page'
      when au.name like '%Hotel List Page%' then 'Total Hotel List Page'
      when au.name like '%Hotel Details Page%' then 'Total Hotel Details Page'
      when au.name like '%Trip Details Page%' then 'Total Trip Details Page'
      when au.name like '%Package%' then 'Total Packages List Page'
      when au.name like '%Hipmunk%' then 'Total Hotel List Page' -- Added for Hipmunk
      else au.name
    end),
    au.name,
    aggregation_level_date_in_et,
    ifnull(imm.name, 'Other'),
    ifnull(imm.report_segment, 'Other'),
    ifnull(imm.country, 'Other'),
    ifnull(imm.finance_region, 'Other')
) data

on dimensions.Date = data.Date
and dimensions.Pub = data.Pub
and dimensions.Site = data.Site
and dimensions."SSN Channel Type" = data."SSN Channel Type"
and dimensions."Type of Ad Unit" = data."Type of Ad Unit"
and dimensions."Ad Unit" = data."Ad Unit"
and dimensions."Market Name" = data."Market Name"
and dimensions.Segment = data.Segment
and dimensions.Country = data.Country
and dimensions."Finance Region" = data."Finance Region"