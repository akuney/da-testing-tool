select
	dimensions.*,
	data.Impressions,
	data.Clicks,
	data.Conversions,
	data.Spend,
	data."Auction Position Sum",
	data.Revenue,
	data."Exposed Conversions",
	data."Exposed Revenue"
from
(
  select *
  from
  (
    select
      distinct(aggregation_level_date_in_et) as Date,
      0 as Zero
    from intent_media_production.advertiser_account_report_aggregations
    where aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York')
  ) dates,
  (
    select
      imm.name as "Market Name",
      imm.report_segment as "Segment",
      imm.country as "Country",
      imm.finance_region as "Finance Region"
    from intent_media_production.intent_media_markets imm

    union

    select
      'Other' as "Market Name",
      'Other' as "Segment",
      'Other' as "Country",
      'Other' as "Finance Region"
  ) markets,
  (
    select
      (case
        when e.name = 'Orbitz' then 'OWW'
        when e.name like '%Travelport%' then 'Travelport'
        else e.name
      end) as Pub,
      s.display_name as "Site",
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
        else au.name
      end) as "Type of Ad Unit",
      au.name as "Ad Unit"
    from intent_media_production.advertiser_account_report_aggregations ppra
    left join intent_media_production.ad_units au on ppra.ad_unit_id = au.id
    left join intent_media_production.sites s on s.id = au.site_id
    left join intent_media_production.entities e on e.id = s.publisher_id
    where ad_type = 'SSR'
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
        else au.name
      end),
      au.name
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
      else au.name
    end) as "Type of Ad Unit",
    au.name as "Ad Unit",
    ifnull(imm.name, 'Other') as "Market Name",
    ifnull(imm.report_segment, 'Other') as "Segment",
    ifnull(imm.country, 'Other') as "Country",
    ifnull(imm.finance_region, 'Other') as "Finance Region",
    sum(impression_count) as Impressions,
    sum(click_count) as Clicks,
    sum(click_conversion_count) as Conversions,
    sum(actual_cpc_sum) as Spend,
    sum(auction_position_sum) as "Auction Position Sum",
    sum(click_conversion_value_sum) as Revenue,
    sum(exposed_conversion_count) as "Exposed Conversions",
    sum(exposed_conversion_value_sum) as "Exposed Revenue"
  from intent_media_production.advertiser_account_report_aggregations aara
  left join intent_media_production.ad_units au on au.id = aara.ad_unit_id
  left join intent_media_production.sites s on s.id = au.site_id
  left join intent_media_production.entities e on e.id = s.publisher_id
  left join intent_media_production.intent_media_markets_publisher_markets immpm on immpm.market_id = aara.market_id
  left join intent_media_production.intent_media_markets imm on imm.id = immpm.intent_media_market_id
  where
    (case
      when s.name = 'TRAVELOCITY'
        then aggregation_level_date_in_et >= '2011-04-01' and aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York')
      else
        aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York')
        and (((au.name like '%SEM%' or au.name like '%Hotel Details%') and aggregation_level_date_in_et >= '2012-05-15')
        or (au.name not like '%SEM%' and au.name not like '%Hotel Details%'))
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
and dimensions."SSN Channel Type" = dimensions."SSN Channel Type"
and dimensions."Type of Ad Unit" = data."Type of Ad Unit"
and dimensions."Ad Unit" = data."Ad Unit"
and dimensions."Market Name" = data."Market Name"
and dimensions."Segment" = data."Segment"
and dimensions."Country" = data."Country"
and dimensions."Finance Region" = data."Finance Region"