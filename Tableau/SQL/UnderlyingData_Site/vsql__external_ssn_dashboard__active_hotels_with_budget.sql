select
	pa.Date,
	pa."Advertiser ID",
	pa."Market ID",
	pa.Site as "Site",
	ifnull(imm.name, 'Other') as "Market Name",
	ifnull(imm.report_segment, 'Other') as "Segment",
	ifnull(imm.country, 'Other') as "Country",
	ifnull(imm.finance_region, 'Other') as "Finance Region",
	pa.Pub,
	pa."SSN Channel Type",
	spend_query."Impressions Served",
	spend_query.Clicks,
	spend_query.Spend,
	spend_query.Conversions,
	spend_query."Exposed Conversions",
	spend_query."Conversion Value Sum",
	spend_query."Exposed Conversion Value Sum"
from
	(
	  select
      pa.aggregation_level_date_in_et as Date,
      pa.advertiser_id as "Advertiser ID",
      pa.market_id as "Market ID",
      s.display_name as Site,
      (case
        when ep.name = 'Orbitz' then 'OWW'
        when ep.name like '%Travelport%' then 'Travelport'
        else ep.name
      end) as Pub,
      ea.ssn_channel_type as "SSN Channel Type"
    from intent_media_production.participating_advertisers pa
    left join intent_media_production.sites s on s.name = pa.site
    left join intent_media_production.entities ep on ep.id = s.publisher_id
    left join intent_media_production.entities ea on ea.id = pa.advertiser_id
    where ep.active = 1
      and ea.active = 1
      and ea.entity_type = 'HotelSsrAdvertiser'
      and ea.first_auction_participation is not null
      and DATE(ea.first_auction_participation at timezone 'America/New_York') <= pa.aggregation_level_date_in_et
    group by
      pa.aggregation_level_date_in_et,
      pa.advertiser_id,
      pa.market_id,
      s.display_name,
      (case
        when ep.name = 'Orbitz' then 'OWW'
        when ep.name like '%Travelport%' then 'Travelport'
        else ep.name
      end),
      ea.ssn_channel_type
  ) pa
left join
	(
	  select
      aara.aggregation_level_date_in_et as Date,
      aara.advertiser_id as "Advertiser ID",
      aara.market_id as "Market ID",
      s.display_name as Site,
      sum(impression_count) as "Impressions Served",
      sum(click_count) as Clicks,
      sum(actual_cpc_sum) as Spend,
      sum(click_conversion_count) as Conversions,
      sum(exposed_conversion_count) as "Exposed Conversions",
      sum(click_conversion_value_sum) as "Conversion Value Sum",
      sum(exposed_conversion_value_sum) as "Exposed Conversion Value Sum"
    from intent_media_production.advertiser_account_report_aggregations aara
    left join intent_media_production.ad_units au on au.id = aara.ad_unit_id
    left join intent_media_production.sites s on s.id = au.site_id
    group by
      aara.aggregation_level_date_in_et,
      aara.advertiser_id,
      aara.market_id,
      s.display_name
  ) spend_query
on pa.Date = spend_query.Date
and pa."Advertiser ID" = spend_query."Advertiser ID"
and pa."Market ID" = spend_query."Market ID"
and pa.Site = spend_query.Site
left join intent_media_production.intent_media_markets_publisher_markets immpm on immpm.market_id = pa."Market ID"
left join intent_media_production.intent_media_markets imm on imm.id = immpm.intent_media_market_id
where pa.Date < date(current_timestamp at timezone 'America/New_York')

union

select
	Date,
	cast(null as int) as "Advertiser ID",
	"Market ID",
	Site,
	"Market Name",
	Segment,
	Country,
	"Finance Region",
	Pub,
	"SSN Channel Type",
	cast(null as integer) as "Impressions Served",
	cast(null as integer) as Clicks,
	cast(null as float) as Spend,
	cast(null as integer) as Conversions,
	cast(null as integer) as "Exposed Conversions",
	cast(null as float) as "Conversion Value Sum",
	cast(null as float) as "Exposed Conversion Value Sum"
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
	  imm.id as "Market ID",
	  imm.name as "Market Name",
	  imm.report_segment as "Segment",
	  imm.country as "Country",
	  imm.finance_region as "Finance Region"
  from intent_media_production.intent_media_markets imm

  union

  select
	  cast(null as integer) as "Market ID",
	  'Other' as "Market Name",
	  'Other' as "Segment",
	  'Other' as "Country",
	  'Other' as "Finance Region"
) markets,
(
  select
    s.display_name as Site,
	  (case
      when e.name = 'Orbitz' then 'OWW'
      when e.name like '%Travelport%' then 'Travelport'
      else e.name
    end) as Pub,
    (case
        when e.name like '%Travelport%' then 'GDS'
        else 'OTA'
    end) as "SSN Channel Type"
  from intent_media_production.publisher_performance_report_aggregations ppra
  left join intent_media_production.ad_units au on ppra.ad_unit_id = au.id
  left join intent_media_production.sites s on s.id = au.site_id
  left join intent_media_production.entities e on e.id = s.publisher_id
  where ad_type = 'SSR'
  group by
    s.display_name,
    (case
      when e.name = 'Orbitz' then 'OWW'
      when e.name like '%Travelport%' then 'Travelport'
      else e.name
    end),
	  (case
        when e.name like '%Travelport%' then 'GDS'
        else 'OTA'
    end)
) sites