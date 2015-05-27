select
	'Flights' as "Product Category Type",
	e.name as "Advertiser Name",
	c.precheck_eligibility_type as "Precheck Type",
	c.display_format as "Display Type",
	case when e.name like '%UK%' then 'UK' else 'US' end as Country,
	(case e.advertiser_category_type
		when 'AIRLINE_DOMESTIC' then 'Domestic Airline'
		when 'AIRLINE_INTERNATIONAL' then 'International Airline'
		when 'HOTEL_CHAIN' then 'Hotel Chain'
		when 'META' then 'Meta'
		when 'TIER_1' then 'Tier 1'
		when 'TIER_2' then 'Tier 2'
		when 'OTA_BUDGET' then 'OTA Budget'
		when 'OTHER' then 'Other'
		else e.advertiser_category_type
	end) as "Advertiser Segment",
	revenue_metrics.*,
	performance."Pages Served"

--------------Revenue Metrics
from
(
    select
    dimensions.*,
    data.Impressions,
    data.Clicks,
    data.Spend,
    data."Click Conversions",
    data."View Conversions",
    data."Click Revenue",
    data."Exposed Revenue",
    data."Auction Position Sum"
  
  from
  (
    select *
    from
    (
      select
        distinct(date_in_et) as Date
      from intent_media_production.air_ct_advertiser_performance_report_aggregations acapra
      where acapra.date_in_et >= date(current_timestamp at timezone 'America/New_York' - interval '371 days')
        and acapra.date_in_et < date(current_timestamp at timezone 'America/New_York' )
    ) dates,
    (
      select DISTINCT
        advertiser_id as "Advertiser ID",
        campaign_id as "Campaign ID"
      from intent_media_production.air_ct_advertiser_performance_report_aggregations acapra
      where acapra.date_in_et >= date(current_timestamp at timezone 'America/New_York' - interval '371 days')
        and acapra.date_in_et < date(current_timestamp at timezone 'America/New_York')
    ) advertisers
  ) dimensions
  left join
  (
    select
      acapra.date_in_et as Date,
      acapra.advertiser_id as "Advertiser ID",
      acapra.Campaign_id as "Campaign ID",
      sum(acapra.impression_count) as Impressions,
      sum(acapra.click_count) as Clicks,
      sum(acapra.actual_cpc_sum) as Spend,
      sum(acapra.click_conversion_count) as "Click Conversions",
      sum(acapra.exposed_conversion_count) as "View Conversions",
      sum(acapra.click_conversion_value_sum) as "Click Revenue",
      sum(acapra.exposed_conversion_value_sum) as "Exposed Revenue",
      sum(acapra.auction_position_sum) as "Auction Position Sum"
    from intent_media_production.air_ct_advertiser_performance_report_aggregations acapra
    left join intent_media_production.ad_units au on au.id = acapra.ad_unit_id
    left join intent_media_production.sites s on s.ID = au.site_ID
    where acapra.date_in_et >= date(current_timestamp at timezone 'America/New_York' - interval '371 days')
      and acapra.date_in_et < date(current_timestamp at timezone 'America/New_York' )
      and acapra.date_in_et >= au.reporting_start_date
    group by
      acapra."date_in_et",
      acapra."advertiser_id",
      acapra."campaign_id"
  ) data
  on dimensions.Date = data.Date
  and dimensions."Advertiser ID"= data."Advertiser ID"
  and dimensions."Campaign ID"= data."Campaign ID"
) revenue_metrics
------------------------Performance
left join
(
	select
		aggregation_level_date_in_et as Date,
		advertiser_id as "Advertiser ID",
		campaign_id as "Campaign ID",
		sum(filtered_ad_count + impression_count) as "Pages Served"
	from intent_media_production.air_ct_impression_share_report_aggregations
	where aggregation_level_date_in_et >= date(current_timestamp at timezone 'America/New_York' - interval '371 days')
		and aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York' )
	group by
	  aggregation_level_date_in_et,
	  advertiser_id,
	  campaign_id
) performance
on performance.date = revenue_metrics.Date
and performance."Advertiser ID" = revenue_metrics."Advertiser ID"
and performance."Campaign ID" = revenue_metrics."Campaign ID"
left join intent_media_production.entities e on revenue_metrics."Advertiser ID" = e.id
left join intent_media_production.campaigns c on revenue_metrics."Campaign ID" = c.id

------------------------------------------------------------------------------------------------------------------------------------------------
union

select
	'Hotels' as "Product Category Type",
	e.name as "Advertiser Name",
	c.precheck_eligibility_type as "Precheck Type",
	c.display_format as "Display Type",
	case when e.name like '%UK%' then 'UK' else 'US' end as Country,
	(case e.advertiser_category_type
		when 'AIRLINE_DOMESTIC' then 'Domestic Airline'
		when 'AIRLINE_INTERNATIONAL' then 'International Airline'
		when 'HOTEL_CHAIN' then 'Hotel Chain'
		when 'META' then 'Meta'
		when 'TIER_1' then 'Tier 1'
		when 'TIER_2' then 'Tier 2'
		when 'OTA_BUDGET' then 'OTA Budget'
		when 'OTHER' then 'Other'
		else e.advertiser_category_type
	end) as "Advertiser Segment",
	revenue_metrics.*,
	performance."Pages Served"
from


--------------Revenue Metrics
(
  select
    dimensions.*,
    data.Impressions,
    data.Clicks,
    data.Spend,
    data."Click Conversions",
    data."View Conversions",
    data."Click Revenue",
    data."Exposed Revenue",
    data."Auction Position Sum"
  from
  (
    select *
    from
    (
      select
        distinct(date_in_et) as Date
      from intent_media_production.hotel_ct_advertiser_performance_report_aggregations hcapra
        where hcapra.date_in_et >= date(current_timestamp at timezone 'America/New_York' - interval '371 days')
          and hcapra.date_in_et < date(current_timestamp at timezone 'America/New_York' )
    ) dates,
    (
      select
        distinct advertiser_id as "Advertiser ID",
        campaign_id as "Campaign ID"
      from intent_media_production.hotel_ct_advertiser_performance_report_aggregations hcapra
        where hcapra.date_in_et >= date(current_timestamp at timezone 'America/New_York' - interval '371 days')
          and hcapra.date_in_et < date(current_timestamp at timezone 'America/New_York')
    ) advertisers
  ) dimensions
  left join
  (
    select
      hcapra.date_in_et as Date,
      hcapra.advertiser_id as "Advertiser ID",
      hcapra.campaign_id as "Campaign ID",
      sum(hcapra.impression_count) as Impressions,
      sum(hcapra.click_count) as Clicks,
      sum(hcapra.actual_cpc_sum) as Spend,
      sum(hcapra.click_conversion_count) as "Click Conversions",
      sum(hcapra.exposed_conversion_count) as "View Conversions",
      sum(hcapra.click_conversion_value_sum) as "Click Revenue",
      sum(hcapra.exposed_conversion_value_sum) as "Exposed Revenue",
      sum(hcapra.auction_position_sum) as "Auction Position Sum"
    from intent_media_production.hotel_ct_advertiser_performance_report_aggregations hcapra
    left join intent_media_production.ad_units au on au.id = hcapra.ad_unit_id
    left join intent_media_production.sites s on s.ID = au.site_ID
    where hcapra.date_in_et >= date(current_timestamp at timezone 'America/New_York' - interval '371 days')
      and hcapra.date_in_et < date(current_timestamp at timezone 'America/New_York' )
      and hcapra.date_in_et >= au.reporting_start_date
    group by
      hcapra."date_in_et",
      hcapra."advertiser_id",
      hcapra.campaign_id
  ) data
  on dimensions.Date = data.Date
  and dimensions."Advertiser ID"= data."Advertiser ID"
  and dimensions."Campaign ID"= data."Campaign ID"
) revenue_metrics
-------------------------Performacne
left join
(
	select
		aggregation_level_date_in_et as Date,
		advertiser_id as "Advertiser ID",
		campaign_id as "Campaign ID",
		sum(filtered_ad_count + impression_count) as "Pages Served"
	from intent_media_production.hotel_ct_impression_share_report_aggregations
	where aggregation_level_date_in_et >= date(current_timestamp at timezone 'America/New_York' - interval '371 days')
		and aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York' )
	group by
	  aggregation_level_date_in_et,
	  advertiser_id,
	  campaign_id
) performance
on performance.date = revenue_metrics.Date
and performance."Advertiser ID" = revenue_metrics."Advertiser ID"
and performance."Campaign ID" = revenue_metrics."Campaign ID"
left join intent_media_production.entities e on revenue_metrics."Advertiser ID" = e.id
left join intent_media_production.campaigns c on revenue_metrics."Campaign ID" = c.id
where revenue_metrics.Date > '2012-12-10'