select 
	fpvs.date_in_et as Date,
	sites.display_name as "Site",
	adv.precheck_eligibility_type as "Precheck Type",
	adv.display_format as "Display Type",
	fpvs."Flight Path Visits",
	convs.conversions AS "Publisher Conversions",
	adv.clicks AS Clicks,
	adv.click_conversions AS "Advertiser Conversions"
from
-----------flight Path
	(
    select
      date_in_et,
      site_id,
      sum(session_count) as "Flight Path Visits"
    from intent_media_production.air_ct_flight_path_visit_aggregations
    where date_in_et < date(current_timestamp at timezone 'America/New_York')
    group by
      date_in_et,
      site_id
	) as fpvs

join
----------------Publisher Conversions
	(
    select
      aggregation_level_date_in_et as date_in_et,
      site_id,
      sum(bookings_count) as conversions
    from intent_media_production.air_ct_transaction_performance_aggregations
    where segmentation_type IS NULL
      and pure_group_type IS NULL
      and aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York')
    group by
      aggregation_level_date_in_et,
      site_id
	) as convs
on fpvs.date_in_et = convs.date_in_et and fpvs.site_id = convs.site_id

join
-------------Advertiser Conversions
	(
    select
      apra.date_in_et,
      apra.advertiser_id,
      (CASE apra.advertiser_id
        WHEN 59777 THEN 6
        WHEN 61224 THEN 2
      END) as site_id,
      c.precheck_eligibility_type,
	  c.display_format,
      sum(apra.click_count) as clicks,
      sum(apra.click_conversion_count) as click_conversions
    from intent_media_production.air_ct_advertiser_performance_report_aggregations apra
    left join intent_media_production.campaigns c on apra.advertiser_id = c.advertiser_id
    where apra.advertiser_id IN (59777, 61224)
      and apra.date_in_et < date(current_timestamp at timezone 'America/New_York')
    group by
      apra.date_in_et,
      apra.advertiser_id,
      c.precheck_eligibility_type,
	  c.display_format
	) as adv
on fpvs.date_in_et = adv.date_in_et and fpvs.site_id = adv.site_id

join intent_media_production.sites on fpvs.site_id = sites.id