select
	dimensions.*,
	data."Pages Available", 
	data."Pages Served", 
	data."Spend", 
	data."Clicks", 
	data."Impressions Served"
from
(select *
from
(select 
	distinct(aggregation_level_date_in_et) as Date,
	0 as Zero
from intent_media_production.publisher_performance_report_aggregations
where aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York')) dates,
(select
	imm.name as "Market Name",
	imm.report_segment as "Segment"
from intent_media_production.intent_media_markets imm
union
select 'Other' as "Market Name",
	'Other' as "Segment") markets,
(select 
	(case when intent_media_production.entities.name = 'Orbitz' then 'OWW' else intent_media_production.entities.name end) as Pub,
	sites.display_name AS "Site",
	(case
		when intent_media_production.ad_units.name = 'Travelocity Martini Package Page' then 'Total Flight-Hotel Cross-Sell Page'
		when intent_media_production.ad_units.name = 'Travelocity List Page (Legacy)' then 'Total Hotel List Page'
		when intent_media_production.ad_units.name = 'Travelocity List Page (New Platform)' then 'Total Hotel List Page'
		when intent_media_production.ad_units.name like '%SEM%' then 'Total SEM List Page'
		when intent_media_production.ad_units.name like '%Hotel List Page%' then 'Total Hotel List Page'
		when intent_media_production.ad_units.name like '%Hotel Details Page%' then 'Total Hotel Details Page'
		when intent_media_production.ad_units.name like '%Trip Details Page%' then 'Total Trip Details Page'
		when intent_media_production.ad_units.name like '%Package Page%' then 'Total Packages List Page'
		else intent_media_production.ad_units.name
	end) as "Type of Ad Unit",
	intent_media_production.ad_units.name as "Ad Unit"
from intent_media_production.publisher_performance_report_aggregations ppra
left join intent_media_production.ad_units on ppra.ad_unit_id = ad_units.id
left join intent_media_production.sites on intent_media_production.sites.id = intent_media_production.ad_units.site_id
left join intent_media_production.entities on intent_media_production.entities.id = intent_media_production.sites.publisher_id
where ad_type = 'SSR'
group by 
(case when intent_media_production.entities.name = 'Orbitz' then 'OWW' else intent_media_production.entities.name end),
	sites.display_name,
	(case
		when intent_media_production.ad_units.name = 'Travelocity Martini Package Page' then 'Total Flight-Hotel Cross-Sell Page'
		when intent_media_production.ad_units.name = 'Travelocity List Page (Legacy)' then 'Total Hotel List Page'
		when intent_media_production.ad_units.name = 'Travelocity List Page (New Platform)' then 'Total Hotel List Page'
		when intent_media_production.ad_units.name like '%SEM%' then 'Total SEM List Page'
		when intent_media_production.ad_units.name like '%Hotel List Page%' then 'Total Hotel List Page'
		when intent_media_production.ad_units.name like '%Hotel Details Page%' then 'Total Hotel Details Page'
		when intent_media_production.ad_units.name like '%Trip Details Page%' then 'Total Trip Details Page'
		when intent_media_production.ad_units.name like '%Package Page%' then 'Total Packages List Page'
		else intent_media_production.ad_units.name
	end),
	intent_media_production.ad_units.name
) ad_unit_names) dimensions
left join
(
select 
	(case when intent_media_production.entities.name = 'Orbitz' then 'OWW' else intent_media_production.entities.name end) as Pub,
	sites.display_name as Site,
	(case
		when intent_media_production.ad_units.name = 'Travelocity Martini Package Page' then 'Total Flight-Hotel Cross-Sell Page'
		when intent_media_production.ad_units.name = 'Travelocity List Page (Legacy)' then 'Total Hotel List Page'
		when intent_media_production.ad_units.name = 'Travelocity List Page (New Platform)' then 'Total Hotel List Page'
		when intent_media_production.ad_units.name like '%SEM%' then 'Total SEM List Page'
		when intent_media_production.ad_units.name like '%Hotel List Page%' then 'Total Hotel List Page'
		when intent_media_production.ad_units.name like '%Hotel Details Page%' then 'Total Hotel Details Page'
		when intent_media_production.ad_units.name like '%Trip Details Page%' then 'Total Trip Details Page'
		when intent_media_production.ad_units.name like '%Package Page%' then 'Total Packages List Page'
		else intent_media_production.ad_units.name
	end) as "Type of Ad Unit",
	intent_media_production.ad_units.name as "Ad Unit",
	aggregation_level_date_in_et as Date, 
	ifnull(imm.name, 'Other') as "Market Name",
	ifnull(imm.report_segment, 'Other') as "Segment",
	sum(ad_call_count) as "Pages Available", 
	sum(case when positions_filled > 0 then ad_call_count else 0 end) as "Pages Served", 
	sum(gross_actual_cpc_sum) as "Spend", 
	sum(click_count) as "Clicks", 
	sum(ad_call_count*positions_filled) as "Impressions Served"
from intent_media_production.publisher_performance_report_aggregations ppra
left join intent_media_production.ad_units on intent_media_production.ad_units.id = ppra.ad_unit_id
left join intent_media_production.sites on intent_media_production.sites.id = intent_media_production.ad_units.site_id
left join intent_media_production.entities on intent_media_production.entities.id = intent_media_production.sites.publisher_id
left join intent_media_production.intent_media_markets_publisher_markets immpm on immpm.market_id = ppra.market_id
left join intent_media_production.intent_media_markets imm on imm.id = immpm.intent_media_market_id
where (case 
		when intent_media_production.sites.name = 'TRAVELOCITY'
			then aggregation_level_date_in_et >= '2011-04-01' and aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York')
		else
			aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York')
			and (((intent_media_production.ad_units.name like '%SEM%' or intent_media_production.ad_units.name like '%Hotel Details%') and aggregation_level_date_in_et >= '2012-05-15')
			or (intent_media_production.ad_units.name not like '%SEM%' and intent_media_production.ad_units.name not like '%Hotel Details%'))
	end)
group by intent_media_production.entities.name,
	sites.display_name,
	(case
		when intent_media_production.ad_units.name = 'Travelocity Martini Package Page' then 'Total Flight-Hotel Cross-Sell Page'
		when intent_media_production.ad_units.name = 'Travelocity List Page (Legacy)' then 'Total Hotel List Page'
		when intent_media_production.ad_units.name = 'Travelocity List Page (New Platform)' then 'Total Hotel List Page'
		when intent_media_production.ad_units.name like '%SEM%' then 'Total SEM List Page'
		when intent_media_production.ad_units.name like '%Hotel List Page%' then 'Total Hotel List Page'
		when intent_media_production.ad_units.name like '%Hotel Details Page%' then 'Total Hotel Details Page'
		when intent_media_production.ad_units.name like '%Trip Details Page%' then 'Total Trip Details Page'
		when intent_media_production.ad_units.name like '%Package Page%' then 'Total Packages List Page'
		else intent_media_production.ad_units.name
	end),
	intent_media_production.ad_units.name,
	aggregation_level_date_in_et, 
	ifnull(imm.name, 'Other'),
	ifnull(imm.report_segment, 'Other')
) data

on dimensions.Date = data.Date
and dimensions.Pub = data.Pub
and dimensions.Site = data.Site
and dimensions."Type of Ad Unit" = data."Type of Ad Unit"
and dimensions."Ad Unit" = data."Ad Unit"
and dimensions."Market Name" = data."Market Name"