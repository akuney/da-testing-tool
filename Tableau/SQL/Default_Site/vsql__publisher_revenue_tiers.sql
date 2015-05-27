select
	'SSN' as Network,
	'Hotels' as "Product Category",
	aggregation_level_date_in_et as Date,
	(case 
		when e.name = 'Orbitz' then 'OWW' 
		when e.name = 'Expedia' then 'Expedia Inc.'
		else e.name 
	end) as Publisher,
	(case s.name 
		when 'EXPEDIA' then 'Expedia'
		when 'CHEAPTICKETS' then 'CheapTickets'
		when 'ORBITZ_GLOBAL' then 'Orbitz'
		when 'BUDGETAIR' then 'BudgetAir'
		when 'VAYAMA' then 'Vayama'
		when 'HOTWIRE' then 'Hotwire'
		when 'KAYAK' then 'Kayak'
		when 'BOOKIT' then 'Bookit'
		else s.name
	end) as "Site",
	sum(gross_actual_cpc_sum) as "Gross Media Revenue"
from intent_media_production.publisher_performance_report_aggregations ppra
left join intent_media_production.entities e on e.id = ppra.publisher_id
left join intent_media_production.ad_units au on au.id = ppra.ad_unit_id
left join intent_media_production.sites s on s.id = au.site_id
where 
	((e.name = 'Orbitz' and aggregation_level_date_in_et >= '2013-01-01') or
	(e.name = 'Travelocity' and aggregation_level_date_in_et >= '2013-01-01')) and
	(aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York'))
group by aggregation_level_date_in_et, e.name, s.name

union

select
	'AfT' as Network,
	'Flights' as "Product Category",
	aggregation_level_date_in_et as Date,
	(case 
		when e.name = 'Orbitz' then 'OWW' 
		when e.name = 'Expedia' then 'Expedia Inc.'
		else e.name 
	end) as Publisher,
	(case s.name 
		when 'EXPEDIA' then 'Expedia'
		when 'CHEAPTICKETS' then 'CheapTickets'
		when 'ORBITZ_GLOBAL' then 'Orbitz'
		when 'BUDGETAIR' then 'BudgetAir'
		when 'VAYAMA' then 'Vayama'
		when 'HOTWIRE' then 'Hotwire'
		when 'KAYAK' then 'Kayak'
		when 'BOOKIT' then 'Bookit'
		else s.name
	end) as "Site",
	sum(gross_revenue_sum) as "Gross Media Revenue"
from intent_media_production.air_ct_media_performance_aggregations acmpa
left join intent_media_production.sites s on s.id = acmpa.site_id
left join intent_media_production.entities e on e.id = s.publisher_id
where
	((e.name = 'Orbitz' and aggregation_level_date_in_et >= '2013-01-01') or
	(e.name = 'Expedia' and aggregation_level_date_in_et >= '2013-04-01') or
	(e.name = 'Hotwire' and aggregation_level_date_in_et >= '2013-04-01') or
	(e.name = 'Airtrade International' and aggregation_level_date_in_et >= '2013-01-17') or
	(e.name = 'Bookit' and aggregation_level_date_in_et >= '2012-08-29') or
	(e.name = 'Kayak Software Corporation' and aggregation_level_date_in_et >= '2013-02-01') or
	(e.name = 'TripAdvisor' and aggregation_level_date_in_et >= '2013-08-01') or
	(e.name = 'Travelzoo' and aggregation_level_date_in_et >= '2013-04-19') or
	(e.name = 'Fareportal') or
	(e.name = 'Travelocity' and aggregation_level_date_in_et >= '2013-10-01') or
	(e.name = 'Oversee' and aggregation_level_date_in_et >= '2013-08-30') or
	(e.name = 'Webjet' and aggregation_level_date_in_et >= '2013-09-04') or
	(e.name = 'Hipmunk' and aggregation_level_date_in_et >= '2013-09-03')) and
	(aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York'))
group by aggregation_level_date_in_et, e.name, s.name

union

select
	'AfT' as Network,
	'Hotels' as "Product Category",
	aggregation_level_date_in_et as Date,
	(case 
		when e.name = 'Orbitz' then 'OWW' 
		when e.name = 'Expedia' then 'Expedia Inc.'
		else e.name 
	end) as Publisher,
	(case s.name 
		when 'EXPEDIA' then 'Expedia'
		when 'CHEAPTICKETS' then 'CheapTickets'
		when 'ORBITZ_GLOBAL' then 'Orbitz'
		when 'BUDGETAIR' then 'BudgetAir'
		when 'VAYAMA' then 'Vayama'
		when 'HOTWIRE' then 'Hotwire'
		when 'KAYAK' then 'Kayak'
		when 'BOOKIT' then 'Bookit'
		else s.name
	end) as "Site",
	sum(gross_revenue_sum) as "Gross Media Revenue"
from intent_media_production.hotel_ct_media_performance_aggregations acmpa
left join intent_media_production.sites s on s.id = acmpa.site_id
left join intent_media_production.entities e on e.id = s.publisher_id
where
	((e.name = 'Orbitz' and aggregation_level_date_in_et >= '2013-01-01') or
	(e.name = 'Expedia' and aggregation_level_date_in_et >= '2013-04-01') or
	(e.name = 'Hotwire' and aggregation_level_date_in_et >= '2013-04-01') or
	(e.name = 'Airtrade International' and aggregation_level_date_in_et >= '2013-01-17') or
	(e.name = 'Bookit' and aggregation_level_date_in_et >= '2012-08-29') or
	(e.name = 'Kayak Software Corporation' and aggregation_level_date_in_et >= '2013-02-01') or
	(e.name = 'TripAdvisor' and aggregation_level_date_in_et >= '2013-08-01') or
	(e.name = 'Travelzoo' and aggregation_level_date_in_et >= '2013-04-19') or
	(e.name = 'Fareportal') or
	(e.name = 'Travelocity' and aggregation_level_date_in_et >= '2013-10-01') or
	(e.name = 'Oversee' and aggregation_level_date_in_et >= '2013-08-30') or
	(e.name = 'Webjet' and aggregation_level_date_in_et >= '2013-09-04') or
	(e.name = 'Hipmunk' and aggregation_level_date_in_et >= '2013-09-03')) and
	(aggregation_level_date_in_et < date(current_timestamp at timezone 'America/New_York'))
group by aggregation_level_date_in_et, e.name, s.name