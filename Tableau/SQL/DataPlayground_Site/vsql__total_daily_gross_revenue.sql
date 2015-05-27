select 
	ssn.date as Date,
	ssn.gross_media_revenue as "SSN Gross Media Revenue",
	ifnull(aft.gross_media_revenue, 0.0) as "AfT Gross Media Revenue",
	ifnull(hotel_aft.gross_media_revenue, 0.0) as "Hotel AfT Gross Media Revenue",
	ifnull(hotel_meta.gross_media_revenue,0.0) as "Hotel Meta Gross Media Revenue",
	ifnull(ssn.gross_media_revenue, 0.0) + ifnull(aft.gross_media_revenue, 0.0) + ifnull(hotel_aft.gross_media_revenue, 0.0) + ifnull(hotel_meta.gross_media_revenue,0.0) as "Total Daily Gross Revenue"
from
	(select
		aggregation_level_date_in_et as date,
		sum(gross_actual_cpc_sum) as gross_media_revenue
	from intent_media_production.publisher_performance_report_aggregations
	group by aggregation_level_date_in_et) ssn
left join
	(select 
		aggregation_level_date_in_et as date,
		sum(gross_revenue_sum) as gross_media_revenue
	from intent_media_production.air_ct_media_performance_aggregations
	where aggregation_level_date_in_et >= '2011-05-23'
	group by aggregation_level_date_in_et) aft
on aft.date = ssn.date
left join
	(select 
		aggregation_level_date_in_et as date,
		sum(gross_revenue_sum) as gross_media_revenue
	from intent_media_production.hotel_ct_media_performance_aggregations
	where aggregation_level_date_in_et >= '2012-12-10'
	group by aggregation_level_date_in_et) hotel_aft
on hotel_aft.date = ssn.date
left join
	(select
		aggregation_level_date_in_et as date,
		sum(gross_revenue_sum) as gross_media_revenue
	from intent_media_production.hotel_meta_media_performance_aggregations
	group by aggregation_level_date_in_et) hotel_meta
on hotel_meta.date = ssn.date
where ssn.date < date(current_timestamp at timezone 'America/New_York')
order by ssn.date desc