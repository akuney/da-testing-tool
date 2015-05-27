--WITH SDate AS (select '12/01/2013' as Start_date)
--select Start_date from SDate
---FROM intent_media_production.publisher_performance_report_aggregations a;

select 
	dimensions.date_in_et as TMONTH,
	SUM(ifnull(ssn.gross_media_revenue,0.0)) as "SSN Gross Media Revenue",
	SUM(ifnull(aft.gross_media_revenue, 0.0)) as "AfT Gross Media Revenue",
	SUM(ifnull(hotel_aft.gross_media_revenue, 0.0)) as "Hotel AfT Gross Media Revenue",
	SUM(ifnull(hotel_meta.gross_media_revenue,0.0)) as "Hotel Meta Gross Media Revenue",
	SUM(ifnull(ssn.gross_media_revenue, 0.0) + ifnull(aft.gross_media_revenue, 0.0) + ifnull(hotel_aft.gross_media_revenue, 0.0) + ifnull(hotel_meta.gross_media_revenue,0.0)) as "Total Daily Gross Revenue",
        
 -----CLICKS       
        SUM(ifnull(ssn.click_count,0.0)) as "SSN Click Count",
	SUM(ifnull(aft.click_count, 0.0)) as "AfT Click Count",
	SUM(ifnull(hotel_aft.click_count, 0.0)) as "Hotel AfT Click Count",
	SUM(ifnull(hotel_meta.click_count,0.0)) as "Hotel Click Count",
	SUM(ifnull(ssn.click_count, 0.0) + ifnull(aft.click_count, 0.0) + ifnull(hotel_aft.click_count, 0.0) + ifnull(hotel_meta.click_count,0.0)) as "Total Click Count"
from
	
-- dimensions

(select *
from
	(select
		distinct(aggregation_level_date_in_et) as date_in_et
	from intent_media_production.publisher_performance_report_aggregations a
	where aggregation_level_date_in_et >= TO_DATE('12/01/2013', 'mm/dd/yyyy')
	  AND aggregation_level_date_in_et < TO_DATE('02/01/2014', 'mm/dd/yyyy')) dates,
	(select
        distinct(e.name) as publisher
	from intent_media_production.sites s
    left join intent_media_production.entities e on e.id = s.publisher_id) sites) dimensions

-- ssn
left join
	(select
		aggregation_level_date_in_et as date_in_et,
		e.name as publisher,
		sum(gross_actual_cpc_sum) as gross_media_revenue
		, sum(click_count) as click_count
	from intent_media_production.publisher_performance_report_aggregations a
	left join intent_media_production.ad_units au on au.id = a.ad_unit_id
	left join intent_media_production.sites s on s.id = au.site_id
	left join intent_media_production.entities e on e.id = s.publisher_id
	       	where aggregation_level_date_in_et >= TO_DATE('12/01/2013', 'mm/dd/yyyy')
	         AND aggregation_level_date_in_et < TO_DATE('02/01/2014', 'mm/dd/yyyy')
	group by
		aggregation_level_date_in_et,
		e.name) ssn
on ssn.date_in_et = dimensions.date_in_et
and ssn.publisher = dimensions.publisher

-- aft flights
left join
	(select 
		aggregation_level_date_in_et as date_in_et,
		e.name as publisher,
		sum(gross_revenue_sum) as gross_media_revenue
		, sum(click_count) as click_count
	from intent_media_production.air_ct_media_performance_aggregations a
	left join intent_media_production.ad_units au on au.id = a.ad_unit_id
	left join intent_media_production.sites s on s.id = au.site_id
	left join intent_media_production.entities e on e.id = s.publisher_id
	       	where aggregation_level_date_in_et >= TO_DATE('12/01/2013', 'mm/dd/yyyy')
	         AND aggregation_level_date_in_et < TO_DATE('02/01/2014', 'mm/dd/yyyy')
	group by
		aggregation_level_date_in_et,
		e.name) aft
on aft.date_in_et = dimensions.date_in_et
and aft.publisher = dimensions.publisher

-- aft hotels
left join
	(select 
		aggregation_level_date_in_et as date_in_et,
		e.name as publisher,
		sum(gross_revenue_sum) as gross_media_revenue
		, sum(click_count) as click_count
	from intent_media_production.hotel_ct_media_performance_aggregations a
	left join intent_media_production.ad_units au on au.id = a.ad_unit_id
	left join intent_media_production.sites s on s.id = au.site_id
	left join intent_media_production.entities e on e.id = s.publisher_id
	       	where aggregation_level_date_in_et >= TO_DATE('12/01/2013', 'mm/dd/yyyy')
	         AND aggregation_level_date_in_et < TO_DATE('02/01/2014', 'mm/dd/yyyy')
	group by
		aggregation_level_date_in_et,
		e.name) hotel_aft
on hotel_aft.date_in_et = dimensions.date_in_et
and hotel_aft.publisher = dimensions.publisher

-- hotel meta
left join
	(select
		aggregation_level_date_in_et as date_in_et,
		e.name as publisher,
		sum(gross_revenue_sum) as gross_media_revenue
		, sum(click_count) as click_count
	from intent_media_production.hotel_meta_media_performance_aggregations a
	left join intent_media_production.ad_units au on au.id = a.ad_unit_id
	left join intent_media_production.sites s on s.id = au.site_id
	left join intent_media_production.entities e on e.id = s.publisher_id
	       	where aggregation_level_date_in_et >= TO_DATE('12/01/2013', 'mm/dd/yyyy')
	         AND aggregation_level_date_in_et < TO_DATE('02/01/2014', 'mm/dd/yyyy')
	group by
		aggregation_level_date_in_et,
		e.name) hotel_meta
on hotel_meta.date_in_et = dimensions.date_in_et
and hotel_meta.publisher = dimensions.publisher

GROUP BY (dimensions.date_in_et)
ORDER BY (dimensions.date_in_et)  

--order by dimensions.date_in_et desc