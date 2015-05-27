select 
	aggregation_level_date_in_et as 'Date',
	au.name as 'Placement',
	e.name as 'Publisher',
	sum(ad_call_count) as 'Pages Available',
	sum((case when positions_filled>0 then ad_call_count else 0 end)) as 'Pages Served',
	sum(gross_actual_cpc_sum) as 'Spend',
	sum(click_count) as 'Clicks',
	sum(ad_call_count*positions_filled) as 'Impressions',
	sum(click_count)/sum(ad_call_count*positions_filled) as 'Ad CTR',
	sum(click_count)/sum((case when positions_filled>0 then ad_call_count else 0 end)) as 'Page CTR',
	1000*sum(gross_actual_cpc_sum)/sum(ad_call_count) as 'Available eCPM',
	1000*sum(gross_actual_cpc_sum)/sum((case when positions_filled>0 then ad_call_count else 0 end)) as 'Served eCPM'
from intent_media_production.publisher_performance_report_aggregations ppra
left join intent_media_production.ad_units au on ppra.ad_unit_id=au.id
left join intent_media_production.entities e on ppra.publisher_id=e.id
group by
	aggregation_level_date_in_et,
	au.name,
	e.name