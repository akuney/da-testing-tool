
-- SSN revenue by Page
select ad_unit_id,  sum(ad_call_count) pages_available, sum(if(positions_filled>0,ad_call_count,0)) pages_served, sum(gross_actual_cpc_sum) spend, sum(click_count) clicks, sum(ad_call_count*positions_filled) as impressions, sum(click_count)/sum(ad_call_count*positions_filled) as ad_ctr, sum(click_count)/sum(if(positions_filled>0,ad_call_count,0)) as page_ctr, 1000 * sum(gross_actual_cpc_sum)/sum(ad_call_count) as available_eCPM, 1000 * sum(gross_actual_cpc_sum)/sum(if(positions_filled>0,ad_call_count,0)) as served_eCPM, sum(gross_actual_cpc_sum)/sum(`click_count`) as cpc, sum(if(positions_filled>0,ad_call_count,0))/sum(ad_call_count) as fill_rate 
from publisher_performance_report_aggregations
where aggregation_level_date_in_et IN ('2012-07-12','2012-07-11','2012-07-10','2012-07-09','2012-07-08','2012-07-13','2012-07-14')
-- and ad_unit_id in (2,3,4,5,24,16,17,21,18,19)
group by 1;

-- SSN revenue by Page 
select ad_unit_id,  sum(ad_call_count) pages_available, sum(if(positions_filled>0,ad_call_count,0)) pages_served, sum(gross_actual_cpc_sum) spend, sum(click_count) clicks, sum(ad_call_count*positions_filled) as impressions, sum(click_count)/sum(ad_call_count*positions_filled) as ad_ctr, sum(click_count)/sum(if(positions_filled>0,ad_call_count,0)) as page_ctr, 1000 * sum(gross_actual_cpc_sum)/sum(ad_call_count) as available_eCPM, 1000 * sum(gross_actual_cpc_sum)/sum(if(positions_filled>0,ad_call_count,0)) as served_eCPM, sum(gross_actual_cpc_sum)/sum(`click_count`) as cpc, sum(if(positions_filled>0,ad_call_count,0))/sum(ad_call_count) as fill_rate 
from publisher_performance_report_aggregations
where aggregation_level_date_in_et BETWEEN ('2012-07-01') AND ('2012-07-14')
and ad_unit_id in (2,3,4,5,24,16,17,21)
group by 1;


-- SSN revenue by Page 
select ad_unit_id,  sum(ad_call_count) pages_available, sum(if(positions_filled>0,ad_call_count,0)) pages_served, sum(gross_actual_cpc_sum) spend, sum(click_count) clicks, sum(ad_call_count*positions_filled) as impressions, sum(click_count)/sum(ad_call_count*positions_filled) as ad_ctr, sum(click_count)/sum(if(positions_filled>0,ad_call_count,0)) as page_ctr, 1000 * sum(gross_actual_cpc_sum)/sum(ad_call_count) as available_eCPM, 1000 * sum(gross_actual_cpc_sum)/sum(if(positions_filled>0,ad_call_count,0)) as served_eCPM, sum(gross_actual_cpc_sum)/sum(`click_count`) as cpc, sum(if(positions_filled>0,ad_call_count,0))/sum(ad_call_count) as fill_rate 
from publisher_performance_report_aggregations
where aggregation_level_date_in_et BETWEEN ('2012-01-01') AND ('2012-07-14')
and ad_unit_id in (2,3,4,5,24,16,17,21)
group by 1;

-- SSN revenue by Page
select ad_unit_id,  avg(ad_call_count) pages_available, sum(if(positions_filled>0,ad_call_count,0)) pages_served, sum(gross_actual_cpc_sum) spend, sum(click_count) clicks, sum(ad_call_count*positions_filled) as impressions, sum(click_count)/sum(ad_call_count*positions_filled) as ad_ctr, sum(click_count)/sum(if(positions_filled>0,ad_call_count,0)) as page_ctr, 1000 * sum(gross_actual_cpc_sum)/sum(ad_call_count) as available_eCPM, 1000 * sum(gross_actual_cpc_sum)/sum(if(positions_filled>0,ad_call_count,0)) as served_eCPM, sum(gross_actual_cpc_sum)/sum(`click_count`) as cpc, sum(if(positions_filled>0,ad_call_count,0))/sum(ad_call_count) as fill_rate 
from publisher_performance_report_aggregations
where aggregation_level_date_in_et IN ('2012-07-12','2012-07-11','2012-07-10','2012-07-09','2012-07-08','2012-07-13','2012-07-14')
and ad_unit_id in (19)
group by 1;


-- SSN revenue by Page For 7 day moving avg
select ad_unit_id,  SUM(ad_call_count)/7 as avg_pages_available, avg(if(positions_filled>0,ad_call_count,0))/avg(ad_call_count) as fill_rate, 
avg(click_count)/avg(ad_call_count*positions_filled) as ad_ctr,
avg(gross_actual_cpc_sum)/avg(`click_count`) as cpc, 
sum(gross_actual_cpc_sum)/7 avg_spend, 
1000 * avg(gross_actual_cpc_sum)/avg(if(positions_filled>0,ad_call_count,0)) as served_eCPM
from publisher_performance_report_aggregations
where aggregation_level_date_in_et IN ('2012-07-12','2012-07-11','2012-07-10','2012-07-09','2012-07-08','2012-07-13','2012-07-14')
and ad_unit_id in (6)
group by 1;