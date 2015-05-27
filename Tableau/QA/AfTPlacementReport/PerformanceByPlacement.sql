
select sum(ad_call_count) as "page views",
sum(not_pure_ad_call_count)/sum(ad_call_count) as "% NP",
sum(not_pure_low_converting_ad_call_count)/sum(not_pure_ad_call_count) as "% Low Converting",
sum(not_pure_low_converting_ad_call_count)/sum(ad_call_count) as "% NP & Low Converting",
1 - sum(suppressed_by_route)/sum(not_pure_low_converting_ad_call_count) as "% Qualified OD",
1 - sum(suppressed_by_c13n_segment)/sum(not_pure_low_converting_ad_call_count) as "% Qualified C13n",
1 - sum(suppressed_by_click_blackout)/sum(not_pure_low_converting_ad_call_count) as "% Qualified click blackout",
1- (sum(suppressed_by_publisher_traffic_share))/ sum(not_pure_low_converting_ad_call_count) as "% Qualified Traffic Share",
1- (sum(not_pure_low_converting_ad_call_count) - sum(not_pure_low_converting_addressable_ad_call_count) - sum(suppressed_by_route) - sum(suppressed_by_c13n_segment) - sum(suppressed_by_click_blackout) - sum(suppressed_by_publisher_traffic_share)) /  sum(not_pure_low_converting_ad_call_count)as "% Qualified Other",
1- ( sum(not_pure_low_converting_ad_call_count) - sum(not_pure_low_converting_addressable_ad_call_count))/  sum(not_pure_low_converting_ad_call_count) as "% Qualified All Rules",
sum(not_pure_low_converting_addressable_ad_call_count)/sum(ad_call_count) as  "% total addressable",
sum(not_pure_low_converting_addressable_ad_call_count) as "addressable page views",
sum(not_pure_low_converting_ad_call_with_ads_count) / sum(not_pure_low_converting_addressable_ad_call_count) as "fill rate",
sum(not_pure_low_converting_ad_call_with_ads_count) as "fillable pages",
1 - sum(suppressed_by_c13n_above_threshold)/sum(not_pure_low_converting_ad_call_with_ads_count) as "% Qualified Monetization Threshold" ,
sum(ad_unit_served_count) as "pages served",
sum(interaction_count)/sum(ad_unit_served_count) as "interaction rate",
sum(interaction_count) as "interactions",
sum(click_count)/sum(interaction_count) as "clicks per interaction",
sum(click_count)/sum(ad_unit_served_count) as "CTR",
sum(click_count) as "clicks", 
sum(gross_revenue_sum) as "Gross Media Revenue",
sum(gross_revenue_sum)/sum(not_pure_low_converting_addressable_ad_call_count) * 1000 as "available eCPM",
sum(gross_revenue_sum)/sum(ad_unit_served_count) * 1000 as "served eCPM"
from air_ct_media_performance_aggregations
--  by day
 where aggregation_level_date_in_et = '20121024'
-- by week
--  where aggregation_level_date_in_et between ('20121008') and ('20121015')
 and ad_unit_id=9
;

-- below will give the page level statistics.
select air_ct_placement_type_performance_aggregations.placement_type,
 air_ct_placement_type_performance_aggregations.interaction_count/sum(air_ct_media_performance_aggregations.ad_unit_served_count) as "interaction rate",
 air_ct_placement_type_performance_aggregations.interaction_count as "interactions",
air_ct_placement_type_performance_aggregations.click_count/air_ct_placement_type_performance_aggregations.interaction_count as "clicks per interaction",
air_ct_placement_type_performance_aggregations.click_count/sum(air_ct_media_performance_aggregations.ad_unit_served_count) as "CTR",
air_ct_placement_type_performance_aggregations.click_count as "clicks", 
air_ct_placement_type_performance_aggregations.actual_cpc_sum as "Gross Media Revenue",
air_ct_placement_type_performance_aggregations.actual_cpc_sum/sum(air_ct_media_performance_aggregations.not_pure_low_converting_addressable_ad_call_count) * 1000 as "available eCPM",
air_ct_placement_type_performance_aggregations.actual_cpc_sum/sum(air_ct_media_performance_aggregations.ad_unit_served_count) * 1000 as "served eCPM"
from air_ct_media_performance_aggregations  
join air_ct_placement_type_performance_aggregations 
	on air_ct_media_performance_aggregations.ad_unit_id = air_ct_placement_type_performance_aggregations.ad_unit_id and
	air_ct_media_performance_aggregations.aggregation_level_date_in_et = air_ct_placement_type_performance_aggregations.date_in_et
where air_ct_media_performance_aggregations.aggregation_level_date_in_et = '20121024'
and air_ct_media_performance_aggregations.ad_unit_id=8
group by air_ct_placement_type_performance_aggregations.placement_type
;

