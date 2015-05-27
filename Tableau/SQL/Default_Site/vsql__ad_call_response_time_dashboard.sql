select 
	acsa.date_in_et as "Date", 
	acsa.ad_unit_id as "Ad Unit ID", 
	u.name as "Ad Unit Name",
	u.ad_type as "Ad Type",
	u.product_category_type as "Product Category Type",
	s.display_name as "Site Name",
	acsa.ad_call_count as "Ad Call Count",
	acsa.ad_calls_greater_than_two_hundred_ms_count as "Ad Calls Greater Than Two Hundred MS Count", 
	acsa.mean_response_time as "Mean Response Time", 
	acsa.ninety_fifth_percentile_response_time as "Average 95th Percentile Response Time",
	/* SLA = Service Level Agreement */
	(100 - (100 * (acsa.ad_calls_greater_than_two_hundred_ms_count / acsa.ad_call_count))) as "% under SLA",
	(case when (100 - (100 * (acsa.ad_calls_greater_than_two_hundred_ms_count / acsa.ad_call_count))) < 100 then 1 else 0 end) as "Count of Ad Units with less 100% SLA"
from intent_media_production.ad_call_statistics_aggregations acsa
left join intent_media_production.ad_units u on u.id = acsa.ad_unit_id
left join intent_media_production.sites s on s.id = u.site_id