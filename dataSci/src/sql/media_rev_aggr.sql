SELECT 
sum(served_page_count) Served_Pages,
sum(gross_revenue_sum)*0.7 Net_Media_Contribution
FROM
intent_media_production.hotel_meta_media_performance_aggregations
WHERE
-- ad_unit_id = '116' and -- w/o getaroom
-- ad_unit_id = '89' and -- w/ getaroom
site_id = '2' and -- 2 ORBITZ_GLOBAL, 3 CHEAPTICKETS, 4 EBOOKERS
aggregation_level_date_in_et BETWEEN '2014-07-31' AND '2014-09-22'
-- aggregation_level_date_in_et between '2014-08-23' AND '2014-09-02'
-- aggregation_level_date_in_et between '2014-06-20' AND '2014-08-04'
and aggregation_level_date_in_et not in ('2014-09-10', '2014-09-11')
;

SELECT 
sum(served_page_count) Served_Pages,
sum(gross_revenue_sum)*0.7 Net_Media_Contribution
FROM
intent_media_production.hotel_meta_media_performance_aggregations
WHERE
-- ad_unit_id = '129' and 
site_id = '2' and -- ORBITZ_GLOBAL
aggregation_level_date_in_et between '2014-04-01' and '2014-05-26'
;


SELECT 
sum(ad_unit_served_count) Page_Impressions,
sum(net_revenue_sum) Net_Media_Contribution
FROM
intent_media_production.air_ct_media_performance_aggregations
WHERE
site_id = '6'
AND
aggregation_level_date_in_et BETWEEN '2014-07-01' AND '2014-07-31'
;

SELECT 
sum(served_ad_count) Page_Impressions,
sum(net_revenue_sum) Net_Media_Contribution
FROM
intent_media_production.hotel_ct_media_performance_aggregations
WHERE
site_id = '6'
AND
aggregation_level_date_in_et BETWEEN '2014-01-01' AND '2014-02-28'
-- aggregation_level_date_in_et > '2013-09-24'
;


select
site_reporting_value_02,
sum(actual_cpc)*0.7 as net_media_rev
from 
(
	(select 
	request_id,
	site_reporting_value_02
	from 
	intent_media_log_data_production.ad_calls 
	where 
	ad_unit_type='META' 
	and site_type = 'ORBITZ_GLOBAL'
	and requested_at_date_in_et between '2014-07-31' and '2014-08-04'
	and ip_address_blacklisted = false
	group by 1,2) as 
a left join
	(select
	ad_call_request_id,
	actual_cpc
	from
	intent_media_log_data_production.clicks
	where
	site_type = 'ORBITZ_GLOBAL'
	and requested_at_date_in_et between '2014-07-31' and '2014-08-04'
	and product_category_type = 'HOTELS'
	and ip_address_blacklisted = false
	and fraudulent = false) as
cl
on a.request_id = cl.ad_call_request_id) as t1
group by 1;


select
mvt_value,
sum(actual_cpc)*0.7 as net_media_rev
from 
(
	(select 
	request_id,
	-- CASE INSTR(multivariate_test_attributes,'HOTEL_META_INTERCARD_DESIGN',1) WHEN 0 THEN 'Not_found' ELSE SUBSTR(multivariate_test_attributes,INSTR(multivariate_test_attributes,'HOTEL_META_INTERCARD_DESIGN',1)+LENGTH('HOTEL_META_INTERCARD_DESIGN')+3,CASE INSTR(multivariate_test_attributes,'","',INSTR(multivariate_test_attributes,'HOTEL_META_INTERCARD_DESIGN',1)) WHEN 0 THEN LENGTH(multivariate_test_attributes)-INSTR(multivariate_test_attributes,'HOTEL_META_INTERCARD_DESIGN',1)-LENGTH('HOTEL_META_INTERCARD_DESIGN')-4 ELSE INSTR(multivariate_test_attributes,'","',INSTR(multivariate_test_attributes,'HOTEL_META_INTERCARD_DESIGN',1))-INSTR(multivariate_test_attributes,'HOTEL_META_INTERCARD_DESIGN',1)-LENGTH('HOTEL_META_INTERCARD_DESIGN')-3 END) END AS mvt_value
        CASE INSTR(multivariate_test_attributes,'HOTEL_META_INTERCARD_POSITION',1) WHEN 0 THEN 'Not_found' ELSE SUBSTR(multivariate_test_attributes,INSTR(multivariate_test_attributes,'HOTEL_META_INTERCARD_POSITION',1)+LENGTH('HOTEL_META_INTERCARD_POSITION')+3,CASE INSTR(multivariate_test_attributes,'","',INSTR(multivariate_test_attributes,'HOTEL_META_INTERCARD_POSITION',1)) WHEN 0 THEN LENGTH(multivariate_test_attributes)-INSTR(multivariate_test_attributes,'HOTEL_META_INTERCARD_DESIGN',1)-LENGTH('HOTEL_META_INTERCARD_POSITION')-4 ELSE INSTR(multivariate_test_attributes,'","',INSTR(multivariate_test_attributes,'HOTEL_META_INTERCARD_POSITION',1))-INSTR(multivariate_test_attributes,'HOTEL_META_INTERCARD_POSITION',1)-LENGTH('HOTEL_META_INTERCARD_POSITION')-3 END) END AS mvt_value
	from 
	intent_media_log_data_production.ad_calls 
	where 
	ad_unit_type='META' 
	and site_type = 'ORBITZ_GLOBAL'
	and requested_at_date_in_et between '2014-08-01' and '2014-08-19'
	and ip_address_blacklisted = false
	and site_reporting_value_02 = 'testA'
	and multivariate_version_id >= 1610
	group by 1,2) as 
a left join
	(select
	ad_call_request_id,
	actual_cpc
	from
	intent_media_log_data_production.clicks
	where
	site_type = 'ORBITZ_GLOBAL'
	and requested_at_date_in_et between '2014-08-01' and '2014-08-19'
	and product_category_type = 'HOTELS'
	and ip_address_blacklisted = false
	and fraudulent = false) as
cl
on a.request_id = cl.ad_call_request_id) as t1
group by 1
order by 1;


select
site_reporting_value_02,
sum(actual_cpc)*0.7 as net_media_rev
from 
(
	(select 
	request_id,
        site_reporting_value_02
	from 
	intent_media_log_data_production.ad_calls 
	where 
	ad_unit_type='META' 
	and site_type = 'ORBITZ_GLOBAL'
	and requested_at_date_in_et between '2014-08-23' and '2014-09-22'
	and requested_at_date_in_et not in ('2014-09-10', '2014-09-11')
	and ip_address_blacklisted = false
	group by 1,2) as 
a left join
	(select
	ad_call_request_id,
	actual_cpc
	from
	intent_media_log_data_production.clicks
	where
	site_type = 'ORBITZ_GLOBAL'
	and requested_at_date_in_et between '2014-08-23' and '2014-09-22'
	and product_category_type = 'HOTELS'
	and ip_address_blacklisted = false
	and fraudulent = false) as
cl
on a.request_id = cl.ad_call_request_id) as t1
group by 1
order by 1;