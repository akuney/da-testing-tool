select
pure_group_type,
mvt_value,
count(*) as visitors,
sum(is_booker) as bookers,
sum(total_conversion_value) as gross_profit
from
(select
uvs.publisher_user_id as publisher_user_id,
uvs.pure_group_type as pure_group_type,
uvs.mvt_value as mvt_value,
max(case 
	when conversions.net_conversion_value > 0 then 1 
	else 0
	end) as is_booker,
sum(conversions.net_conversion_value) as total_conversion_value		
from
	(select 
	publisher_user_id,
	CASE INSTR(multivariate_test_attributes,'CLICK_TYPE',1) WHEN 0 THEN 'Not_found' ELSE SUBSTR(multivariate_test_attributes,INSTR(multivariate_test_attributes,'CLICK_TYPE',1)+LENGTH('CLICK_TYPE')+3,CASE INSTR(multivariate_test_attributes,'","',INSTR(multivariate_test_attributes,'CLICK_TYPE',1)) WHEN 0 THEN LENGTH(multivariate_test_attributes)-INSTR(multivariate_test_attributes,'CLICK_TYPE',1)-LENGTH('CLICK_TYPE')-4 ELSE INSTR(multivariate_test_attributes,'","',INSTR(multivariate_test_attributes,'CLICK_TYPE',1))-INSTR(multivariate_test_attributes,'CLICK_TYPE',1)-LENGTH('CLICK_TYPE')-3 END) END AS mvt_value,
	max(pure_group_type) as pure_group_type,
	min(requested_at) as first_ad_call_time
	from 
	intent_media_log_data_production.ad_calls 
	where 
	ad_unit_type='CT' 
	and site_type = 'EXPEDIA'
	and product_category_type = 'FLIGHTS'
	and requested_at_date_in_et between '2014-03-19' and '2014-03-31'
	and ip_address_blacklisted = false
	and multivariate_version_id >= 880
	group by 1,2) as 
uvs left join
	(select
	publisher_user_id,
	net_conversion_value,
	requested_at
	from
	intent_media_log_data_production.conversions
	where
	site_type = 'EXPEDIA'
	and entity_id = 45
	and requested_at_date_in_et between '2014-03-19' and '2014-03-31'
	and product_category_type = 'FLIGHTS'
	and ip_address_blacklisted = false) as
conversions
on uvs.publisher_user_id = conversions.publisher_user_id
and uvs.first_ad_call_time < conversions.requested_at
group by 1,2,3) as 
pulled_data
group by 1,2;
