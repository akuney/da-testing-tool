select
traffic_share_type,
hotel_meta_intercard_position,
count(*) as visitors,
sum(is_booker) as bookers,
sum(total_conversion_value) as gross_profit
from
(select
uvs.publisher_user_id as publisher_user_id,
uvs.traffic_share_type as traffic_share_type,
uvs.mvt_value as hotel_meta_intercard_position,
max(case 
	when conversions.net_conversion_value > 0 then 1 
	else 0
	end) as is_booker,
sum(conversions.net_conversion_value) as total_conversion_value		
from
	(select 
	publisher_user_id,
		CASE INSTR(multivariate_test_attributes,'HOTEL_META_INTERCARD_POSITION',1) WHEN 0 THEN 'Not_found' ELSE SUBSTR(multivariate_test_attributes,INSTR(multivariate_test_attributes,'HOTEL_META_INTERCARD_POSITION',1)+LENGTH('HOTEL_META_INTERCARD_POSITION')+3,CASE INSTR(multivariate_test_attributes,'","',INSTR(multivariate_test_attributes,'HOTEL_META_INTERCARD_POSITION',1)) WHEN 0 THEN LENGTH(multivariate_test_attributes)-INSTR(multivariate_test_attributes,'HOTEL_META_INTERCARD_DESIGN',1)-LENGTH('HOTEL_META_INTERCARD_POSITION')-4 ELSE INSTR(multivariate_test_attributes,'","',INSTR(multivariate_test_attributes,'HOTEL_META_INTERCARD_POSITION',1))-INSTR(multivariate_test_attributes,'HOTEL_META_INTERCARD_POSITION',1)-LENGTH('HOTEL_META_INTERCARD_POSITION')-3 END) END AS mvt_value,
	max(traffic_share_type) as traffic_share_type,
	min(requested_at_in_et) as first_ad_call_time
	from 
	intent_media_log_data_production.ad_calls 
	where 
	ad_unit_type='META' 
	and site_type = 'ORBITZ_GLOBAL'
	and requested_at_date_in_et between '2014-08-01' and '2014-08-19'
	and ip_address_blacklisted = 0
	and site_reporting_value_02 = 'testA'
	and multivariate_version_id >= 1610
	group by 1,2) as 
uvs left join
	(select
	publisher_user_id,
	net_conversion_value,
	requested_at_in_et
	from
	intent_media_log_data_production.conversions
	where
	site_type = 'ORBITZ_GLOBAL'
	and entity_id = 55
	and requested_at_date_in_et between '2014-08-01' and '2014-08-19'
	and product_category_type = 'HOTELS'
	and ip_address_blacklisted = 0) as
conversions
on uvs.publisher_user_id = conversions.publisher_user_id
and uvs.first_ad_call_time < conversions.requested_at_in_et
group by 1,2,3) as 
pulled_data
group by 1,2
order by 1,2;
