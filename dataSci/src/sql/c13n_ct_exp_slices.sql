select
model_slice_id,
count(*) as visitors,
sum(is_booker) as bookers,
sum(total_conversion_value) as gross_profit
from
(select
uvs.publisher_user_id as publisher_user_id,
uvs.model_slice_id as model_slice_id,
max(case 
	when conversions.net_conversion_value > 0 then 1 
	else 0
	end) as is_booker,
sum(conversions.net_conversion_value) as total_conversion_value	
from
	(select 
	publisher_user_id,
	max(model_slice_id) as model_slice_id,
	min(requested_at) as first_ad_call_time
	from 
	intent_media_log_data_production.ad_calls 
	where 
	ad_unit_type='CT' 
	-- and ad_unit_id in (89,116)
	and site_type = 'EXPEDIA'
	and product_category_type = 'HOTELS'
	and requested_at_date_in_et between '2014-05-01' and '2014-06-06'
	and ip_address_blacklisted = 0
	group by 1) as 
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
	and requested_at_date_in_et between '2014-05-01' and '2014-06-06'
	and product_category_type = 'HOTELS'
	and ip_address_blacklisted = 0) as
conversions
on uvs.publisher_user_id = conversions.publisher_user_id
and uvs.first_ad_call_time < conversions.requested_at
group by 1,2) as 
pulled_data
group by 1
order by 1;