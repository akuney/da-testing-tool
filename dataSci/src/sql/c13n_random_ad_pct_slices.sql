-- Query for generating BRDs for the various Randomized Ad % slices

select
model_slice_id,
segmentation_model_threshold,
pure_group_type,
count(*) as visitors,
sum(is_booker) as bookers,
sum(total_conversion_value) as gross_profit
from
(select
uvs.publisher_user_id as publisher_user_id,
uvs.model_slice_id as model_slice_id,
min(uvs.segmentation_model_threshold) as segmentation_model_threshold,
min(pure_group_type) as pure_group_type,
max(case 
	when conversions.net_conversion_value > 0 then 1 
	else 0
	end) as is_booker,
sum(conversions.net_conversion_value) as total_conversion_value	
from
	(select 
	publisher_user_id,
	min(pure_group_type) as pure_group_type,
	max(model_slice_id) as model_slice_id,
	min(requested_at) as first_ad_call_time,
	FLOOR(min(segmentation_model_threshold) * 10) as segmentation_model_threshold
	from 
	intent_media_log_data_production.ad_calls 
	where 
	ad_unit_type='CT' 
	and site_type = 'HOTWIRE' --'CHEAPTICKETS'
	and product_category_type = 'HOTELS' --'FLIGHTS'
	and requested_at_date_in_et between '2014-10-09' and '2015-01-15'
	and ip_address_blacklisted = 0
	and (model_slice_id in (274, 275, 278, 279)  or pure_group_type = 'PURE')
	group by 1) as 
uvs left join
	(select
	publisher_user_id,
	net_conversion_value,
	requested_at
	from
	intent_media_log_data_production.conversions
	where
	site_type = 'HOTWIRE' --'CHEAPTICKETS'
	and entity_id = 85 --55
	and requested_at_date_in_et between '2014-10-09' and '2015-01-15'
	and product_category_type = 'HOTELS' --'FLIGHTS'
	and ip_address_blacklisted = 0) as
conversions
on uvs.publisher_user_id = conversions.publisher_user_id
and uvs.first_ad_call_time < conversions.requested_at
group by 1,2) as 
pulled_data
group by 1, 2, 3
order by 1, 2, 3;
