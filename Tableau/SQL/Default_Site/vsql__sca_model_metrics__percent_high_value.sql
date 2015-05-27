select
	requested_at_date_in_et as Date,
	site_type as Site,
	au.name as "Ad Unit",
	treatment as Treatment,
	model_slice_id as "Model Slice ID",
	ad_calls.product_category_type as "Product Category Type",
	sum(case when segmentation_type = 'HIGH_CONVERTING' then 1 else 0 end) as 'High Value Ad Calls',
	count(1) as 'Ad Calls'
from intent_media_log_data_production.ad_calls
left join intent_media_production.ad_units au on au.id = ad_calls.ad_unit_id
where ip_address_blacklisted = 0 and
((ad_calls.product_category_type = 'FLIGHTS' and site_type in ('EXPEDIA', 'EXPEDIA_CA', 'CHEAPTICKETS', 'ORBITZ_GLOBAL','OPODO_UK')) or
(ad_calls.product_category_type = 'HOTELS' and site_type in ('EXPEDIA', 'EXPEDIA_CA', 'HOTWIRE')))
and ad_unit_type = 'CT'
group by
	requested_at_date_in_et,
	site_type,
	au.name,
	treatment,
	model_slice_id,
	ad_calls.product_category_type