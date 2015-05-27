select
	site_type as "Site",
	created_at as "Date",
	ifnull(model_slice_id,'Total') as "Model Slice ID",
	treatment as "Treatment",
	metric_type as "Metric Type",
	operation_point as "Operation Point",
	user_conversion_type as "User Conversion Type",
	ad_call_product_category_type as "Path Product Category Type",
	product_category_type as "Metric Product Category Type",
	revenue as "Percent of Revenue",
	bookers as "Percent of Bookers"	
from segmentation_model_metrics