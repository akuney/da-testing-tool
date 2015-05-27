select
	experiment_name as "Experiment Name",
	data_start_date_inclusive as "Start Date",
	data_end_date_inclusive as "End Date",
	(case clicked_type 
		when 'NOT_CLICKED' then 'Not Clicked'
		when 'CLICKED' then 'Clicked'
		else clicked_type
	end) as "Clicked Type",
	(case pure_group_type 
		when 'PURE' then 'Pure'
		when 'NOT_PURE' then 'Not Pure'
		when 'MULTIPLE' then 'Multiple'
		else pure_group_type
	end) as "Pure Group Type",
	training_group_type as "Training Group ID",
	(case publisher_type 
		when 'EXPEDIA' then 'Expedia'
		when 'ORBITZ_GLOBAL' then 'Orbitz'
		when 'CHEAPTICKETS' then 'CheapTickets'
		else publisher_type
	end) as "Pub",
	(case user_segmentation_type
		when 'DIRTY' then 'Mixed'
		when 'LOW_CONVERTING' then 'Low Converting'
		when 'HIGH_CONVERTING' then 'High Converting'
		else user_segmentation_type
	end) as "User Segmentation Type",
	segmentation_model_percentile as "Segmentation Model Percentile",
	model_slice_id as "Model Slice ID",
	treatment as "Treatment",
	'Flights' as "Product Category Type",
	number_users_total as "Number of Users",
	number_ad_calls_flights as "Pages Available",
	has_conversion_flights as "Has Conversions",
	number_conversions_flights as "Conversions",
	net_value_conversions_flights as "Conversion Net Value",
	has_insurance_conversion_flights as "Has Insurance Conversion",
	number_insurance_conversions_flights as "Insurance Conversions",
	net_value_insurance_conversions_flights as "Insurance Conversions Net Value",
	number_flight_attach_conversions_flights as "Flight Conversions Attached",
	number_flight_attach_conversions_hotels as "Hotel Conversions Attached",
	number_flight_attach_conversions_cars as "Car Conversions Attached",
	number_flight_attach_conversions_packages as "Package Conversions Attached",
	number_flight_attach_conversions_total as "Total Conversions Attached",
	net_media_revenue as "Net Media Revenue"
from intent_media_sandbox_production.datasandbox_c13n_experiment_comparisons

union

select
	experiment_name as "Experiment Name",
	data_start_date_inclusive as "Start Date",
	data_end_date_inclusive as "End Date",
	(case clicked_type 
		when 'NOT_CLICKED' then 'Not Clicked'
		when 'CLICKED' then 'Clicked'
		else clicked_type
	end) as "Clicked Type",
	(case pure_group_type 
		when 'PURE' then 'Pure'
		when 'NOT_PURE' then 'Not Pure'
		when 'MULTIPLE' then 'Multiple'
		else pure_group_type
	end) as "Pure Group Type",
	training_group_type as "Training Group ID",
	(case publisher_type 
		when 'EXPEDIA' then 'Expedia'
		when 'ORBITZ_GLOBAL' then 'Orbitz'
		when 'CHEAPTICKETS' then 'CheapTickets'
		else publisher_type
	end) as "Pub",
	(case user_segmentation_type
		when 'DIRTY' then 'Mixed'
		when 'LOW_CONVERTING' then 'Low Converting'
		when 'HIGH_CONVERTING' then 'High Converting'
		else user_segmentation_type
	end) as "User Segmentation Type",
	segmentation_model_percentile as "Segmentation Model Percentile",
	model_slice_id as "Model Slice ID",
	treatment as "Treatment",
	'Hotels' as "Product Category Type",
	number_users_total as "Number of Users",
	cast(null as int) as "Pages Available",
	has_conversion_hotels as "Has Conversions",
	number_conversions_hotels as "Conversions",
	net_value_conversions_hotels as "Conversion Net Value",
	has_insurance_conversion_hotels as "Has Insurance Conversion",
	number_insurance_conversions_hotels as "Insurance Conversions",
	net_value_insurance_conversions_hotels as "Insurance Conversions Net Value",
	number_hotel_attach_conversions_flights as "Flight Conversions Attached",
	number_hotel_attach_conversions_hotels as "Hotel Conversions Attached",
	number_hotel_attach_conversions_cars as "Car Conversions Attached",
	number_hotel_attach_conversions_packages as "Package Conversions Attached",
	number_hotel_attach_conversions_total as "Total Conversions Attached",
	cast(null as float) as "Net Media Revenue"
from intent_media_sandbox_production.datasandbox_c13n_experiment_comparisons

union

select
	experiment_name as "Experiment Name",
	data_start_date_inclusive as "Start Date",
	data_end_date_inclusive as "End Date",
	(case clicked_type 
		when 'NOT_CLICKED' then 'Not Clicked'
		when 'CLICKED' then 'Clicked'
		else clicked_type
	end) as "Clicked Type",
	(case pure_group_type 
		when 'PURE' then 'Pure'
		when 'NOT_PURE' then 'Not Pure'
		when 'MULTIPLE' then 'Multiple'
		else pure_group_type
	end) as "Pure Group Type",
	training_group_type as "Training Group ID",
	(case publisher_type 
		when 'EXPEDIA' then 'Expedia'
		when 'ORBITZ_GLOBAL' then 'Orbitz'
		when 'CHEAPTICKETS' then 'CheapTickets'
		else publisher_type
	end) as "Pub",
	(case user_segmentation_type
		when 'DIRTY' then 'Mixed'
		when 'LOW_CONVERTING' then 'Low Converting'
		when 'HIGH_CONVERTING' then 'High Converting'
		else user_segmentation_type
	end) as "User Segmentation Type",
	segmentation_model_percentile as "Segmentation Model Percentile",
	model_slice_id as "Model Slice ID",
	treatment as "Treatment",
	'Cars' as "Product Category Type",
	number_users_total as "Number of Users",
	cast(null as int) as "Pages Available",
	has_conversion_cars as "Has Conversions",
	number_conversions_cars as "Conversions",
	net_value_conversions_cars as "Conversion Net Value",
	has_insurance_conversion_cars as "Has Insurance Conversion",
	number_insurance_conversions_cars as "Insurance Conversions",
	net_value_insurance_conversions_cars as "Insurance Conversions Net Value",
	cast(null as int) as "Flight Conversions Attached",
	cast(null as int) as "Hotel Conversions Attached",
	cast(null as int) as "Car Conversions Attached",
	cast(null as int) as "Package Conversions Attached",
	cast(null as int) as "Total Conversions Attached",
	cast(null as float) as "Net Media Revenue"
from intent_media_sandbox_production.datasandbox_c13n_experiment_comparisons

union

select
	experiment_name as "Experiment Name",
	data_start_date_inclusive as "Start Date",
	data_end_date_inclusive as "End Date",
	(case clicked_type 
		when 'NOT_CLICKED' then 'Not Clicked'
		when 'CLICKED' then 'Clicked'
		else clicked_type
	end) as "Clicked Type",
	(case pure_group_type 
		when 'PURE' then 'Pure'
		when 'NOT_PURE' then 'Not Pure'
		when 'MULTIPLE' then 'Multiple'
		else pure_group_type
	end) as "Pure Group Type",
	training_group_type as "Training Group ID",
	(case publisher_type 
		when 'EXPEDIA' then 'Expedia'
		when 'ORBITZ_GLOBAL' then 'Orbitz'
		when 'CHEAPTICKETS' then 'CheapTickets'
		else publisher_type
	end) as "Pub",
	(case user_segmentation_type
		when 'DIRTY' then 'Mixed'
		when 'LOW_CONVERTING' then 'Low Converting'
		when 'HIGH_CONVERTING' then 'High Converting'
		else user_segmentation_type
	end) as "User Segmentation Type",
	segmentation_model_percentile as "Segmentation Model Percentile",
	model_slice_id as "Model Slice ID",
	treatment as "Treatment",
	'Packages' as "Product Category Type",
	number_users_total as "Number of Users",
	cast(null as int) as "Pages Available",
	has_conversion_packages as "Has Conversions",
	number_conversions_packages as "Conversions",
	net_value_conversions_packages as "Conversion Net Value",
	has_insurance_conversion_packages as "Has Insurance Conversion",
	number_insurance_conversions_packages as "Insurance Conversions",
	net_value_insurance_conversions_packages as "Insurance Conversions Net Value",
	cast(null as int) as "Flight Conversions Attached",
	cast(null as int) as "Hotel Conversions Attached",
	cast(null as int) as "Car Conversions Attached",
	cast(null as int) as "Package Conversions Attached",
	cast(null as int) as "Total Conversions Attached",
	cast(null as float) as "Net Media Revenue"
from intent_media_sandbox_production.datasandbox_c13n_experiment_comparisons
