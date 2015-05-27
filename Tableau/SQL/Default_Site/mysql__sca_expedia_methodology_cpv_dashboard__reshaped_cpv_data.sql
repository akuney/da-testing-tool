select
	data_start_date_inclusive as "Start Date",
	data_end_date_inclusive as "End Date",
	(case c13n_run_type
		when 'USER_LEVEL_TRUE_NORTH_C13N_SPECIFIC_PERIOD' then 'User Level True North C13N - Specific Period'
		when 'USER_LEVEL_TRUE_NORTH_C13N_TO_DATE' then 'User Level True North C13N - To Date'
		else c13n_run_type
	end) as "C13N Run Type",
	(case time_period_type
		when 'ROLLING_ONE_MONTH' then 'Rolling 1 Month'
		when 'ROLLING_TWO_MONTHS' then 'Rolling 2 Months'
		when 'ROLLING_THREE_MONTHS' then 'Rolling 3 Months'
		when 'ROLLING_FOUR_MONTHS' then 'Rolling 4 Months'
		else time_period_type
	end) as "Time Period",
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
	(case training_group_type
		when 'NOT_TRAINING' then 'Not Training'
		when 'TRAINING' then 'Training'
		when 'MULTIPLE' then 'Multiple'
		else training_group_type
	end) as "Training Group Type",
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
	(case
		when product_category_type = 'FLIGHTS' then 'Flights'
		when product_category_type = 'HOTELS' then 'Hotels'
		when product_category_type is null then 'Total'
		else product_category_type
	end) as "Product",
	'Flights' as "Product Category Type",
	sum(number_users_total) as "Number of Users",
	sum(number_ad_calls_flights) as "Ad Calls",
	sum(has_conversion_flights) as "Has Conversions",
	sum(number_conversions_flights) as "Conversions",
	sum(net_value_conversions_flights) as "Conversion Net Value",
	sum(has_insurance_conversion_flights) as "Has Insurance Conversion",
	sum(number_insurance_conversions_flights) as "Insurance Conversions",
	sum(net_value_insurance_conversions_flights) as "Insurance Conversions Net Value",
	sum(number_flight_attach_conversions_flights) as "Flight Conversions Attached",
	sum(number_flight_attach_conversions_hotels) as "Hotel Conversions Attached",
	sum(number_flight_attach_conversions_cars) as "Car Conversions Attached",
	sum(number_flight_attach_conversions_packages) as "Package Conversions Attached",
	sum(number_flight_attach_conversions_total) as "Total Conversions Attached"
from user_level_true_north_c13n
group by
	data_start_date_inclusive,
	data_end_date_inclusive,
	c13n_run_type,
	time_period_type,
	clicked_type,
	pure_group_type,
	training_group_type,
	publisher_type,
	user_segmentation_type,
	product_category_type

union

select
	data_start_date_inclusive as "Start Date",
	data_end_date_inclusive as "End Date",
	(case c13n_run_type
		when 'USER_LEVEL_TRUE_NORTH_C13N_SPECIFIC_PERIOD' then 'User Level True North C13N - Specific Period'
		when 'USER_LEVEL_TRUE_NORTH_C13N_TO_DATE' then 'User Level True North C13N - To Date'
		else c13n_run_type
	end) as "C13N Run Type",
	(case time_period_type
		when 'ROLLING_ONE_MONTH' then 'Rolling 1 Month'
		when 'ROLLING_TWO_MONTHS' then 'Rolling 2 Months'
		when 'ROLLING_THREE_MONTHS' then 'Rolling 3 Months'
		when 'ROLLING_FOUR_MONTHS' then 'Rolling 4 Months'
		else time_period_type
	end) as "Time Period",
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
	(case training_group_type
		when 'NOT_TRAINING' then 'Not Training'
		when 'TRAINING' then 'Training'
		when 'MULTIPLE' then 'Multiple'
		else training_group_type
	end) as "Training Group Type",
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
	(case
		when product_category_type = 'FLIGHTS' then 'Flights'
		when product_category_type = 'HOTELS' then 'Hotels'
		when product_category_type is null then 'Total'
		else product_category_type
	end) as "Product",
	'Hotels' as "Product Category Type",
	sum(number_users_total) as "Number of Users",
	sum(number_ad_calls_hotels) as "Ad Calls",
	sum(has_conversion_hotels) as "Has Conversions",
	sum(number_conversions_hotels) as "Conversions",
	sum(net_value_conversions_hotels) as "Conversion Net Value",
	sum(has_insurance_conversion_hotels) as "Has Insurance Conversion",
	sum(number_insurance_conversions_hotels) as "Insurance Conversions",
	sum(net_value_insurance_conversions_hotels) as "Insurance Conversions Net Value",
	sum(number_hotel_attach_conversions_flights) as "Flight Conversions Attached",
	sum(number_hotel_attach_conversions_hotels) as "Hotel Conversions Attached",
	sum(number_hotel_attach_conversions_cars) as "Car Conversions Attached",
	sum(number_hotel_attach_conversions_packages) as "Package Conversions Attached",
	sum(number_hotel_attach_conversions_total) as "Total Conversions Attached"
from user_level_true_north_c13n
group by
	data_start_date_inclusive,
	data_end_date_inclusive,
	c13n_run_type,
	time_period_type,
	clicked_type,
	pure_group_type,
	training_group_type,
	publisher_type,
	user_segmentation_type,
	product_category_type

union

select
	data_start_date_inclusive as "Start Date",
	data_end_date_inclusive as "End Date",
	(case c13n_run_type
		when 'USER_LEVEL_TRUE_NORTH_C13N_SPECIFIC_PERIOD' then 'User Level True North C13N - Specific Period'
		when 'USER_LEVEL_TRUE_NORTH_C13N_TO_DATE' then 'User Level True North C13N - To Date'
		else c13n_run_type
	end) as "C13N Run Type",
	(case time_period_type
		when 'ROLLING_ONE_MONTH' then 'Rolling 1 Month'
		when 'ROLLING_TWO_MONTHS' then 'Rolling 2 Months'
		when 'ROLLING_THREE_MONTHS' then 'Rolling 3 Months'
		when 'ROLLING_FOUR_MONTHS' then 'Rolling 4 Months'
		else time_period_type
	end) as "Time Period",
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
	(case training_group_type
		when 'NOT_TRAINING' then 'Not Training'
		when 'TRAINING' then 'Training'
		when 'MULTIPLE' then 'Multiple'
		else training_group_type
	end) as "Training Group Type",
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
	(case
		when product_category_type = 'FLIGHTS' then 'Flights'
		when product_category_type = 'HOTELS' then 'Hotels'
		when product_category_type is null then 'Total'
		else product_category_type
	end) as "Product",
	'Cars' as "Product Category Type",
	sum(number_users_total) as "Number of Users",
	sum(number_ad_calls_cars) as "Ad Calls",
	sum(has_conversion_cars) as "Has Conversions",
	sum(number_conversions_cars) as "Conversions",
	sum(net_value_conversions_cars) as "Conversion Net Value",
	sum(has_insurance_conversion_cars) as "Has Insurance Conversion",
	sum(number_insurance_conversions_cars) as "Insurance Conversions",
	sum(net_value_insurance_conversions_cars) as "Insurance Conversions Net Value",
	null as "Flight Conversions Attached",
	null as "Hotel Conversions Attached",
	null as "Car Conversions Attached",
	null as "Package Conversions Attached",
	null as "Total Conversions Attached"
from user_level_true_north_c13n
group by
	data_start_date_inclusive,
	data_end_date_inclusive,
	c13n_run_type,
	time_period_type,
	clicked_type,
	pure_group_type,
	training_group_type,
	publisher_type,
	user_segmentation_type,
	product_category_type

union

select
	data_start_date_inclusive as "Start Date",
	data_end_date_inclusive as "End Date",
	(case c13n_run_type
		when 'USER_LEVEL_TRUE_NORTH_C13N_SPECIFIC_PERIOD' then 'User Level True North C13N - Specific Period'
		when 'USER_LEVEL_TRUE_NORTH_C13N_TO_DATE' then 'User Level True North C13N - To Date'
		else c13n_run_type
	end) as "C13N Run Type",
	(case time_period_type
		when 'ROLLING_ONE_MONTH' then 'Rolling 1 Month'
		when 'ROLLING_TWO_MONTHS' then 'Rolling 2 Months'
		when 'ROLLING_THREE_MONTHS' then 'Rolling 3 Months'
		when 'ROLLING_FOUR_MONTHS' then 'Rolling 4 Months'
		else time_period_type
	end) as "Time Period",
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
	(case training_group_type
		when 'NOT_TRAINING' then 'Not Training'
		when 'TRAINING' then 'Training'
		when 'MULTIPLE' then 'Multiple'
		else training_group_type
	end) as "Training Group Type",
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
	(case
		when product_category_type = 'FLIGHTS' then 'Flights'
		when product_category_type = 'HOTELS' then 'Hotels'
		when product_category_type is null then 'Total'
		else product_category_type
	end) as "Product",
	'Packages' as "Product Category Type",
	sum(number_users_total) as "Number of Users",
	sum(number_ad_calls_packages) as "Ad Calls",
	sum(has_conversion_packages) as "Has Conversions",
	sum(number_conversions_packages) as "Conversions",
	sum(net_value_conversions_packages) as "Conversion Net Value",
	sum(has_insurance_conversion_packages) as "Has Insurance Conversion",
	sum(number_insurance_conversions_packages) as "Insurance Conversions",
	sum(net_value_insurance_conversions_packages) as "Insurance Conversions Net Value",
	null as "Flight Conversions Attached",
	null as "Hotel Conversions Attached",
	null as "Car Conversions Attached",
	null as "Package Conversions Attached",
	null as "Total Conversions Attached"
from user_level_true_north_c13n
group by
	data_start_date_inclusive,
	data_end_date_inclusive,
	c13n_run_type,
	time_period_type,
	clicked_type,
	pure_group_type,
	training_group_type,
	publisher_type,
	user_segmentation_type,
	product_category_type