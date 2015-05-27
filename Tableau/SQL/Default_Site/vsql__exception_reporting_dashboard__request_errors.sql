select
	requested_at_date_in_et as "Date",
	browser_family as "Browser Family",
	browser as "Browser",
	os_family as "OS Family",
	os as "OS",
	sum(case when uncaught_exception then 1 else 0 end) as "Uncaught Exception",
	sum(case when parsing_error then 1 else 0 end) as "Parsing Error",
	sum(case when inactive_entity then 1 else 0 end) as "Inactive Entity",
	sum(case when derived_intent_mismatch then 1 else 0 end) as "Derived Intent Mismatch",
	sum(case when unknown_entity then 1 else 0 end) as "Unknown Entity",
	sum(case when unknown_hotel_property then 1 else 0 end) as "Unknown Hotel Property",
	sum(case when unknown_product_category then 1 else 0 end) as "Unknown Product Category",
	sum(case when unknown_ad_unit then 1 else 0 end) as "Unknown Ad Unit",
	sum(case when unknown_market then 1 else 0 end) as "Unknown Market",
	sum(case when unknown_airport_code then 1 else 0 end) as "Unknown Airport Code",
	sum(case when publisher_show_ads_suppression then 1 else 0 end) as "Suppressed by Publisher",
	sum(case when suspicious_hotel_conversion_value then 1 else 0 end) as "Suspicious Hotel Conversion Value",
	count(1) as "Request Errors"
from intent_media_log_data_production.request_errors
where requested_at_date_in_et >= (date(current_timestamp at timezone 'America/New_York') - interval '38 days')
	and requested_at_date_in_et < date(current_timestamp at timezone 'America/New_York')
and ip_address_blacklisted = 0
group by
	requested_at_date_in_et,
	os_family,
	os,
	browser_family,
	browser