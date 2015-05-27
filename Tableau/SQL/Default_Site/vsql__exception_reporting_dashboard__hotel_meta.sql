select
	meta."Date",
	ad_units.name as "Ad Unit",
	meta."Ad Type",
	meta."Product Category Type",
	meta."Site",
	meta."Hotel Meta Page Views",
	meta."Hotel Meta Pages Served",
	meta."Hotel Meta Impressions",
	meta."Hotel Meta Clicks",
	meta."Hotel Meta Fraudulent Clicks",
	meta."Hotel Meta Interactions",
	meta."Hotel Meta Gross Media Revenue"
from
	(select 
		meta1."Date",
		meta1."Ad Unit ID",
		meta1."Ad Type",
		meta1."Product Category Type",
		meta1."Site",
		meta1."Hotel Meta Page Views",
		meta2."Hotel Meta Pages Served",
		meta2."Hotel Meta Impressions",
		meta2."Hotel Meta Clicks",
		meta2."Hotel Meta Fraudulent Clicks",
		meta2."Hotel Meta Interactions",
		meta2."Hotel Meta Gross Media Revenue"
	from	
		(select 
			ad_calls.requested_at_date_in_et as "Date",
			ad_calls.ad_unit_id as "Ad Unit ID",
			ad_calls.ad_unit_type as "Ad Type",
			ad_calls.product_category_type as "Product Category Type",
			ad_calls.site_type as "Site",
			count(distinct(ad_calls.request_correlation_id)) as "Hotel Meta Page Views"
		from intent_media_log_data_production.ad_calls
		where ad_calls.requested_at_date_in_et >= (date(current_timestamp at timezone 'America/New_York') - interval '36 days')
        and ad_calls.requested_at_date_in_et < date(current_timestamp at timezone 'America/New_York')
		and ad_calls.ad_unit_type = 'META'
		and ad_calls.ip_address_blacklisted = 0
		group by 
			ad_calls.requested_at_date_in_et,
			ad_calls.ad_unit_id,
			ad_calls.ad_unit_type,
			ad_calls.product_category_type,
			ad_calls.site_type) meta1

	left join

		(select 
			ad_calls.requested_at_date_in_et as "Date",
			ad_calls.ad_unit_id as "Ad Unit ID",
			ad_calls.ad_unit_type as "Ad Type",
			ad_calls.product_category_type as "Product Category Type",
			ad_calls.site_type as "Site",
			count(distinct(ad_calls.request_correlation_id)) as "Hotel Meta Pages Served",
			count(ad_calls.request_correlation_id) as "Hotel Meta Impressions",
			count(case when (not clicks.fraudulent) and (not clicks.ip_address_blacklisted) then clicks.request_id end)	as "Hotel Meta Clicks",
			count(case when clicks.fraudulent then clicks.request_id end) as "Hotel Meta Fraudulent Clicks",
            count(distinct(case when (not clicks.fraudulent) and (not clicks.ip_address_blacklisted) then clicks.request_id end)) as "Hotel Meta Interactions",
            round(sum(case when (not clicks.fraudulent) and (not clicks.ip_address_blacklisted) then clicks.actual_cpc end),2) as "Hotel Meta Gross Media Revenue"
		from intent_media_log_data_production.impressions
		left join intent_media_log_data_production.ad_calls on impressions.request_id = ad_calls.request_id
		left join intent_media_log_data_production.clicks on impressions.external_id = clicks.external_impression_id
		where ad_calls.requested_at_date_in_et >= (date(current_timestamp at timezone 'America/New_York') - interval '36 days')
        and ad_calls.requested_at_date_in_et < date(current_timestamp at timezone 'America/New_York')
		and impressions.requested_at_date_in_et >= (date(current_timestamp at timezone 'America/New_York') - interval '36 days')
        and impressions.requested_at_date_in_et < date(current_timestamp at timezone 'America/New_York')
		and ((clicks.requested_at_date_in_et is null) or 
			(clicks.requested_at_date_in_et >= (date(current_timestamp at timezone 'America/New_York') - interval '36 days')
        	and clicks.requested_at_date_in_et < date(current_timestamp at timezone 'America/New_York')))  
		and ad_calls.ad_unit_type = 'META'
		and ad_calls.ip_address_blacklisted = 0
		group by 
			ad_calls.requested_at_date_in_et,
			ad_calls.ad_unit_id,
			ad_calls.ad_unit_type,
			ad_calls.product_category_type,
			ad_calls.site_type) meta2

	on meta1."Date" = meta2."Date"
	and meta1."Ad Unit ID" = meta2."Ad Unit ID"
	and meta1."Site" = meta2."Site") meta

left join 
	(select id, name, site_id, active from intent_media_production.ad_units) ad_units 
on ad_units.id = meta."Ad Unit ID"