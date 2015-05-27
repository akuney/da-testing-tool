select
	display_name,
	product,
	ad_unit_id,
	requested_at_date_in_et,
	outcome_type,
	os,
	browser_family,
	browser,
	ad_unit_name,
	ad_unit_active,
	unique_visitors,
	page_views,
	served_page_views,
	interactions,
	clicks,
	revenue
from
/* SCA */
(
	select
		s.display_name,
		case au.product_category_type
			when 'FLIGHTS' then 'SCA Flights'
			when 'HOTELS' then 'SCA Hotels'
			when 'CARS' then 'SCA Cars'
			else 'Other'
		end as product,
		au.id as ad_unit_id,
		ac_c.requested_at_date_in_et,
		ac_c.outcome_type,
		ac_c.os,
		ac_c.browser_family,
		ac_c.browser,
		au.name as ad_unit_name,
		au.active as ad_unit_active,
		count(distinct ac_c.publisher_user_id) as unique_visitors,
		count(ac_c.request_id) as page_views,
		count(case when ac_c.outcome_type = 'SERVED' then ac_c.request_id end) as served_page_views,
		sum(interactions) as interactions,
		sum(clicks) as clicks,
		sum(revenue) as revenue
	from
	(
		select
			ac.request_id,
			min(ac.requested_at_date_in_et) as requested_at_date_in_et,
			min(ac.publisher_user_id) as publisher_user_id,
			min(ac.site_id) as site_id,
			min(ac.ad_unit_id) as ad_unit_id,
		  min(ac.ad_unit_type) as ad_unit_type,
		  min(ac.outcome_type) as outcome_type,
		  min(ac.positions_filled) as positions_filled,
		  min(ac.browser_family) as browser_family,
		  min(ac.browser) as browser,
		  min(ac.os_family) as os_family,
		  min(ac.os) as os,
		  count(distinct c.ad_call_request_id) as interactions,
		  count(c.request_id) as clicks,
		  sum(c.actual_cpc) as revenue
		from intent_media_log_data_production.ad_calls ac
		left join intent_media_log_data_production.clicks c
		on ac.request_id = c.ad_call_request_id
		and c.requested_at_date_in_et >= date(current_timestamp at timezone 'America/New_York') - interval '90 days'
		and c.ip_address_blacklisted = 0
		and c.fraudulent = 0
		where ac.requested_at_date_in_et >= date(current_timestamp at timezone 'America/New_York') - interval '90 days'
		  and ac.ip_address_blacklisted = 0
		  and ac.ad_unit_type = 'CT'
		group by
			ac.request_id
	) ac_c
	inner join intent_media_production.sites s on ac_c.site_id = s.id
	inner join intent_media_production.ad_units au on ac_c.ad_unit_id = au.id
	group by 1,2,3,4,5,6,7,8,9,10
) sca
union all
/* SSN */
(
	select
		s.display_name,
		case au.product_category_type
			when 'HOTELS' then 'SSN Hotels'
			else 'Other'
		end as product,
		au.id as ad_unit_id,
		ac_c.requested_at_date_in_et,
		ac_c.outcome_type,
		ac_c.os,
		ac_c.browser_family,
		ac_c.browser,
		au.name as ad_unit_name,
		au.active as ad_unit_active,
		count(distinct ac_c.publisher_user_id) as unique_visitors,
		count(ac_c.request_id) as page_views,
		count(case when ac_c.positions_filled > 0 then ac_c.request_id end) as served_page_views,
		sum(interactions) as interactions,
		sum(clicks) as clicks,
		sum(revenue) as revenue
	from
	(
		select
			ac.request_id,
			min(ac.requested_at_date_in_et) as requested_at_date_in_et,
			min(ac.publisher_user_id) as publisher_user_id,
			min(ac.site_id) as site_id,
			min(ac.ad_unit_id) as ad_unit_id,
		  min(ac.ad_unit_type) as ad_unit_type,
		  min(ac.outcome_type) as outcome_type,
		  min(ac.positions_filled) as positions_filled,
		  min(ac.browser_family) as browser_family,
		  min(ac.browser) as browser,
		  min(ac.os_family) as os_family,
		  min(ac.os) as os,
		  count(distinct c.ad_call_request_id) as interactions,
		  count(c.request_id) as clicks,
		  sum(c.actual_cpc) as revenue
		from intent_media_log_data_production.ad_calls ac
		left join intent_media_log_data_production.clicks c
		on ac.request_id = c.ad_call_request_id
		and c.requested_at_date_in_et >= date(current_timestamp at timezone 'America/New_York') - interval '90 days'
		and c.ip_address_blacklisted = 0
		and c.fraudulent = 0
		where ac.requested_at_date_in_et >= date(current_timestamp at timezone 'America/New_York') - interval '90 days'
		  and ac.ip_address_blacklisted = 0
		  and ac.ad_unit_type = 'SSR'
		  and ac.outcome_type = 'SERVED'
		group by
			ac.request_id
	) ac_c
	inner join intent_media_production.sites s on ac_c.site_id = s.id
	inner join intent_media_production.ad_units au on ac_c.ad_unit_id = au.id
	group by 1,2,3,4,5,6,7,8,9,10
)
union all
/* PPA */
(
	select
		s.display_name,
		case au.product_category_type
			when 'HOTELS' then 'PPA Hotels'
			else 'Other'
		end as product,
		au.id as ad_unit_id,
		ac_c.requested_at_date_in_et,
		ac_c.outcome_type,
		ac_c.os,
		ac_c.browser_family,
		ac_c.browser,
		au.name as ad_unit_name,
		au.active as ad_unit_active,
		count(distinct ac_c.publisher_user_id) as unique_visitors,
		count(ac_c.request_id) as page_views,
		count(case when ac_c.positions_filled > 0 then ac_c.request_id end) as served_page_views,
		sum(interactions) as interactions,
		sum(clicks) as clicks,
		sum(revenue) as revenue
	from
	(
		select
			ac.request_correlation_id as request_id,
			min(ac.requested_at_date_in_et) as requested_at_date_in_et,
			min(ac.publisher_user_id) as publisher_user_id,
			min(ac.site_id) as site_id,
			min(ac.ad_unit_id) as ad_unit_id,
		  min(ac.ad_unit_type) as ad_unit_type,
		  min(ac.outcome_type) as outcome_type,
		  min(ac.positions_filled) as positions_filled,
		  min(ac.browser_family) as browser_family,
		  min(ac.browser) as browser,
		  min(ac.os_family) as os_family,
		  min(ac.os) as os,
		  count(distinct c.ad_call_request_id) as interactions,
		  count(c.request_id) as clicks,
		  sum(c.actual_cpc) as revenue
		from intent_media_log_data_production.ad_calls ac
		left join intent_media_log_data_production.clicks c
		on ac.request_id = c.ad_call_request_id
		and c.requested_at_date_in_et >= date(current_timestamp at timezone 'America/New_York') - interval '90 days'
		and c.ip_address_blacklisted = 0
		and c.fraudulent = 0
		where ac.requested_at_date_in_et >= date(current_timestamp at timezone 'America/New_York') - interval '90 days'
		  and ac.ip_address_blacklisted = 0
		  and ac.ad_unit_type = 'META'
		group by
			ac.request_correlation_id
	) ac_c
	inner join intent_media_production.sites s on ac_c.site_id = s.id
	inner join intent_media_production.ad_units au on ac_c.ad_unit_id = au.id
	group by 1,2,3,4,5,6,7,8,9,10
)
