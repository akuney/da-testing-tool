drop table if exists intent_media_sandbox_production.DP_MVT_44_c13n_pure_ac_raw
;

create table intent_media_sandbox_production.DP_MVT_44_c13n_pure_ac_raw as
select 
    publisher_user_id,
    air_ct_revenue_share,
    pure_group_type,
	CASE INSTR(multivariate_test_attributes,'MVT_GROUP_OVERRIDE_ID',1) WHEN 0 THEN 'Not_found' ELSE SUBSTR(multivariate_test_attributes,INSTR(multivariate_test_attributes,'MVT_GROUP_OVERRIDE_ID',1)+LENGTH('MVT_GROUP_OVERRIDE_ID')+3,CASE INSTR(multivariate_test_attributes,'","',INSTR(multivariate_test_attributes,'MVT_GROUP_OVERRIDE_ID',1)) WHEN 0 THEN LENGTH(multivariate_test_attributes)-INSTR(multivariate_test_attributes,'MVT_GROUP_OVERRIDE_ID',1)-LENGTH('MVT_GROUP_OVERRIDE_ID')-4 ELSE INSTR(multivariate_test_attributes,'","',INSTR(multivariate_test_attributes,'MVT_GROUP_OVERRIDE_ID',1))-INSTR(multivariate_test_attributes,'MVT_GROUP_OVERRIDE_ID',1)-LENGTH('MVT_GROUP_OVERRIDE_ID')-3 END) END AS mvt_value,
    segmentation_type,
	request_id,
	requested_at,
    case when outcome_type = 'SERVED' then 1 end as served_ad_calls
from intent_media_log_data_production.ad_calls
where
	ip_address_blacklisted = 0 and
	site_type = 'EXPEDIA' and
	publisher_user_id is not null and
	publisher_id = 45 and
	product_category_type = 'FLIGHTS' and
	requested_at_date_in_et >= '2013-12-12' and
	requested_at_date_in_et <= '2014-01-14' and
	multivariate_version_id >= 661 and
	multivariate_version_id < 686
;

drop table if exists intent_media_sandbox_production.DP_MVT_44_c13n_pure_ac
;

create table intent_media_sandbox_production.DP_MVT_44_c13n_pure_ac as
select 
    publisher_user_id,
    min(air_ct_revenue_share) AS air_ct_revenue_share,
    CASE WHEN count(distinct(pure_group_type))>1 then 'MULTIPLE' ELSE min(pure_group_type) end pure_group_type,
	CASE WHEN count(distinct(mvt_value))>1 then 'DIRTY' ELSE min(mvt_value) end mvt_value,
	CASE WHEN count(distinct(segmentation_type))>1 then 'DIRTY' ELSE min(segmentation_type) end user_segmentation_type,
    count(request_id) as user_number_ad_calls_flights,
    sum(served_ad_calls) as user_number_served_ad_calls_flights
from intent_media_sandbox_production.DP_MVT_44_c13n_pure_ac_raw
where mvt_value <> 'Not_found'
group by publisher_user_id
;

drop table if exists intent_media_sandbox_production.DP_MVT_44_c13n_pure_min_req
;

create table intent_media_sandbox_production.DP_MVT_44_c13n_pure_min_req as
select publisher_user_id,
	min(requested_at) AS requested_at
from intent_media_sandbox_production.DP_MVT_44_c13n_pure_ac_raw
group by publisher_user_id
;

drop table if exists intent_media_sandbox_production.DP_MVT_44_c13n_pure
;

create table intent_media_sandbox_production.DP_MVT_44_c13n_pure as
select
        'MVT 44: bake-off vs. pure' as experiment_name,
        'EXPEDIA' as publisher_type,
        '2013-12-12' as data_start_date_inclusive,
        '2014-01-14' as data_end_date_inclusive,
        ad_call.pure_group_type,
		ad_call.mvt_value,
        ad_call.user_segmentation_type,
        COUNT(*) as number_users_total,
        SUM(ad_call.user_number_ad_calls_flights) as number_ad_calls_flights,
        SUM(click.actual_cpc_sum*ad_call.air_ct_revenue_share) as net_media_revenue,
		SUM(ad_call.user_number_served_ad_calls_flights) as number_served_ad_calls_flights,
        SUM(CASE WHEN conversion.user_number_conversion_flights > 0 THEN 1 ELSE 0 END) as has_conversion_flights,
        SUM(CASE WHEN conversion.user_number_conversion_hotels > 0 THEN 1 ELSE 0 END) as has_conversion_hotels,
        SUM(CASE WHEN conversion.user_number_conversion_cars > 0 THEN 1 ELSE 0 END) as has_conversion_cars,
        SUM(CASE WHEN conversion.user_number_conversion_packages > 0 THEN 1 ELSE 0 END) as has_conversion_packages,
        SUM(CASE WHEN (conversion.user_number_conversion_flights > 0 or
                                  conversion.user_number_conversion_hotels > 0 or
                                  conversion.user_number_conversion_cars > 0 or
                                  conversion.user_number_conversion_packages > 0)
                                  THEN 1 ELSE 0 END) as has_conversion_total,
        -- now lets all sum the conversions nums
        SUM(conversion.user_number_conversion_flights) as number_conversions_flights,
        SUM(conversion.user_number_conversion_hotels) as number_conversions_hotels,
        SUM(conversion.user_number_conversion_cars) as number_conversions_cars,
        SUM(conversion.user_number_conversion_packages) as number_conversions_packages,
        SUM(conversion.user_number_conversion_packages+conversion.user_number_conversion_flights+conversion.user_number_conversion_cars+conversion.user_number_conversion_hotels) as number_conversions_total,
        -- net values of conversions
        SUM(conversion.user_net_conversion_flights) as net_value_conversions_flights,
        SUM(conversion.user_net_conversion_hotels) as net_value_conversions_hotels,
        SUM(conversion.user_net_conversion_cars) as net_value_conversions_cars,
        SUM(conversion.user_net_conversion_packages) as net_value_conversions_packages,
        SUM(conversion.user_net_conversion_flights+conversion.user_net_conversion_cars+conversion.user_net_conversion_packages+conversion.user_net_conversion_hotels) as net_value_conversions_total,
        -- FLIGHTS attach conversions ---
        SUM(conversion.user_flights_attach_flights) as number_flight_attach_conversions_flights,
        SUM(conversion.user_flights_attach_hotels) as number_flight_attach_conversions_hotels,
        SUM(conversion.user_flights_attach_cars) as number_flight_attach_conversions_cars,
        SUM(conversion.user_flights_attach_packages) as number_flight_attach_conversions_packages,
        SUM(conversion.user_flights_attach_flights+conversion.user_flights_attach_cars+conversion.user_flights_attach_packages+conversion.user_flights_attach_hotels) as number_flight_attach_conversions_total,
        -- HOTELS attach conversions ---
        SUM(conversion.user_hotels_attach_flights) as number_hotel_attach_conversions_flights,
        SUM(conversion.user_hotels_attach_hotels) as number_hotel_attach_conversions_hotels,
        SUM(conversion.user_hotels_attach_cars) as number_hotel_attach_conversions_cars,
        SUM(conversion.user_hotels_attach_packages) as number_hotel_attach_conversions_packages,
        SUM(conversion.user_hotels_attach_flights+conversion.user_hotels_attach_cars+conversion.user_hotels_attach_packages+conversion.user_hotels_attach_hotels) as number_hotel_attach_conversions_total,
        ---- insurance --
        SUM(CASE WHEN conversion.user_number_insurance_conversion_flights > 0 THEN 1 ELSE 0 END) as has_insurance_conversion_flights,
        SUM(CASE WHEN conversion.user_number_insurance_conversion_hotels > 0 THEN 1 ELSE 0 END) as has_insurance_conversion_hotels,
        SUM(CASE WHEN conversion.user_number_insurance_conversion_cars > 0 THEN 1 ELSE 0 END) as has_insurance_conversion_cars,
        SUM(CASE WHEN conversion.user_number_insurance_conversion_packages > 0 THEN 1 ELSE 0 END) as has_insurance_conversion_packages,
        SUM(CASE WHEN conversion.user_number_insurance_conversion_flights > 0 or
                                  conversion.user_number_insurance_conversion_cars > 0 or
                                  conversion.user_number_insurance_conversion_hotels > 0 or
                                  conversion.user_number_insurance_conversion_packages > 0
                THEN 1 ELSE 0 END) as has_insurance_conversion_total,
        SUM(conversion.user_number_insurance_conversion_flights) as number_insurance_conversions_flights,
        SUM(conversion.user_number_insurance_conversion_hotels) as number_insurance_conversions_hotels,
        SUM(conversion.user_number_insurance_conversion_cars) as number_insurance_conversions_cars,
        SUM(conversion.user_number_insurance_conversion_packages) as number_insurance_conversions_packages,
        SUM(conversion.user_number_insurance_conversion_packages+conversion.user_number_insurance_conversion_hotels+conversion.user_number_insurance_conversion_cars+conversion.user_number_insurance_conversion_flights)
                as number_insurance_conversions_total,
        -- insurance net --
        SUM(conversion.user_net_insurance_conversion_flights) as net_value_insurance_conversions_flights,
        SUM(conversion.user_net_insurance_conversion_hotels) as net_value_insurance_conversions_hotels,
        SUM(conversion.user_net_insurance_conversion_cars) as net_value_insurance_conversions_cars,
        SUM(conversion.user_net_insurance_conversion_packages) as net_value_insurance_conversions_packages,
        SUM(conversion.user_net_insurance_conversion_packages+conversion.user_net_insurance_conversion_flights+conversion.user_net_insurance_conversion_cars+conversion.user_net_insurance_conversion_hotels) as net_value_insurance_conversions_total
from intent_media_sandbox_production.DP_MVT_44_c13n_pure_ac ad_call
left join
-------------------------
----conversions ---------
-------------------------
        (select deduped_conversion.publisher_user_id,
                -- how many HOTELS, CARS and FLIGHTS conversions are attached to the first flight conversions
                SUM(CASE WHEN deduped_conversion.product_category_type ='HOTELS' and deduped_conversion.requested_at>min_date_by_publisher_user_id.min_flight_conversion THEN 1 ELSE 0 END) as user_flights_attach_hotels,
                SUM(CASE WHEN deduped_conversion.product_category_type ='CARS' and deduped_conversion.requested_at>min_date_by_publisher_user_id.min_flight_conversion THEN 1 ELSE 0 END) as user_flights_attach_cars,
                SUM(CASE WHEN deduped_conversion.product_category_type ='PACKAGES' and deduped_conversion.requested_at>min_date_by_publisher_user_id.min_flight_conversion THEN 1 ELSE 0 END) as user_flights_attach_packages,
                SUM(CASE WHEN deduped_conversion.product_category_type ='FLIGHTS' and deduped_conversion.requested_at>min_date_by_publisher_user_id.min_flight_conversion THEN 1 ELSE 0 END) as user_flights_attach_flights,
                -- how many HOTELS, CARS and FLIGHTS conversions are attached to the first HOTEL conversions
                SUM(CASE WHEN deduped_conversion.product_category_type ='HOTELS' and deduped_conversion.requested_at>min_date_by_publisher_user_id.min_hotel_conversion THEN 1 ELSE 0 END) as user_hotels_attach_hotels,
                SUM(CASE WHEN deduped_conversion.product_category_type ='CARS' and deduped_conversion.requested_at>min_date_by_publisher_user_id.min_hotel_conversion THEN 1 ELSE 0 END) as user_hotels_attach_cars,
                SUM(CASE WHEN deduped_conversion.product_category_type ='PACKAGES' and deduped_conversion.requested_at>min_date_by_publisher_user_id.min_hotel_conversion THEN 1 ELSE 0 END) as user_hotels_attach_packages,
                SUM(CASE WHEN deduped_conversion.product_category_type ='FLIGHTS' and deduped_conversion.requested_at>min_date_by_publisher_user_id.min_hotel_conversion THEN 1 ELSE 0 END) as user_hotels_attach_flights,
                -- just counts of each type of conversions
                SUM(CASE WHEN deduped_conversion.product_category_type ='FLIGHTS' THEN 1 ELSE 0 END) as user_number_conversion_flights,
                SUM(CASE WHEN deduped_conversion.product_category_type ='HOTELS' THEN 1 ELSE 0 END) as user_number_conversion_hotels,
                SUM(CASE WHEN deduped_conversion.product_category_type ='CARS' THEN 1 ELSE 0 END) as user_number_conversion_cars,
                SUM(CASE WHEN deduped_conversion.product_category_type ='PACKAGES' THEN 1 ELSE 0 END) as user_number_conversion_packages,
                --now conversions net value
                SUM(CASE WHEN deduped_conversion.product_category_type ='FLIGHTS' THEN deduped_conversion.net_conversion_value ELSE 0 END) as user_net_conversion_flights,
                SUM(CASE WHEN deduped_conversion.product_category_type ='HOTELS' THEN deduped_conversion.net_conversion_value ELSE 0 END) as user_net_conversion_hotels,
                SUM(CASE WHEN deduped_conversion.product_category_type ='CARS' THEN deduped_conversion.net_conversion_value ELSE 0 END) as user_net_conversion_cars,
                SUM(CASE WHEN deduped_conversion.product_category_type ='PACKAGES' THEN deduped_conversion.net_conversion_value ELSE 0 END) as user_net_conversion_packages,
                --now insurance counts
                SUM(CASE WHEN (deduped_conversion.product_category_type ='FLIGHTS' AND deduped_conversion.net_insurance_value >0) THEN 1 ELSE 0 END) as user_number_insurance_conversion_flights,
                SUM(CASE WHEN (deduped_conversion.product_category_type ='HOTELS' AND deduped_conversion.net_insurance_value >0) THEN 1 ELSE 0 END) as user_number_insurance_conversion_hotels,
                SUM(CASE WHEN (deduped_conversion.product_category_type ='CARS' AND deduped_conversion.net_insurance_value >0) THEN 1 ELSE 0 END) as user_number_insurance_conversion_cars,
                SUM(CASE WHEN (deduped_conversion.product_category_type ='PACKAGES' AND deduped_conversion.net_insurance_value >0) THEN 1 ELSE 0 END) as user_number_insurance_conversion_packages,
                --now insurance net value
                SUM(CASE WHEN (deduped_conversion.product_category_type ='FLIGHTS' AND deduped_conversion.net_insurance_value >0) THEN deduped_conversion.net_insurance_value ELSE 0 END) as user_net_insurance_conversion_flights,
                SUM(CASE WHEN (deduped_conversion.product_category_type ='HOTELS' AND deduped_conversion.net_insurance_value >0) THEN deduped_conversion.net_insurance_value ELSE 0 END) as user_net_insurance_conversion_hotels,
                SUM(CASE WHEN (deduped_conversion.product_category_type ='CARS' AND deduped_conversion.net_insurance_value >0) THEN deduped_conversion.net_insurance_value ELSE 0 END) as user_net_insurance_conversion_cars,
                SUM(CASE WHEN (deduped_conversion.product_category_type ='PACKAGES' AND deduped_conversion.net_insurance_value >0) THEN deduped_conversion.net_insurance_value ELSE 0 END) as user_net_insurance_conversion_packages
        from
                (select
                        publisher_user_id, order_id,
                        round(net_conversion_value,2) groupable_net_conversion_value,
                        min(requested_at) as requested_at,
                        min(net_conversion_value) as net_conversion_value,
                        min(product_category_type) as product_category_type,
                        min(net_insurance_value) as net_insurance_value
                from intent_media_log_data_production.conversions
                where
                        ip_address_blacklisted = 0 and
                        site_type = 'EXPEDIA' and
                        entity_id = 45 and
                        publisher_user_id is not null and
                        requested_at_date_in_et >= '2013-12-12' and
                        requested_at_date_in_et <= '2014-01-14'
                group by publisher_user_id, order_id, groupable_net_conversion_value
                order by requested_at) deduped_conversion
        join
                (select 
                        conversions.publisher_user_id,
                        MIN(case when conversions.product_category_type = 'FLIGHTS' then conversions.requested_at else NULL END) as min_flight_conversion,
                        MIN(case when conversions.product_category_type = 'HOTELS' then conversions.requested_at else NULL END) as min_hotel_conversion,
                        min(min_date_by_publisher_user_id.requested_at) as min_any_conversion
                from intent_media_log_data_production.conversions
                join intent_media_sandbox_production.DP_MVT_44_c13n_pure_min_req min_date_by_publisher_user_id 
                on conversions.publisher_user_id=min_date_by_publisher_user_id.publisher_user_id
                        and conversions.requested_at > min_date_by_publisher_user_id.requested_at
                where
                        ip_address_blacklisted = 0 and
                        site_type = 'EXPEDIA' and
                        entity_id = 45 and
                        conversions.requested_at_date_in_et >= '2013-12-12' and
                        conversions.requested_at_date_in_et <= '2014-01-14'
                group by conversions.publisher_user_id) min_date_by_publisher_user_id 
        on deduped_conversion.publisher_user_id=min_date_by_publisher_user_id.publisher_user_id
         and deduped_conversion.requested_at > min_date_by_publisher_user_id.min_any_conversion
        group by deduped_conversion.publisher_user_id) conversion 
on conversion.publisher_user_id=ad_call.publisher_user_id
----------------------
----clicks -----------
----------------------
left join
        (select
                ac.publisher_user_id,
                case when count(*) > 0 then 'CLICKED' else 'NOT_CLICKED' END as clicked_type,
                sum(c.actual_cpc) as actual_cpc_sum
        from intent_media_log_data_production.clicks c
        join intent_media_sandbox_production.DP_MVT_44_c13n_pure_ac_raw ac on c.ad_call_request_id=ac.request_id
        where
			c.fraudulent = 0 and
			c.ip_address_blacklisted = 0 and
			c.requested_at_date_in_et >= '2013-12-12' and
			c.requested_at_date_in_et <= '2014-01-14' and
			c.site_type = 'EXPEDIA' and
			c.product_category_type = 'FLIGHTS'
        group by
                ac.publisher_user_id) click
on click.publisher_user_id=ad_call.publisher_user_id

group by
        ad_call.pure_group_type,
		ad_call.mvt_value,
        ad_call.user_segmentation_type
;