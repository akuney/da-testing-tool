create table intent_media_sandbox_production.YB_attributed_conversion as
select
  ac.request_id,
  ac.product_category_type,
  i_final.external_id,
  i_final.advertiser_id,
  ac.requested_at_in_et,
  ac.requested_at_date_in_et,
  ac.ad_unit_id,
  ac.ad_unit_type,
  ac.site_type,
  ac.trip_type,
  i_final.auction_position,
  count(clicks_with_conversions.click_request_id) as click_count,
  sum(clicks_with_conversions.actual_cpc) as actual_cpc_sum,
  sum(conversion_count_flights) as conversion_count_flights,
  sum(conversion_count_hotels) as conversion_count_hotels,
  sum(conversion_count_packages) as conversion_count_packages,
  sum(conversion_count_cars) as conversion_count_cars,
  sum(conversion_count_total) as conversion_count_total,
  sum(conversion_value_sum_flights) as conversion_value_sum_flights,
  sum(conversion_value_sum_hotels) as conversion_value_sum_hotels,
  sum(conversion_value_sum_packages) as conversion_value_sum_packages,
  sum(conversion_value_sum_cars) as conversion_value_sum_cars,
  sum(conversion_value_sum_total) as conversion_value_sum_total,
  sum(net_conversion_value_sum_flights) as net_conversion_value_sum_flights,
  sum(net_conversion_value_sum_hotels) as net_conversion_value_sum_hotels,
  sum(net_conversion_value_sum_packages) as net_conversion_value_sum_packages,
  sum(net_conversion_value_sum_cars) as net_conversion_value_sum_cars,
  sum(net_conversion_value_sum_total) as net_conversion_value_sum_total
from
/* impressions */
(
  select
    ad_copy_id,
    advertisement_id,
    cell_id,
    advertiser_id,
    auction_position,
    request_id as i_request_id,
    requested_at,
    external_id,
    actual_cpc
  from intent_media_log_data_production.impressions
  where requested_at_date_in_et between '2014-07-31' and '2014-07-31'
    and ip_address_blacklisted = 0
) i_final
left join -- ad calls --
  (
    select
      request_id as ac_request_id,
      requested_at_in_et,
      requested_at_date_in_et,
      product_category_type,
      ad_unit_id,
      ad_unit_type,
      site_type,
      trip_type,
      multivariate_test_attributes_variable,
      ct_layout_type,
      right_rail_ct_layout_type,
      inter_card_ct_layout_type,
      footer_ct_layout_type,
      destination_code,
      origination_code,
      travel_date_start,
      travel_date_end,
      travelers,
      model_slice_id
    from intent_media_log_data_production.ad_calls
    where requested_at_date_in_et between '2014-07-31' and '2014-07-31'
        and ip_address_blacklisted = 0
        and outcome_type = 'SERVED'
        and ad_unit_type = 'CT'
  ) ac
on ac.request_id = i_final.request_id
left join -- clicks with conversions --
(
  select
    vcl.*,
    conversion_count_flights,
    conversion_count_hotels,
    conversion_count_packages,
    conversion_count_cars,
    conversion_count_total,
    conversion_value_sum_flights,
    conversion_value_sum_hotels,
    conversion_value_sum_packages,
    conversion_value_sum_cars,
    conversion_value_sum_total,
    net_conversion_value_sum_flights,
    net_conversion_value_sum_hotels,
    net_conversion_value_sum_packages,
    net_conversion_value_sum_cars,
    net_conversion_value_sum_total
    from
    (
      select cl.*
      from
      (
        select
          clicks.request_id as click_request_id,
          clicks.requested_at as click_requested_at,
          clicks.webuser_id,
          clicks.external_impression_id,
          cl_i.advertiser_id,
          clicks.actual_cpc
        from intent_media_log_data_production.clicks
        left join
        (
          select
            request_id,
            requested_at,
            external_id,
            advertiser_id,
            auction_position
          from intent_media_log_data_production.impressions
          where requested_at_date_in_et between '2014-07-31' and '2014-07-31'
            and ip_address_blacklisted = 0
        ) cl_i
        on cl_i.external_id = clicks.external_impression_id
        where clicks.requested_at_date_in_et between '2014-07-31' and (date('2014-07-31') + interval '31 days')
          and clicks.ip_address_blacklisted = 0
          and clicks.fraudulent = 0
      ) cl
      left join
      (
        select
          request_id,
          requested_at,
          external_id,
          advertiser_id,
          auction_position
        from intent_media_log_data_production.impressions
        where requested_at_date_in_et between '2014-07-31' and '2014-07-31'
          and ip_address_blacklisted = 0
      ) vcl_i
      on vcl_i.external_id = cl.external_impression_id
      where vcl_i.requested_at + interval '1 day' >= cl.click_requested_at
    ) vcl
    left join -- deduped con by click --
        (select
            click_request_id,
            count(case when product_category_type = 'FLIGHTS' then conversion_request_id end) as conversion_count_flights,
            count(case when product_category_type = 'HOTELS' then conversion_request_id end) as conversion_count_hotels,
            count(case when product_category_type = 'PACKAGES' then conversion_request_id end) as conversion_count_packages,
            count(case when product_category_type = 'CARS' then conversion_request_id end) as conversion_count_cars,
            count(conversion_request_id) as conversion_count_total,
            sum(case when product_category_type = 'FLIGHTS' then conversion_value end) as conversion_value_sum_flights,
            sum(case when product_category_type = 'HOTELS' then conversion_value end) as conversion_value_sum_hotels,
            sum(case when product_category_type = 'PACKAGES' then conversion_value end) as conversion_value_sum_packages,
            sum(case when product_category_type = 'CARS' then conversion_value end) as conversion_value_sum_cars,
            sum(conversion_value) as conversion_value_sum_total,
            sum(case when product_category_type = 'FLIGHTS' then net_conversion_value end) as net_conversion_value_sum_flights,
            sum(case when product_category_type = 'HOTELS' then net_conversion_value end) as net_conversion_value_sum_hotels,
            sum(case when product_category_type = 'PACKAGES' then net_conversion_value end) as net_conversion_value_sum_packages,
            sum(case when product_category_type = 'CARS' then net_conversion_value end) as net_conversion_value_sum_cars,
            sum(net_conversion_value) as net_conversion_value_sum_total
        from
            (select *
            from
            (
                select
                    deduped_con.*,
                    vcl.click_request_id,
                    rank() over (partition by conversion_request_id order by click_requested_at desc) as click_rank
                from -- deduped con --
                (
                    select con.*
                    from
                    (
                        select
                            con.entity_id,
                            con.order_id,
                            min(con.conversion_requested_at) as min_requested_at
                        from
                        (
                            select
                                entity_id,
                                order_id,
                                request_id as conversion_request_id,
                                requested_at as conversion_requested_at,
                                product_category_type,
                                webuser_id,
                                conversion_value,
                                net_conversion_value,
                                site_type
                            from intent_media_log_data_production.conversions
                            where requested_at_date_in_et between '2014-07-31' and (date('2014-07-31') + interval '31 days')
                                and ip_address_blacklisted = 0
                        ) con
                        where con.order_id is not null
                        group by
                            con.entity_id,
                            con.order_id
                    ) distinct_con
                    left join
                    (
                        select
                            entity_id,
                            order_id,
                            request_id as conversion_request_id,
                            requested_at as conversion_requested_at,
                            product_category_type,
                            webuser_id,
                            conversion_value,
                            net_conversion_value,
                            site_type
                        from intent_media_log_data_production.conversions
                        where requested_at_date_in_et between '2014-07-31' and (date('2014-07-31') + interval '31 days')
                            and ip_address_blacklisted = 0
                    ) con
                    on con.entity_id = distinct_con.entity_id
                    and con.order_id = distinct_con.order_id
                    and con.conversion_requested_at = distinct_con.min_requested_at
                    union
                    select *
                    from
                    (
                        select
                            entity_id,
                            order_id,
                            request_id as conversion_request_id,
                            requested_at as conversion_requested_at,
                            product_category_type,
                            webuser_id,
                            conversion_value,
                            net_conversion_value,
                            site_type
                        from intent_media_log_data_production.conversions
                        where requested_at_date_in_et between '2014-07-31' and (date('2014-07-31') + interval '31 days')
                            and ip_address_blacklisted = 0
                    ) con
                    where con.order_id is null
                ) deduped_con
                cross join -- vcl --
                    (select cl.*
                    from 
                        (select
                            clicks.request_id as click_request_id,
                            clicks.requested_at as click_requested_at,
                            clicks.webuser_id,
                            clicks.external_impression_id,
                            cl_i.advertiser_id,
                            clicks.actual_cpc
                        from intent_media_log_data_production.clicks
                        left join 
                            (select
                                request_id,
                                requested_at,
                                external_id,
                                advertiser_id,
                                auction_position    
                            from intent_media_log_data_production.impressions
                            where requested_at_date_in_et between '2014-07-31' and '2014-07-31'
                                and ip_address_blacklisted = 0) cl_i
                            on cl_i.external_id = clicks.external_impression_id
                        where clicks.requested_at_date_in_et between '2014-07-31' and (date('2014-07-31') + interval '31 days')
                            and clicks.ip_address_blacklisted = 0
                            and clicks.fraudulent = 0) cl
                        left join
                        (select
                            request_id,
                            requested_at,
                            external_id,
                            advertiser_id,
                            auction_position    
                        from intent_media_log_data_production.impressions
                        where requested_at_date_in_et between '2014-07-31' and '2014-07-31'
                            and ip_address_blacklisted = 0) vcl_i
                        on vcl_i.external_id = cl.external_impression_id
                        where vcl_i.requested_at + interval '1 day' >= cl.click_requested_at) vcl
                where vcl.webuser_id = deduped_con.webuser_id
                    and vcl.advertiser_id = deduped_con.entity_id
                    and (vcl.click_requested_at + interval '30 days') >= deduped_con.conversion_requested_at
                    and vcl.click_requested_at < deduped_con.conversion_requested_at
            ) deduped_con_cl_rank
            where click_rank = 1
            ) con_attribution
        group by 
            click_request_id
    ) deduped_con_by_click
    on vcl.click_request_id = deduped_con_by_click.click_request_id) clicks_with_conversions
on clicks_with_conversions.external_impression_id = i_final.external_id
group by
    ac.request_id,
    ac.product_category_type,
    i_final.external_id,
    i_final.advertiser_id,
    ac.requested_at_in_et,
    ac.requested_at_date_in_et,
    ac.ad_unit_id,
    ac.ad_unit_type,
    ac.site_type,
    ac.trip_type,
    i_final.auction_position;