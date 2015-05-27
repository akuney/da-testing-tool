/* Query for return rate */
/* Entire Population */
select
    acc.requested_at_date_in_et,
    count(acc.publisher_user_id) as users,
    sum(served_ad_calls) as served_ad_calls,
    sum(interactions) as interactions,
    sum(clicks) as clicks,
    sum(revenue) as revenue
from
(
    select
        ac_c.requested_at_date_in_et,
        ac_c.publisher_user_id,
        count(ac_c.request_id) as served_ad_calls,
        sum(interactions) as interactions,
        sum(clicks) as clicks,
        sum(revenue) as revenue
    from
    (
        select
            min(ac.requested_at_date_in_et) as requested_at_date_in_et,
            min(ac.publisher_user_id) as publisher_user_id,
            ac.request_id,
            case when count(c.request_id) > 0 then 1 else 0 end as interactions,
            count(c.request_id) as clicks,
            sum(c.actual_cpc) as revenue
        from intent_media_log_data_production.ad_calls ac
        left join intent_media_log_data_production.clicks c
        on ac.request_id = c.ad_call_request_id
        and ac.requested_at_in_et < c.requested_at_in_et
        and ac.requested_at_in_et + interval '24 hours' >= c.requested_at_in_et
        where ac.requested_at_date_in_et = date(current_timestamp at timezone 'America/New_York') - interval '2 days'
            and ac.ip_address_blacklisted = 0
            and ac.outcome_type = 'SERVED'
            and ac.site_type = 'HOTWIRE'
            and ac.product_category_type = 'HOTELS'
            and ac.ad_unit_type = 'CT'
            and ac.publisher_user_id is not null
            and (c.requested_at_date_in_et is null or c.requested_at_date_in_et between date(current_timestamp at timezone 'America/New_York') - interval '2 days' and date(current_timestamp at timezone 'America/New_York') - interval '1 days')
            and (c.ip_address_blacklisted is null or c.ip_address_blacklisted = 0)
            and (c.fraudulent is null or c.fraudulent = 0)
            and (c.site_type is null or c.site_type = 'HOTWIRE')
            and (c.product_category_type is null or c.product_category_type = 'HOTELS')
        group by
            ac.request_id
    ) ac_c
  group by
      ac_c.requested_at_date_in_et,
      ac_c.publisher_user_id
) acc
group by
    acc.requested_at_date_in_et;

/* Return Population */
select
    acc.requested_at_date_in_et,
    count(acc.publisher_user_id) as users,
    sum(served_ad_calls) as served_ad_calls,
    sum(interactions) as interactions,
    sum(clicks) as clicks,
    sum(revenue) as revenue
from
(
    select
        requested_at_date_in_et,
        publisher_user_id,
        count(request_id) as served_ad_calls,
        sum(interactions) as interactions,
        sum(clicks) as clicks,
        sum(revenue) as revenue
    from
    (
        select
            min(ac.requested_at_date_in_et) as requested_at_date_in_et,
            min(ac.publisher_user_id) as publisher_user_id,
            ac.request_id,
            case when count(c.request_id) > 0 then 1 else 0 end as interactions,
            count(c.request_id) as clicks,
            sum(c.actual_cpc) as revenue
        from intent_media_log_data_production.ad_calls ac
        left join intent_media_log_data_production.clicks c
        on ac.request_id = c.ad_call_request_id
        and ac.requested_at_in_et < c.requested_at_in_et
        and ac.requested_at_in_et + interval '24 hours' >= c.requested_at_in_et
        where ac.requested_at_date_in_et = date(current_timestamp at timezone 'America/New_York') - interval '2 days'
            and ac.ip_address_blacklisted = 0
            and ac.outcome_type = 'SERVED'
            and ac.site_type = 'HOTWIRE'
            and ac.product_category_type = 'HOTELS'
            and ac.ad_unit_type = 'CT'
            and ac.publisher_user_id is not null
            and (c.requested_at_date_in_et is null or c.requested_at_date_in_et between date(current_timestamp at timezone 'America/New_York') - interval '2 days' and date(current_timestamp at timezone 'America/New_York') - interval '1 days')
            and (c.ip_address_blacklisted is null or c.ip_address_blacklisted = 0)
            and (c.fraudulent is null or c.fraudulent = 0)
            and (c.site_type is null or c.site_type = 'HOTWIRE')
            and (c.product_category_type is null or c.product_category_type = 'HOTELS')
        group by
            ac.request_id
    ) ac_c
    group by
        requested_at_date_in_et,
        publisher_user_id
) acc
inner join
(
    select
        t.publisher_user_id
    from
    (
        select distinct publisher_user_id
        from intent_media_log_data_production.ad_calls
        where requested_at_date_in_et = date(current_timestamp at timezone 'America/New_York') - interval '2 days'
            and ip_address_blacklisted = 0
            and outcome_type = 'SERVED'
            and site_type = 'HOTWIRE'
            and product_category_type = 'HOTELS'
            and ad_unit_type = 'CT'
            and publisher_user_id is not null
    ) lag
    inner join
    (
        select distinct publisher_user_id
        from intent_media_log_data_production.ad_calls
        where requested_at_date_in_et = date(current_timestamp at timezone 'America/New_York') - interval '1 days'
            and ip_address_blacklisted = 0
            and outcome_type = 'SERVED'
            and site_type = 'HOTWIRE'
            and product_category_type = 'HOTELS'
            and ad_unit_type = 'CT'
            and publisher_user_id is not null
    ) t
    on lag.publisher_user_id = t.publisher_user_id
) r
on acc.publisher_user_id = r.publisher_user_id
group by
    acc.requested_at_date_in_et