/* Travelzoo 
    - publisher_id = 112
    - site_id = 34
    - site_type = 'TRAVELZOO'
    - product_category_type = 'FLIGHTS'
*/

/* Finding 1 (for last 7 days)
    - Per site, date, and MVT version id,
        - count(request_id) = count(distinct request_id) >> nice, because I don't need to do "distinct" 
        - count(distinct publisher_user_id) < count(distinct webuser_id) >> which is more reliable measure to count the number of user?
*/

select  
    requested_at_date_in_et,
    multivariate_version_id,
    count(request_id) as page_load,
    count(distinct request_id) as distinct_page_load,
    count(distinct publisher_user_id) as pub_users,
    count(distinct webuser_id) as web_users
from intent_media_log_data_production.search_compare_form_events
where requested_at_date_in_et >= requested_at_date_in_et - interval '7 days'
    and ip_address_blacklisted = 0
    and site_id = 34
    and product_category_type = 'FLIGHTS'
group by 
    requested_at_date_in_et,
    multivariate_version_id
order by
    requested_at_date_in_et,
    multivariate_version_id;

/* Finding 2 (for last 7 days)
    - Majority of users have one MVT version id while there exist users with more than 1 MVT version id 
*/
    
select
    mvt_version_count,
    count(distinct publisher_user_id) as user_versions
from (
    select
        publisher_user_id,
        count(distinct multivariate_version_id) as mvt_version_count
    from intent_media_log_data_production.search_compare_form_events
    where requested_at_date_in_et >= requested_at_date_in_et - interval '7 days'
        and ip_address_blacklisted = 0
        and site_id = 34
        and product_category_type = 'FLIGHTS'
    group by
        publisher_user_id) per_user
group by
    mvt_version_count
order by
    mvt_version_count;
    
--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
/* Tableau Data Source Query */
select
    sfe.site_type as Site,
    sfe.product_category_type as "Product Category Type",
    sfe.browser as Browser,
    sfe.click_type as "Click Type",
    sfe.multivariate_version_id as "MVT Version ID",
    sfe.multivariate_test_attributes_variable as "Attribute Values JSON",
    ads.placement_type as "Placement Type",
    count(sfe.publisher_user_id) as users,
    sum(sfe.page_load_count) as page_load_count,
    sum(ads.ad_calls) as ad_calls,
    sum(ads.ad_calls * ads.ad_calls) as ad_calls_2_u,
    sum(ads.interactions) as interactions,
    sum(ads.interactions_2_ac) as interactions_2_ac,
    sum(ads.interactions * ads.interactions) as interactions_2_u,
    sum(ads.clicks) as clicks,
    sum(ads.clicks_2_ac) as clicks_2_ac,
    sum(ads.clicks * ads.clicks) as clicks_2_u,
    sum(ads.revenue) as revenue,
    sum(ads.revenue_2_ac) as revenue_2_ac,
    sum(ads.revenue * ads.revenue) as revenue_2_u
from
(
    select
        site_type,
        (case product_category_type
            when 'FLIGHTS' then 'Flights'
            when 'HOTELS' then 'Hotels'
            when 'PACKAGES' then 'Packages'
            when 'CARS' then 'Cars'
            else 'Unknown'
        end) as product_category_type,
        (case browser_family
            when 'IE' then 'IE'
            when 'CHROME' then 'Chrome'
            when 'FIREFOX' then 'Firefox'
            when 'SAFARI' then 'Safari'
            else 'Other'
        end) as browser,
        (case browser_family
            when 'CHROME' then 'Single'
            when 'IE' then 'Single'
        else 'Multi'
        end) as click_type,
        multivariate_version_id,
        multivariate_test_attributes_variable,
        publisher_user_id,
        requested_at,
        count(request_id) as page_load_count
    from intent_media_log_data_production.search_compare_form_events
    where requested_at_date_in_et >= '2014-07-01'
        and ip_address_blacklisted = 0
        and site_type = 'TRAVELZOO'
        and publisher_user_id not in
            (
                select
                    publisher_user_id
                from intent_media_log_data_production.search_compare_form_events
                where requested_at_date_in_et >= '2014-07-01'
                  and ip_address_blacklisted = 0
                  and site_type = 'TRAVELZOO'
                group by
                    publisher_user_id
                having
                    count(distinct browser_family) > 1
            )
    group by
        site_type,
        (case product_category_type
            when 'FLIGHTS' then 'Flights'
            when 'HOTELS' then 'Hotels'
            when 'PACKAGES' then 'Packages'
            when 'CARS' then 'Cars'
            else 'Unknown'
        end),
        (case browser_family
            when 'IE' then 'IE'
            when 'CHROME' then 'Chrome'
            when 'FIREFOX' then 'Firefox'
            when 'SAFARI' then 'Safari'
            else 'Other'
        end),
        (case browser_family
            when 'CHROME' then 'Single'
            when 'IE' then 'Single'
            else 'Multi'
        end),
        multivariate_version_id,
        multivariate_test_attributes_variable,
        publisher_user_id,
        requested_at
) sfe
left join
(
    select
        ac.site_type,
        (case ac.product_category_type
            when 'FLIGHTS' then 'Flights'
            when 'HOTELS' then 'Hotels'
            when 'PACKAGES' then 'Packages'
            when 'CARS' then 'Cars'
            else 'Unknown'
        end) as product_category_type,
        (case ac.browser_family
            when 'IE' then 'IE'
            when 'CHROME' then 'Chrome'
            when 'FIREFOX' then 'Firefox'
            when 'SAFARI' then 'Safari'
            else 'Other'
        end) as browser,
        (case ac.browser_family
            when 'CHROME' then 'Single'
            when 'IE' then 'Single'
        else 'Multi'
        end) as click_type,
        ac.multivariate_version_id,
        ac.multivariate_test_attributes_variable,
        ac.publisher_user_id,
        ac.requested_at,
        (case
          when count(distinct c.placement_type) > 1 then 'Mixed'
          else min(c.placement_type)
        end) as placement_type,
        count(ac.request_id) as ad_calls,
        sum(interactions) as interactions,
        sum(interactions * interactions) as interactions_2_ac,
        sum(clicks) as clicks,
        sum(clicks * clicks) as clicks_2_ac,
        sum(revenue) as revenue,
        sum(revenue * revenue) as revenue_2_ac
    from intent_media_log_data_production.ad_calls ac
    left join
    (
        select
            ad_call_request_id,
            requested_at,
            min(placement_type) as placement_type,
            case when count(request_id) > 0 then 1 else 0 end as interactions,
            count(request_id) as clicks,
            sum(actual_cpc) as revenue
        from intent_media_log_data_production.clicks
        where requested_at_date_in_et >= '2014-07-01'
            and ip_address_blacklisted = 0
            and fraudulent = 0
            and site_type = 'TRAVELZOO'
        group by
            ad_call_request_id,
            requested_at
    ) c
    on ac.request_id = c.ad_call_request_id
    and ac.requested_at + interval '24 hours' >= c.requested_at
    where ac.requested_at_date_in_et >= '2014-07-01'
        and ac.ip_address_blacklisted = 0
        and ac.outcome_type = 'SERVED'
        and ac.site_type = 'TRAVELZOO'
        and ac.ad_unit_type = 'CT'
        and ac.publisher_user_id not in
            (
                select
                    publisher_user_id
                from intent_media_log_data_production.ad_calls
                where requested_at_date_in_et >= '2014-07-01'
                  and ip_address_blacklisted = 0
                  and outcome_type='SERVED'
                  and ad_unit_type = 'CT'
                  and site_type = 'TRAVELZOO'
                group by
                    publisher_user_id
                having
                    count(distinct browser_family) > 1
            )
    group by
        ac.site_type,
        (case ac.product_category_type
            when 'FLIGHTS' then 'Flights'
            when 'HOTELS' then 'Hotels'
            when 'PACKAGES' then 'Packages'
            when 'CARS' then 'Cars'
            else 'Unknown'
        end),
        (case ac.browser_family
            when 'IE' then 'IE'
            when 'CHROME' then 'Chrome'
            when 'FIREFOX' then 'Firefox'
            when 'SAFARI' then 'Safari'
            else 'Other'
        end),
        (case ac.browser_family
            when 'CHROME' then 'Single'
            when 'IE' then 'Single'
            else 'Multi'
        end),
        ac.multivariate_version_id,
        ac.multivariate_test_attributes_variable,
        ac.publisher_user_id,
        ac.requested_at
) ads
on sfe.site_type = ads.site_type
and sfe.product_category_type = ads.product_category_type
and sfe.browser = ads.browser
and sfe.click_type = ads.click_type
and sfe.multivariate_version_id = ads.multivariate_version_id
and sfe.multivariate_test_attributes_variable = ads.multivariate_test_attributes_variable
and sfe.publisher_user_id = ads.publisher_user_id
and sfe.requested_at <= ads.requested_at
group by
    sfe.site_type,
    sfe.product_category_type,
    sfe.browser,
    sfe.click_type,
    sfe.multivariate_version_id,
    sfe.multivariate_test_attributes_variable,
    ads.placement_type