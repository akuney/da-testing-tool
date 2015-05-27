/* 'Table [n]' numbering in the tables below is based on SP's PPA VPC queries here:
  https://github.com/intentmedia/data/blob/master/dataSci/src/sql/ppa_auction_vpc.sql */
  
/* Table 1 - Ad Calls */
drop table if exists intent_media_sandbox_production.JS_PPA_auction_vpc_ac_kayak;
create table intent_media_sandbox_production.JS_PPA_auction_vpc_ac_kayak as
    select 
        site_type,
        publisher_user_id,
        request_id,
        request_correlation_id,
        result_page_number,
        rank_in_page,
        requested_at_in_et,
        webuser_id,
        CASE WHEN expedia_cookie_aspp LIKE '%FRONT_DOOR%' THEN 1 ELSE 0 END as front_door,
        CASE WHEN expedia_cookie_aspp LIKE '%FLOATING_COMPARE%' THEN 1 ELSE 0 END as floating_compare,
        CASE WHEN (expedia_cookie_aspp LIKE '%FRONT_DOOR%' OR expedia_cookie_aspp LIKE '%FLOATING_COMPARE%')
           THEN 0 ELSE 1 END as other
        from intent_media_log_data_production.ad_calls
        where requested_at_date_in_et between '2014-06-01' and '2014-08-31'
            and ip_address_blacklisted = false
--            and site_type = 'ORBITZ_GLOBAL'
--            and site_type = 'EXPEDIA'
--               and site_type = 'HOTWIRE'
--               and site_type = 'BOOKIT'
            and site_type = 'TVLY'
              and product_category_type = 'HOTELS'
            and outcome_type = 'SERVED'
--             and ad_unit_type = 'CT'
--             and ad_unit_type = 'SSR'
            and ad_unit_type = 'CT'
--             and (referrer_url LIKE '%kayak%' OR expedia_cookie_aspp LIKE '%kayak%');
--             and orbitz_referring_source LIKE '%kayak%';
--             and (hotwire_marketing_code LIKE '%S281%' OR 
--                  hotwire_marketing_code LIKE '%S312%' OR
--                  hotwire_marketing_code LIKE '%S358%');
            and referrer_url LIKE '%kayak%';

            

/* Table 7 - Deduped Conversions */
drop table if exists intent_media_sandbox_production.JS_PPA_auction_vpc_con_kayak;
create table intent_media_sandbox_production.JS_PPA_auction_vpc_con_kayak as
    select 
        con.request_id,
        con.requested_at_in_et,
        con.site_id,
        con.entity_id,
        con.order_id,
        con.publisher_user_id,
        con.webuser_id,
        con.conversion_value,
        con.net_conversion_value
    from intent_media_log_data_production.conversions con
    inner join 
        (select 
            entity_id,
            order_id,
            min(requested_at_in_et) as min_requested_at
        from intent_media_log_data_production.conversions
        where requested_at_date_in_et between '2014-06-01' and (date('2014-08-31') + interval '31 day')
            and ip_address_blacklisted = 0
            and order_id is not null
            and net_conversion_value is not null
--             and site_id = 6 -- EXPEDIA
--             and entity_id = 45 -- Conversion on Expedia (not the duplicate on 59777, Expedia-Ads)
--             and site_id = 2 -- ORBITZ_GLOBAL
--             and entity_id = 55
--            and site_id = 12
--            and entity_id = 85
--            and site_id = 13 -- BOOKIT
--            and entity_id = 95
              and site_id = 5 -- TRAVELOCITY_ON_EXPEDIA
              and entity_id = 65
        group by 
            entity_id,
            order_id) e_o_min 
    on con.entity_id = e_o_min.entity_id 
    and con.order_id = e_o_min.order_id 
    and con.requested_at_in_et = e_o_min.min_requested_at
    
    union
    
    select 
        request_id,
        requested_at_in_et,
        site_id,
        entity_id,
        order_id, 
        publisher_user_id,
        webuser_id,
        conversion_value,
        net_conversion_value
    from intent_media_log_data_production.conversions
    where requested_at_date_in_et between '2014-06-01' and (date('2014-08-31') + interval '31 day')
        and ip_address_blacklisted = 0
        and order_id is null
        and net_conversion_value is not null
--         and site_id = 6 -- EXPEDIA
--         and entity_id = 45;
--         and site_id = 2 -- ORBITZ_GLOBAL
--         and entity_id = 55;
--         and site_id = 12 -- HOTWIRE
--         and entity_id = 85;
--        and site_id = 13 -- BOOKIT
--        and entity_id = 95;
       and site_id = 5 -- TRAVELOCITY_ON_EXPEDIA
       and entity_id = 65;
       
        
        
/* Table 8 - Reference Table for the time difference between Ad Calls and Conversions */
drop table if exists intent_media_sandbox_production.JS_PPA_auction_vpc_ac_con_time_diff_kayak;
create table intent_media_sandbox_production.JS_PPA_auction_vpc_ac_con_time_diff_kayak as
        select
            con.request_id as con_request_id,
            ac.request_id as c_request_id,
            con.requested_at_in_et as con_requested_at,
            ac.requested_at_in_et as c_requested_at,
            con.net_conversion_value as net_conversion_value,
            datediff('ss', ac.requested_at_in_et, con.requested_at_in_et) as con_ac_time_diff
        from intent_media_sandbox_production.JS_PPA_auction_vpc_con_kayak con
        inner join intent_media_sandbox_production.JS_PPA_auction_vpc_ac_kayak ac 
            on con.webuser_id = ac.webuser_id; -- just join on webuser_id (no additional site_id),
            -- since this is all on EXP        


/* Table 9 - Attributed Conversion with Ad Calls (30 days) */
drop table if exists intent_media_sandbox_production.JS_PPA_auction_vpc_con_with_ac_kayak;
create table intent_media_sandbox_production.JS_PPA_auction_vpc_con_with_ac_kayak as
    select
        ac.publisher_user_id,
        ac.webuser_id,
        ac.request_id,
        ac.requested_at_in_et,
        ac.front_door,
        ac.floating_compare,
        ac.other,
        case when att_con.conversions is null then 0 else att_con.conversions end as clicked_conversions,
        case when att_con.total_conversion_value is null then 0 else att_con.total_conversion_value end as total_conversion_value
    from intent_media_sandbox_production.JS_PPA_auction_vpc_ac_kayak ac
    left join 
        (select
            con_c.c_request_id,
            count(con_c.con_request_id) as conversions,
            sum(con_c.net_conversion_value) as total_conversion_value
        from intent_media_sandbox_production.JS_PPA_auction_vpc_ac_con_time_diff_kayak con_c
        inner join 
            (select 
                con_request_id,
                min(con_ac_time_diff) as min_con_ac_time_diff
            from intent_media_sandbox_production.JS_PPA_auction_vpc_ac_con_time_diff_kayak
            where con_ac_time_diff between 1 and 2592000
            group by 
                con_request_id) min_con_ac_time_diff 
        on con_c.con_request_id = min_con_ac_time_diff.con_request_id 
        and con_c.con_ac_time_diff = min_con_ac_time_diff.min_con_ac_time_diff
        group by 
            con_c.c_request_id) att_con
    on ac.request_id = att_con.c_request_id;
