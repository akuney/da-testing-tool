/* Table 1 - Ad Calls */
drop table if exists intent_media_sandbox_production.SP_PPA_auction_vpc_ac;
create table intent_media_sandbox_production.SP_PPA_auction_vpc_ac as
    select 
        site_type,
        publisher_user_id,
        request_id,
        hotel_property_id,
        requested_at_in_et
        from intent_media_log_data_production.ad_calls
        where requested_at_date_in_et between '2014-07-01' and '2014-07-31'
            and ip_address_blacklisted = false
	    and site_type in ('CHEAPTICKETS', 'EBOOKERS',  'ORBITZ_GLOBAL')
            and ad_unit_type = 'META'
            and outcome_type = 'SERVED'
            and hotel_property_id is not null;
    

 /* Table 2 - Advertiser Ids from Impressions */   
drop table if exists intent_media_sandbox_production.SP_PPA_auction_vpc_i
;
create table
 intent_media_sandbox_production.SP_PPA_auction_vpc_i as
select request_id,
       external_id, 
       advertiser_id
from intent_media_log_data_production.impressions
where requested_at_date_in_et between '2014-07-01' and '2014-07-31'
	AND ip_address_blacklisted = false
	AND ad_unit_id in (129, 143, 89, 116)
;

/* Table 3 - Ad Calls with Advertiser Id from Impressions */   
drop table if exists intent_media_sandbox_production.SP_PPA_auction_vpc_ac_i;
create table intent_media_sandbox_production.SP_PPA_auction_vpc_ac_i as
    select
     ac.*,
     i.external_id,
     i.advertiser_id 
    from intent_media_sandbox_production.SP_PPA_auction_vpc_ac ac
      left join intent_media_sandbox_production.SP_PPA_auction_vpc_i i
    on ac.request_id = i.request_id
;
 
 
/* Table 3c - Filtering for only advertisers who are also publishers
   and we have piggy-backed conversion data. For all advertisers, 
   simply use SP_PPA_auction_vpc_ac_m */
drop table if exists intent_media_sandbox_production.SP_PPA_auction_vpc_ac_p;
create table intent_media_sandbox_production.SP_PPA_auction_vpc_ac_p as 
     select
     ac.*
     from intent_media_sandbox_production.SP_PPA_auction_vpc_ac_i ac
     inner join intent_media_sandbox_production.conversion_piggybacked_advertisers p
     on ac.advertiser_id = p.advertiser_id
;

/* Table 4 - Reference Table - advertisements, ad_groups, campaigns */
drop table if exists intent_media_sandbox_production.SP_PPA_auction_vpc_ad;
create table intent_media_sandbox_production.SP_PPA_auction_vpc_ad as
    select
        ad.id as advertisement_id,
        c.advertiser_id
    from intent_media_sandbox_production.SP_advertisements_copy ad
    inner join intent_media_production.ad_groups ag on ad.ad_group_id = ag.id
    inner join intent_media_production.campaigns c on c.id = ag.campaign_id
;
   
 /* Table 5 - Clicks */
drop table if exists intent_media_sandbox_production.SP_PPA_auction_vpc_c;
create table intent_media_sandbox_production.SP_PPA_auction_vpc_c as
    select
        c.site_type, 
        c.request_id,
        c.requested_at_in_et,
        c.ad_call_request_id,
        c.external_impression_id,
        c.publisher_user_id,
        c.webuser_id,
        c.actual_cpc,
        ad.advertiser_id
    from intent_media_log_data_production.clicks c
    inner join intent_media_log_data_production.ad_calls ac
        on c.ad_call_request_id = ac.request_id
    inner join intent_media_sandbox_production.SP_PPA_auction_vpc_ad ad
        on c.advertisement_id = ad.advertisement_id
    where c.requested_at_date_in_et between '2014-07-01' and (date('2014-07-31') + interval '1 day')
        and ac.requested_at_date_in_et between '2014-07-01' and '2014-07-31'
        and ac.ip_address_blacklisted = 0
        and c.ip_address_blacklisted = 0
        and c.fraudulent = 0
        and c.site_type in ('CHEAPTICKETS', 'EBOOKERS',  'ORBITZ_GLOBAL')
        and ac.site_type in ('CHEAPTICKETS', 'EBOOKERS',  'ORBITZ_GLOBAL')
        and ac.outcome_type = 'SERVED'
        and ac.ad_unit_type = 'META'
        and ac.product_category_type = 'HOTELS'; 

/* Table 6 - Advertiser Piggybacked Site Id */
drop table if exists intent_media_sandbox_production.SP_PPA_auction_vpc_c_p;
create table intent_media_sandbox_production.SP_PPA_auction_vpc_c_p as 
     select
     c.*,
     p.site_id as piggybacked_advertiser_site_id
     from intent_media_sandbox_production.SP_PPA_auction_vpc_c c
     inner join intent_media_sandbox_production.conversion_piggybacked_advertisers p
     on c.advertiser_id = p.advertiser_id
;
        
/* Table 7 - Deduped Conversions */
drop table if exists intent_media_sandbox_production.SP_PPA_auction_vpc_con;
create table intent_media_sandbox_production.SP_PPA_auction_vpc_con as
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
        where requested_at_date_in_et between '2014-07-01' and (date('2014-07-31') + interval '31 day')
            and ip_address_blacklisted = 0
            and order_id is not null
            and net_conversion_value is not null
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
    where requested_at_date_in_et between '2014-07-01' and (date('2014-07-31') + interval '31 day')
        and ip_address_blacklisted = 0
        and order_id is null
        and net_conversion_value is not null
;

/* Table 8 - Reference Table for the time difference between Clicks and Conversions */
drop table if exists intent_media_sandbox_production.SP_PPA_auction_vpc_con_c;
create table intent_media_sandbox_production.SP_PPA_auction_vpc_con_c as
        select
            con.request_id as con_request_id,
            c.request_id as c_request_id,
            con.requested_at_in_et as con_requested_at,
            c.requested_at_in_et as c_requested_at,
            con.net_conversion_value as net_conversion_value,
            datediff('ss', c.requested_at_in_et, con.requested_at_in_et) as con_c_time_diff
        from intent_media_sandbox_production.SP_PPA_auction_vpc_con con
        inner join intent_media_sandbox_production.SP_PPA_auction_vpc_c_p c 
            on con.webuser_id = c.webuser_id
            and con.site_id = c.piggybacked_advertiser_site_id;

/* Table 9 - Attributed Conversion with Clicks (30 days) */
drop table if exists intent_media_sandbox_production.SP_PPA_auction_vpc_c_con;
create table intent_media_sandbox_production.SP_PPA_auction_vpc_c_con as
    select
        c.ad_call_request_id,
        c.publisher_user_id,
        c.webuser_id,
        c.request_id,
        c.requested_at_in_et,
        c.actual_cpc,
        c.external_impression_id,
        case when att_con.conversions is null then 0 else att_con.conversions end as clicked_conversions,
        case when att_con.total_conversion_value is null then 0 else att_con.total_conversion_value end as total_conversion_value
    from intent_media_sandbox_production.SP_PPA_auction_vpc_c c
    left join 
        (select
            con_c.c_request_id,
            count(con_c.con_request_id) as conversions,
            sum(con_c.net_conversion_value) as total_conversion_value
        from intent_media_sandbox_production.SP_PPA_auction_vpc_con_c con_c
        inner join 
            (select 
                con_request_id,
                min(con_c_time_diff) as min_con_c_time_diff
            from intent_media_sandbox_production.SP_PPA_auction_vpc_con_c
            where con_c_time_diff between 1 and 2592000
            group by 
                con_request_id) min_con_c_time_diff 
        on con_c.con_request_id = min_con_c_time_diff.con_request_id 
        and con_c.con_c_time_diff = min_con_c_time_diff.min_con_c_time_diff
        group by 
            con_c.c_request_id) att_con
    on c.request_id = att_con.c_request_id;

    
/* Table 10 - Join Table 3 0r 3a (ad calls) with Table 9 (clicks + deduped_conv) */
drop table if exists intent_media_sandbox_production.SP_PPA_auction_vpc_ac_c;
create table intent_media_sandbox_production.SP_PPA_auction_vpc_ac_c as
    select 
        ac.*,
        case when c.actual_cpc is null then 0 else 1 end as click_count,
        case when c.actual_cpc is null then 0 else c.actual_cpc end as cpc_revenue,
        case when c.clicked_conversions is null then 0 else c.clicked_conversions end as clicked_conversion_count,
        case when c.total_conversion_value is null then 0 else c.total_conversion_value end as clicked_conversion_value
    from intent_media_sandbox_production.SP_PPA_auction_vpc_ac_p ac
    left join intent_media_sandbox_production.SP_PPA_auction_vpc_c_con c 
    on ac.external_id = c.external_impression_id;

/* Table 11 - Final table */
drop table if exists intent_media_sandbox_production.SP_PPA_auction_vpc_final;
create table intent_media_sandbox_production.SP_PPA_auction_vpc_final as
    select 
        hotel_property_id,
        advertiser_id,
        count(1) as impressions,
        sum(click_count) as clicks,
        sum(cpc_revenue) as cpc_revenue,
        sum(clicked_conversion_count) as clicked_conversion_count,
        sum(clicked_conversion_value) as clicked_conversion_value,
        case when sum(click_count) = 0 then -2.0
             when sum(clicked_conversion_count) = 0 then -1.0 
             else sum(clicked_conversion_value)/sum(cpc_revenue) end as ROI
    from intent_media_sandbox_production.SP_PPA_auction_vpc_ac_c
    where advertiser_id is not null
    group by 
        hotel_property_id,
        advertiser_id
;

/* Analysis */
drop table if exists intent_media_sandbox_production.SP_PPA_auction_vpc_results;
create table intent_media_sandbox_production.SP_PPA_auction_vpc_results as
select
 hotel_property_id,
 ROI,
 case when ROI <= -1.1 then 'NO_CLICKS' 
      when (ROI > -2.0 and ROI < -0.1) then 'NO_CONVERSIONS' 
      when ROI >= 1 then 'ROI_POSITIVE' 
      else 'ROI_NEGATIVE' end as roi_status
from
intent_media_sandbox_production.SP_PPA_auction_vpc_final
;

select 
roi_status,
count(*)
from
intent_media_sandbox_production.SP_PPA_auction_vpc_results
group by 1;

select 
-- clicks,
clicked_conversion_count,
count(*)
from
intent_media_sandbox_production.SP_PPA_auction_vpc_final
group by 1
order by 1 asc;


select 
* 
from 
intent_media_sandbox_production.SP_PPA_auction_vpc_con
order by request_id;