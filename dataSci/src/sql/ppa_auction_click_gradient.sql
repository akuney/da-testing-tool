/* Table 1 - Ad Calls */
drop table if exists intent_media_sandbox_production.SP_PPA_auction_vpc_ac;
create table intent_media_sandbox_production.SP_PPA_auction_vpc_ac as
    select 
        site_type,
        publisher_user_id,
        request_id,
        request_correlation_id,
        result_page_number,
        rank_in_page,
        requested_at_in_et
        from intent_media_log_data_production.ad_calls
        where requested_at_date_in_et between '2014-07-01' and '2014-07-31'
            and ip_address_blacklisted = false
	    and site_type in ('CHEAPTICKETS', 'EBOOKERS',  'ORBITZ_GLOBAL')
            and ad_unit_type = 'META'
            and outcome_type = 'SERVED';
    
 /* Table 2 - Advertiser Ids from Impressions */   
drop table if exists intent_media_sandbox_production.SP_PPA_auction_vpc_i
;
create table
intent_media_sandbox_production.SP_PPA_auction_vpc_i as
select request_id,
       external_id, 
       advertiser_id,
       auction_position,
       hotel_average_nightly_rate,
       RANK() over (partition by request_id order by hotel_average_nightly_rate asc) as price_rank,
       case when max(hotel_average_nightly_rate) over (partition by request_id) > min(hotel_average_nightly_rate) over (partition by request_id) then 0 else 1 end as price_parity
from ( select request_id,
       external_id, 
       advertiser_id,
       auction_position,
       hotel_average_nightly_rate
from intent_media_log_data_production.impressions
where requested_at_date_in_et between '2014-07-01' and '2014-07-31'
	AND ip_address_blacklisted = false
	AND ad_unit_id in (129, 143, 89, 116)
	) raw_impression_data
order by request_id
;

/* Table 3 - Ad Calls with Advertiser Id from Impressions */   
drop table if exists intent_media_sandbox_production.SP_PPA_auction_vpc_ac_i;
create table intent_media_sandbox_production.SP_PPA_auction_vpc_ac_i as
    select
     ac.*,
     i.external_id,
     i.advertiser_id,
     i.auction_position, 
     i.hotel_average_nightly_rate,
     i.price_rank,
     i.price_parity
    from intent_media_sandbox_production.SP_PPA_auction_vpc_ac ac
      left join intent_media_sandbox_production.SP_PPA_auction_vpc_i i
    on ac.request_id = i.request_id
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

    
/* Table 6 - Join Table 3 (ad calls) with Table 5 (clicks) */
drop table if exists intent_media_sandbox_production.SP_PPA_auction_vpc_ac_c;
create table intent_media_sandbox_production.SP_PPA_auction_vpc_ac_c as
    select 
        ac.*,
        case when c.actual_cpc is null then 0 else 1 end as click_count,
        case when c.actual_cpc is null then 0 else c.actual_cpc end as cpc_revenue
    from 
        intent_media_sandbox_production.SP_PPA_auction_vpc_ac_i ac
    left join intent_media_sandbox_production.SP_PPA_auction_vpc_c c 
    on ac.external_id = c.external_impression_id;

/* Table 11 - Final table */
drop table if exists intent_media_sandbox_production.SP_PPA_auction_vpc_final;
create table intent_media_sandbox_production.SP_PPA_auction_vpc_final as
    select 
        site_type,
        advertiser_id,
        -- request_correlation_id,
        result_page_number,
        rank_in_page,
        auction_position,
        price_rank,
        price_parity,
        -- count(distinct(request_correlation_id)) as page_views,
        count(1) as impressions,
        sum(click_count) as clicks
    from intent_media_sandbox_production.SP_PPA_auction_vpc_ac_c
    where advertiser_id is not null
    group by 
        site_type,
        advertiser_id,
        -- request_correlation_id,
        result_page_number,
        rank_in_page,
        auction_position,
        price_rank,
        price_parity
    order by 
        site_type,
        advertiser_id,
        -- request_correlation_id,
        result_page_number,
        rank_in_page,
        auction_position   
;

/* Analysis */
drop table if exists intent_media_sandbox_production.SP_PPA_auction_vpc_results;
create table intent_media_sandbox_production.SP_PPA_auction_vpc_results as
select
e.name as advertiser,
t1.*
from
intent_media_sandbox_production.SP_PPA_auction_vpc_final t1
join intent_media_production.entities e
on t1.advertiser_id = e.id
;

