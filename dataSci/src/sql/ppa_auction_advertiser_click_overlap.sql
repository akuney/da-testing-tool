/* Table 1 - Ad Calls */
drop table if exists intent_media_sandbox_production.SP_PPA_auction_click_overlap_ac;
create table intent_media_sandbox_production.SP_PPA_auction_click_overlap_ac as
    select 
        site_type,
        publisher_user_id,
        request_id,
        hotel_property_id,
        market_id,
        requested_at_in_et
        from intent_media_log_data_production.ad_calls
        where requested_at_date_in_et between '2014-07-01' and '2014-09-30'
            and ip_address_blacklisted = false
	    and site_type in ('CHEAPTICKETS', 'EBOOKERS',  'ORBITZ_GLOBAL')
            and ad_unit_type = 'META'
            and outcome_type = 'SERVED'
            and hotel_property_id is not null;
    

 /* Table 2 - Advertiser Ids from Impressions */   
drop table if exists intent_media_sandbox_production.SP_PPA_auction_click_overlap_i
;
create table
 intent_media_sandbox_production.SP_PPA_auction_click_overlap_i as
select request_id,
       external_id, 
       advertiser_id
from intent_media_log_data_production.impressions
where requested_at_date_in_et between '2014-07-01' and '2014-09-30'
	AND ip_address_blacklisted = false
	AND ad_unit_id in (129, 143, 89, 116)
;

/* Table 3 - Ad Calls with Advertiser Id from Impressions */   
drop table if exists intent_media_sandbox_production.SP_PPA_auction_click_overlap_ac_i;
create table intent_media_sandbox_production.SP_PPA_auction_click_overlap_ac_i as
    select
     ac.*,
     i.external_id,
     i.advertiser_id 
    from intent_media_sandbox_production.SP_PPA_auction_click_overlap_ac ac
      left join intent_media_sandbox_production.SP_PPA_auction_click_overlap_i i
    on ac.request_id = i.request_id
;
 
/* Table 4 - Reference Table - advertisements, ad_groups, campaigns */
drop table if exists intent_media_sandbox_production.SP_PPA_auction_click_overlap_ad;
create table intent_media_sandbox_production.SP_PPA_auction_click_overlap_ad as
    select
        ad.id as advertisement_id,
        c.advertiser_id
    from intent_media_sandbox_production.SP_advertisements_copy ad
    inner join intent_media_production.ad_groups ag on ad.ad_group_id = ag.id
    inner join intent_media_production.campaigns c on c.id = ag.campaign_id
;
   
 /* Table 5 - Clicks */
drop table if exists intent_media_sandbox_production.SP_PPA_auction_click_overlap_c;
create table intent_media_sandbox_production.SP_PPA_auction_click_overlap_c as
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
    inner join intent_media_sandbox_production.SP_PPA_auction_click_overlap_ad ad
        on c.advertisement_id = ad.advertisement_id
    where c.requested_at_date_in_et between '2014-07-01' and (date('2014-09-30') + interval '1 day')
        and ac.requested_at_date_in_et between '2014-07-01' and '2014-09-30'
        and ac.ip_address_blacklisted = 0
        and c.ip_address_blacklisted = 0
        and c.fraudulent = 0
        and c.site_type in ('CHEAPTICKETS', 'EBOOKERS',  'ORBITZ_GLOBAL')
        and ac.site_type in ('CHEAPTICKETS', 'EBOOKERS',  'ORBITZ_GLOBAL')
        and ac.outcome_type = 'SERVED'
        and ac.ad_unit_type = 'META'
        and ac.product_category_type = 'HOTELS'; 


/* Table 10 - Join Table 3 0r 3a (ad calls) with Table 9 (clicks + deduped_conv) */
drop table if exists intent_media_sandbox_production.SP_PPA_auction_click_overlap_ac_c;
create table intent_media_sandbox_production.SP_PPA_auction_click_overlap_ac_c as
    select 
        ac.*,
        case when c.actual_cpc is null then 0 else 1 end as click_count,
        case when c.actual_cpc is null then 0 else c.actual_cpc end as cpc_revenue
    from intent_media_sandbox_production.SP_PPA_auction_click_overlap_ac_i ac
    left join intent_media_sandbox_production.SP_PPA_auction_click_overlap_c c 
    on ac.external_id = c.external_impression_id;

/* Table 11 - Final table */
drop table if exists intent_media_sandbox_production.SP_PPA_auction_click_overlap_final;
create table intent_media_sandbox_production.SP_PPA_auction_click_overlap_final as
    select   
        advertiser_id,
        hotel_property_id,
        max(market_id) as market_id,
        count(1) as impressions,
        sum(click_count) as clicks,
        sum(click_count)/count(1) as CTR,
        sum(cpc_revenue) as cpc_revenue
    from intent_media_sandbox_production.SP_PPA_auction_click_overlap_ac_c
    where advertiser_id is not null
    group by 
        advertiser_id,
        hotel_property_id
;

/* Analysis */
drop table if exists intent_media_sandbox_production.SP_PPA_auction_click_overlap_results;
create table intent_media_sandbox_production.SP_PPA_auction_click_overlap_results as
select
e.name as advertiser,
t1.*
from
intent_media_sandbox_production.SP_PPA_auction_click_overlap_final t1
join intent_media_production.entities e
on t1.advertiser_id = e.id
;
