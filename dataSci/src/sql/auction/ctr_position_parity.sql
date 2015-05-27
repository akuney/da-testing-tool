/* Table 1 - Ad Calls */
drop table if exists intent_media_sandbox_production.AUCSIM_ppa_auction_position_parity_ac;
create table intent_media_sandbox_production.AUCSIM_ppa_auction_position_parity_ac as
    select 
        site_type,
        publisher_user_id,
        request_id,
        request_correlation_id,
        result_page_number,
        rank_in_page,
        requested_at_in_et
        from intent_media_log_data_production.ad_calls
        where requested_at_date_in_et between '2014-10-01' and '2014-10-31'
            and ip_address_blacklisted = false
            and ad_unit_type = 'META'
            and outcome_type = 'SERVED';

 /* Table 2 - Advertiser Ids from Impressions */   
drop table if exists intent_media_sandbox_production.AUCSIM_ppa_auction_position_parity_i
;
create table
intent_media_sandbox_production.AUCSIM_ppa_auction_position_parity_i as
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
where requested_at_date_in_et between '2014-10-01' and '2014-10-31'
    AND ip_address_blacklisted = false
    ) raw_impression_data
order by request_id
;

/* Table 3 - Ad Calls with Advertiser Id from Impressions */   
drop table if exists intent_media_sandbox_production.AUCSIM_ppa_auction_position_parity_ac_i;
create table intent_media_sandbox_production.AUCSIM_ppa_auction_position_parity_ac_i as
    select
     ac.*,
     i.external_id,
     i.advertiser_id,
     i.auction_position, 
     i.hotel_average_nightly_rate,
     i.price_rank,
     i.price_parity
    from intent_media_sandbox_production.AUCSIM_ppa_auction_position_parity_ac ac
      left join intent_media_sandbox_production.AUCSIM_ppa_auction_position_parity_i i
    on ac.request_id = i.request_id
;

/* Table 4 - Reference Table - advertisements, ad_groups, campaigns */
drop table if exists intent_media_sandbox_production.AUCSIM_ppa_auction_position_parity_ad;
create table intent_media_sandbox_production.AUCSIM_ppa_auction_position_parity_ad as
    select
        ad.id as advertisement_id,
        c.advertiser_id
    from intent_media_sandbox_production.advertisements ad
    inner join intent_media_production.ad_groups ag on ad.ad_group_id = ag.id
    inner join intent_media_production.campaigns c on c.id = ag.campaign_id
;
   
 /* Table 5 - Clicks */
drop table if exists intent_media_sandbox_production.AUCSIM_ppa_auction_position_parity_c;
create table intent_media_sandbox_production.AUCSIM_ppa_auction_position_parity_c as
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
    inner join intent_media_sandbox_production.AUCSIM_ppa_auction_position_parity_ad ad
        on c.advertisement_id = ad.advertisement_id
    where c.requested_at_date_in_et between '2014-10-01' and (date('2014-10-31') + interval '1 day')
        and ac.requested_at_date_in_et between '2014-10-01' and '2014-10-31'
        and ac.ip_address_blacklisted = 0
        and c.ip_address_blacklisted = 0
        and c.fraudulent = 0
        and c.placement_type = 'IN_CARD'
        and ac.outcome_type = 'SERVED'
        and ac.ad_unit_type = 'META'
        and ac.product_category_type = 'HOTELS';

    
/* Table 6 - Join Table 3 (ad calls) with Table 5 (clicks) */
drop table if exists intent_media_sandbox_production.AUCSIM_ppa_auction_position_parity_ac_c;
create table intent_media_sandbox_production.AUCSIM_ppa_auction_position_parity_ac_c as
    select 
        ac.*,
        case when c.actual_cpc is null then 0 else 1 end as click_count,
        case when c.actual_cpc is null then 0 else c.actual_cpc end as cpc_revenue
    from 
        intent_media_sandbox_production.AUCSIM_ppa_auction_position_parity_ac_i ac
    left join intent_media_sandbox_production.AUCSIM_ppa_auction_position_parity_c c 
    on ac.external_id = c.external_impression_id;

/* Table 11 - Final table */
drop table if exists intent_media_sandbox_production.AUCSIM_ppa_auction_position_parity_final;
create table intent_media_sandbox_production.AUCSIM_ppa_auction_position_parity_final as
    select 
        site_type,
        advertiser_id,
        result_page_number,
        rank_in_page,
        auction_position,
        price_rank,
        price_parity,
        count(1) as impressions,
        sum(click_count) as clicks
    from intent_media_sandbox_production.AUCSIM_ppa_auction_position_parity_ac_c
    where advertiser_id is not null
    group by 
        site_type,
        advertiser_id,
        result_page_number,
        rank_in_page,
        auction_position,
        price_rank,
        price_parity
    order by 
        site_type,
        advertiser_id,
        result_page_number,
        rank_in_page,
        auction_position   
;

SELECT 
s.id,  
p.site_type,
p.auction_position,  
sum(p.impressions) as num_impressions,
sum(p.clicks) as num_clicks, 
sum(p.clicks) / sum(impressions) as ctr
from intent_media_sandbox_production.AUCSIM_ppa_auction_ctr_position_parity_final p
left join sites s
on p.site_type = s.name
where price_parity = 1
group by s.id, site_type, auction_position
order by s.id, site_type, auction_position;