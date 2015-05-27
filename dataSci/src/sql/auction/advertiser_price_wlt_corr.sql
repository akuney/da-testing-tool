-- W/L/T correlation matrix
/* Table 1 - Ad Calls */
drop table if exists intent_media_sandbox_production.AUCSIM_ppa_auction_wlt_ac;
create table intent_media_sandbox_production.AUCSIM_ppa_auction_wlt_ac as
    select 
        site_id,
        request_id,
        hotel_property_id,
        publisher_hotel_price
        from intent_media_log_data_production.ad_calls
        where requested_at_date_in_et between '2014-10-01' and '2014-10-31'
            and ip_address_blacklisted = false
            and ad_unit_type = 'META'
            and outcome_type = 'SERVED';
    
 /* Table 2a - Advertiser Id 1 from Impressions */   
drop table if exists intent_media_sandbox_production.AUCSIM_ppa_auction_wlt_i1
;
create table
 intent_media_sandbox_production.AUCSIM_ppa_auction_wlt_i1 as
 select request_id,
        advertiser_id as advertiser_id1,
        hotel_average_nightly_rate as advertiser_hotel_price
        from intent_media_log_data_production.impressions
        where requested_at_date_in_et between '2014-10-01' and '2014-10-31'
    and ip_address_blacklisted = false;


/* Table 3a - Ad Calls with Advertiser Id 1 from Impressions */   
drop table if exists intent_media_sandbox_production.AUCSIM_ppa_auction_wlt_ac_i1;
create table intent_media_sandbox_production.AUCSIM_ppa_auction_wlt_ac_i1 as
    select
     ac.request_id,
     ac.site_id,
     i.advertiser_id1,
     ac.publisher_hotel_price,
     i.advertiser_hotel_price,
     case when TRUNC(advertiser_hotel_price) < TRUNC(publisher_hotel_price) then 1
          when TRUNC(advertiser_hotel_price) > TRUNC(publisher_hotel_price) then -1
          else 0 end as displayed_price_adv_win
     from intent_media_sandbox_production.AUCSIM_ppa_auction_wlt_ac ac
      inner join intent_media_sandbox_production.AUCSIM_ppa_auction_wlt_i1 i
    on ac.request_id = i.request_id;

 /* Table 2b - Advertiser Id 2 from Impressions */   
drop table if exists intent_media_sandbox_production.AUCSIM_ppa_auction_wlt_i2;
create table
 intent_media_sandbox_production.AUCSIM_ppa_auction_wlt_i2 as
 select request_id,
        advertiser_id as advertiser_id2,
        hotel_average_nightly_rate as advertiser_hotel_price
        from intent_media_log_data_production.impressions
        where requested_at_date_in_et between '2014-10-01' and '2014-10-31'
    and ip_address_blacklisted = false;

/* Table 3b - Ad Calls with Advertiser Id 2 from Impressions */   
drop table if exists intent_media_sandbox_production.AUCSIM_ppa_auction_wlt_ac_i2;
create table intent_media_sandbox_production.AUCSIM_ppa_auction_wlt_ac_i2 as
    select
     ac.request_id,
     ac.site_id,
     i.advertiser_id2,
     ac.publisher_hotel_price,
     i.advertiser_hotel_price,
     case when TRUNC(advertiser_hotel_price) < TRUNC(publisher_hotel_price) then 1
          when TRUNC(advertiser_hotel_price) > TRUNC(publisher_hotel_price) then -1
          else 0 end as displayed_price_adv_win
     from intent_media_sandbox_production.AUCSIM_ppa_auction_wlt_ac ac
      inner join intent_media_sandbox_production.AUCSIM_ppa_auction_wlt_i2 i
    on ac.request_id = i.request_id;

/* Table 4 - Two lists */   
drop table if exists intent_media_sandbox_production.AUCSIM_ppa_auction_wlt_s;
create table intent_media_sandbox_production.AUCSIM_ppa_auction_wlt_s as
select
one.site_id,
one.advertiser_id1,
two.advertiser_id2,
one.displayed_price_adv_win as adv_one,
two.displayed_price_adv_win as adv_two
from
intent_media_sandbox_production.AUCSIM_ppa_auction_wlt_ac_i1 one
inner join
intent_media_sandbox_production.AUCSIM_ppa_auction_wlt_ac_i2 two
on 
one.request_id = two.request_id;

select site_id,
       advertiser_id1,
       advertiser_id2,
       CORR(adv_one, adv_two) 
from intent_media_sandbox_production.AUCSIM_ppa_auction_wlt_s
group by 1,2,3;