/* Table 1 - Ad Calls */
drop table if exists intent_media_sandbox_production.AUCSIM_ppa_auction_prob_ac;
create table intent_media_sandbox_production.AUCSIM_ppa_auction_prob_ac as
    select 
        site_id,
        request_id,
        publisher_hotel_price
        from intent_media_log_data_production.ad_calls
        where requested_at_date_in_et between '2014-10-01' and '2014-10-31'
            and ip_address_blacklisted = false
            and ad_unit_type = 'META'
            and outcome_type = 'SERVED'
            and hotel_property_id is not null;
    


 /* Table 2 - Advertiser Ids from Impressions */   
drop table if exists intent_media_sandbox_production.AUCSIM_ppa_auction_prob_i;
create table intent_media_sandbox_production.AUCSIM_ppa_auction_prob_i as
select request_id,
       advertiser_id,
       hotel_average_nightly_rate as advertiser_hotel_price
from intent_media_log_data_production.impressions
where requested_at_date_in_et between '2014-10-01' and '2014-10-31'
  AND ip_address_blacklisted = false
;


/* Table 3 - Ad Calls with Advertiser Id from Impressions */   
drop table if exists intent_media_sandbox_production.AUCSIM_ppa_auction_prob_ac_i;
create table intent_media_sandbox_production.AUCSIM_ppa_auction_prob_ac_i as
    select
     ac.*,
     i.advertiser_id,
     case when TRUNC(advertiser_hotel_price) < TRUNC(publisher_hotel_price) then 1
           else 0 end as adv_win,
     case when TRUNC(advertiser_hotel_price) > TRUNC(publisher_hotel_price)  then 1
           else 0 end as adv_loss,
     case when TRUNC(advertiser_hotel_price) = TRUNC(publisher_hotel_price) then 1
           else 0 end as adv_tie
    from intent_media_sandbox_production.AUCSIM_ppa_auction_prob_ac ac
      left join intent_media_sandbox_production.AUCSIM_ppa_auction_prob_i i
    on ac.request_id = i.request_id
;

 /* Aggregate */
select
  site_id,
  advertiser_id,
  sum(adv_win)/count(*) as adv_win_percent,
  sum(adv_tie)/count(*) as adv_tie_percent,
  sum(adv_loss)/count(*) as adv_loss_percent
from intent_media_sandbox_production.AUCSIM_ppa_auction_prob_ac_i
where advertiser_id is not null
group by
  site_id,
  advertiser_id;