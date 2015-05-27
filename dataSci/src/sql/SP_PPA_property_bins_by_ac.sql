drop table if exists intent_media_sandbox_production.SP_PPA_auction_property_ac_count;
create table intent_media_sandbox_production.SP_PPA_auction_property_ac_count as
select
hotel_property_id,
ad_call_count,
RANK() over (order by ad_call_count desc) as rank
from
    (select 
        hotel_property_id,
        count(*) as ad_call_count
        from intent_media_log_data_production.ad_calls
        where requested_at_date_in_et between '2014-07-01' and '2014-07-31'
            and ip_address_blacklisted = false
	    and site_type in ('CHEAPTICKETS', 'EBOOKERS',  'ORBITZ_GLOBAL')
            and ad_unit_type = 'META'
            and outcome_type = 'SERVED'
            and hotel_property_id is not null
        group by 1) t
order by rank asc            
;

drop table if exists intent_media_sandbox_production.SP_PPA_auction_property_ac_bins;
create table intent_media_sandbox_production.SP_PPA_auction_property_ac_bins as
select 
hotel_property_id,
rank,
-- Simply looked up the total number of ad calls 
-- i.e. 87,709,503. Hacky way, can be done via SQL!
cast(ceiling(100*cumul_ad_call_count/87709503) as int) as percentile,
cast(ceiling(10*cumul_ad_call_count/87709503) as int) as decile,
cast(ceiling(4*cumul_ad_call_count/87709503) as int) as quartile
from
  (select 
        t1.hotel_property_id as hotel_property_id,
        max(t1.ad_call_count) as ad_call_count,
        sum(t2.ad_call_count) as cumul_ad_call_count,
        max(t1.rank) as rank
        from 
        intent_media_sandbox_production.SP_PPA_auction_property_ac_count t1
        inner join 
        intent_media_sandbox_production.SP_PPA_auction_property_ac_count t2
        on t2.rank <= t1.rank
        group by t1.hotel_property_id
        order by rank desc) t
 order by rank asc
;


