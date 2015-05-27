drop table if exists intent_media_sandbox_production.SP_PPA_auction_market_ac_count;
create table intent_media_sandbox_production.SP_PPA_auction_market_ac_count as
select
market_id,
ad_call_count,
RANK() over (order by ad_call_count desc) as rank
from
    (select 
        market_id,
        count(*) as ad_call_count
        from intent_media_log_data_production.ad_calls
        where requested_at_date_in_et between '2014-07-01' and '2014-07-31'
            and ip_address_blacklisted = false
	    and site_type in ('CHEAPTICKETS', 'EBOOKERS',  'ORBITZ_GLOBAL')
            and ad_unit_type = 'META'
            and outcome_type = 'SERVED'
            and market_id is not null
        group by 1) t
order by rank asc            
;

drop table if exists intent_media_sandbox_production.SP_PPA_auction_market_ac_bins;
create table intent_media_sandbox_production.SP_PPA_auction_market_ac_bins as
select 
market_id,
rank,
-- Simply looked up the total number of ad calls 
-- i.e. 84,742,855. Hacky way, can be done via SQL!
cast(ceiling(100*cumul_ad_call_count/84742855) as int) as percentile,
cast(ceiling(10*cumul_ad_call_count/84742855) as int) as decile,
cast(ceiling(4*cumul_ad_call_count/84742855) as int) as quartile
from
  (select 
        t1.market_id as market_id,
        max(t1.ad_call_count) as ad_call_count,
        sum(t2.ad_call_count) as cumul_ad_call_count,
        max(t1.rank) as rank
        from 
        intent_media_sandbox_production.SP_PPA_auction_market_ac_count t1
        inner join 
        intent_media_sandbox_production.SP_PPA_auction_market_ac_count t2
        on t2.rank <= t1.rank
        group by t1.market_id
        order by rank desc) t
 order by rank asc
;


drop table if exists intent_media_sandbox_production.SP_PPA_auction_market_ac_bins_details;
create table intent_media_sandbox_production.SP_PPA_auction_market_ac_bins_details as
select 
m.name as market_name,
m.state as state,
m.country as country,
r.*
from
intent_media_sandbox_production.SP_PPA_auction_market_ac_bins r
inner join 
        (select 
        pm.market_id as market_id,
        im.name as name,
        im.state as state,
        im.country as country
        from
        intent_media_production.intent_media_markets im 
        inner join 
        intent_media_production.intent_media_markets_publisher_markets pm
        on
        pm.intent_media_market_id = im.id) m
on m.market_id = r.market_id
order by r.rank asc
;