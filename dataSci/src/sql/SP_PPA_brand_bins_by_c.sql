drop table if exists intent_media_sandbox_production.SP_PPA_auction_brand_c_count;
create table intent_media_sandbox_production.SP_PPA_auction_brand_c_count as
select
brand_id,
click_count,
RANK() over (order by click_count desc) as rank
from
        (select 
        brand_id,
        sum(click_count) as click_count
        from 
                (select 
                ac.*,
                case when c.ad_call_request_id is null then 0 else 1 end as click_count               
                from
                        (select
                        a.*,
                        b.brand_id as brand_id
                        from
                        (select * 
                        from
                        intent_media_log_data_production.ad_calls  
                        where requested_at_date_in_et between '2014-07-01' and '2014-07-31'
                        and ip_address_blacklisted = false
	                and site_type in ('CHEAPTICKETS', 'EBOOKERS',  'ORBITZ_GLOBAL')
                        and ad_unit_type = 'META'
                        and outcome_type = 'SERVED'
                        and hotel_property_id is not null) a
                        left join
                        (select
                        id,
                        brand_id
                        from
                        intent_media_production.hotel_properties
                        where id != 914361) b
                        on a.hotel_property_id = b.id
                        ) ac
                        left join
                        (select *
                        from
                        intent_media_log_data_production.clicks
                        where requested_at_date_in_et between '2014-07-01' and (date('2014-07-31') + interval '1 day')
                        and ip_address_blacklisted = 0
                        and fraudulent = 0
                        and site_type in ('CHEAPTICKETS', 'EBOOKERS',  'ORBITZ_GLOBAL')) c 
                        on 
                        ac.request_id = c.ad_call_request_id             
                ) joined_ac_c
          group by 1) click_count_by_brand     
order by rank asc            
;

drop table if exists intent_media_sandbox_production.SP_PPA_auction_brand_c_bins;
create table intent_media_sandbox_production.SP_PPA_auction_brand_c_bins as
select 
brand_id,
rank,
-- Simply looked up the total number of ad calls 
-- i.e. 50,250. Hacky way, can be done via SQL!
cast(ceiling(100*cumul_click_count/50250) as int) as percentile,
cast(ceiling(10*cumul_click_count/50250) as int) as decile,
cast(ceiling(4*cumul_click_count/50250) as int) as quartile
from
  (select 
        t1.brand_id as brand_id,
        max(t1.click_count) as click_count,
        sum(t2.click_count) as cumul_click_count,
        max(t1.rank) as rank
        from 
        intent_media_sandbox_production.SP_PPA_auction_brand_c_count t1
        inner join 
        intent_media_sandbox_production.SP_PPA_auction_brand_c_count t2
        on t2.rank <= t1.rank
        group by t1.brand_id
        order by rank desc) t
 order by rank asc
;


