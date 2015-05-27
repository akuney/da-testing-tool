select * from intent_media_sandbox_production.SP_PPA_auction_vpc_ac_c where clicked_conversion_count > 0 limit 20;

select * from intent_media_sandbox_production.SP_PPA_auction_vpc_ac_c limit 20;

select *
from intent_media_log_data_production.impressions
where requested_at_date_in_et = '2014-07-01' 
	AND ip_address_blacklisted = false
	AND ad_unit_id in (129, 143, 89, 116)
	limit 100;
	

select *
from intent_media_log_data_production.ad_calls
where requested_at_date_in_et = '2014-07-01' 
	AND ip_address_blacklisted = false
	AND ad_unit_type = 'META'
	limit 100;
	

select 
price_parity,
count(distinct(request_id))
from intent_media_sandbox_production.SP_PPA_auction_vpc_i
group by 1;

select
market_id,
ad_call_count,
RANK() over (order by ad_call_count desc) as rank
from 
  (select 
   market_id,
   count(*) as ad_call_count
   from intent_media_log_data_production.ad_calls
   where requested_at_date_in_et = '2014-07-01' 
	and ip_address_blacklisted = false
	and ad_unit_type = 'META'
	and market_id is not null
   group by 1) t1
order by rank asc
;

select * from intent_media_production.intent_media_markets where id=5449368;


select 
avg(net_conversion_value),
stddev(net_conversion_value)
from
intent_media_sandbox_production.SP_PPA_auction_vpc_con
where entity_id in (45,55,85);

select 
decile,
avg(clicked_conversion_value),
stddev(clicked_conversion_value)
from
intent_media_sandbox_production.SP_PPA_auction_vpc_ac_c
where clicked_conversion_value>0
group by decile;


select 
decile_by_property_search_volume,
avg(clicked_conversion_value),
stddev(clicked_conversion_value)
from
intent_media_sandbox_production.SP_PPA_auction_vpc_ac_c
where clicked_conversion_value>0
group by decile_by_property_search_volume;
        
select    
brand_id,
count(*) as hotel_count
from
intent_media_production.hotel_properties
group by 1
order by 2 desc
;

select name from intent_media_production.hotel_properties where brand_id = 914361 limit 20;

copy 
intent_media_sandbox_production.SP_hotel_star_ratings
from
local '/Users/saurav.pandit/Desktop/Datafiles/star_rating.csv'
delimiter ','
;