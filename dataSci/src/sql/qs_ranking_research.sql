select 
request_id, 
advertiser_id,
quality_score,
effective_bid,
auction_position
from 
intent_media_log_data_production.impressions
where 
requested_at_date_in_et > '2014-07-15'
and ad_unit_id = 32
order by 1,2
;

select
advertiser_id,
auction_position,
count(*)
from 
intent_media_sandbox_development.SP_impressions_old_1k
group by 1,2
order by 1,2
;

select
advertiser_id,
avg(auction_position) as avg_rank,
stddev(auction_position) as stddev_rank,
count(*) as freq
from 
intent_media_sandbox_development.SP_impressions_new_5kT
group by 1
order by 1
;

select
id, name