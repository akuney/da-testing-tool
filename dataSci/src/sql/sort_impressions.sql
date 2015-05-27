select
request_id,
advertiser_id,
quality_score,
jittered_quality_score,
effective_bid,
actual_cpc,
auction_position
from
intent_media_log_data_development.impressions
order by 1,4
;

select
o.request_id,
o.advertiser_id,
o.quality_score,
o.jittered_quality_score,
o.effective_bid,
o.actual_cpc,
o.auction_position,
n.request_id,
n.advertiser_id,
n.quality_score,
n.jittered_quality_score,
n.effective_bid,
n.actual_cpc,
n.auction_position
from
intent_media_sandbox_development.SP_impressions_new n
join
intent_media_sandbox_development.SP_impressions_old o
on n.external_id = o.external_id;
;