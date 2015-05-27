select
ac.predict_mode_type,
-- jittered_quality_score
count(*) as zero_quality_score,
sum(case jittered_quality_score when 0 then 1 else 0 end) as zero_jittered_score
from
(select
request_id  ,
predict_mode_type
from 
intent_media_log_data_production.ad_calls
where
requested_at_date_in_et = '2014-06-11'
and product_category_type = 'FLIGHTS') ac
inner join
(select
request_id as ad_call_request_id,
jittered_quality_score
from
intent_media_log_data_production.impressions
where
requested_at_date_in_et = '2014-06-11'
and quality_score = 0.0) zi
on
ac.request_id = zi.ad_call_request_id
group by 1
;

select
quality_score,
jittered_quality_score
from
intent_media_log_data_production.impressions
where
requested_at_date_in_et = '2014-06-11'
limit 200;