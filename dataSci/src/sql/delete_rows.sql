delete from intent_media_log_data_development.impressions;


drop table if exists intent_media_sandbox_development.SP_impressions_new_5kT;
create table intent_media_sandbox_development.SP_impressions_new_5kT as
select 
request_id,
requested_at_in_et,
advertiser_id,
ad_unit_id,
quality_score,
jittered_quality_score,
effective_bid,
actual_cpc,
auction_position 
from intent_media_log_data_development.impressions; 


drop table if exists intent_media_sandbox_development.SP_impressions_old_5kT;
create table intent_media_sandbox_development.SP_impressions_old_5kT as
select
request_id,
requested_at_in_et,
advertiser_id,
ad_unit_id,
quality_score,
jittered_quality_score,
effective_bid,
actual_cpc,
auction_position  
from intent_media_log_data_development.impressions; 

