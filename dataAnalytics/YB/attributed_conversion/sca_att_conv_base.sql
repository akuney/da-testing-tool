/* Load Ad calls and Impressions */
drop table if exists intent_media_sandbox_production.YB_SCA_ADV_ATT_CONV_ACI;
create table intent_media_sandbox_production.YB_SCA_ADV_ATT_CONV_ACI as
select 
  ac.request_id as ad_call_request_id, 
  ac.webuser_id as ad_call_webuser_id, 
  ac.requested_at_in_et as ad_call_timestamp, 
  i.external_id
from intent_media_log_data_production.ad_calls ac
inner join intent_media_log_data_production.impressions i on ac.request_id = i.request_id
where ac.requested_at_date_in_et = date(current_timestamp at timezone 'EST') - interval '31 days'
  and i.requested_at_date_in_et = date(current_timestamp at timezone 'EST') - interval '31 days'
  and ac.ip_address_blacklisted = 0
  and i.ip_address_blacklisted = 0
  and ac.outcome_type = 'SERVED'
  and ac.ad_unit_type = 'CT';
select analyze_histogram('intent_media_sandbox_production.YB_SCA_ADV_ATT_CONV_ACI',100);

/* Load Clicks and Impressions */
drop table if exists intent_media_sandbox_production.YB_SCA_ADV_ATT_CONV_C;
create table intent_media_sandbox_production.YB_SCA_ADV_ATT_CONV_C as
select
  i.advertiser_id,
  c.external_impression_id, 
  c.request_id as click_request_id, 
  c.requested_at_in_et as click_timestamp, 
  c.webuser_id as click_webuser_id, 
  rank() over (partition by c.external_impression_id order by c.requested_at_in_et) as single_click_rank
from intent_media_log_data_production.clicks c
inner join intent_media_log_data_production.impressions i
on c.external_impression_id = i.external_id
where c.requested_at_date_in_et >= date(current_timestamp at timezone 'EST') - interval '31 days'
  and c.requested_at_date_in_et <= date(current_timestamp at timezone 'EST') - interval '30 days'
  and i.requested_at_date_in_et = date(current_timestamp at timezone 'EST') - interval '31 days'
  and c.ip_address_blacklisted = 0
  and i.ip_address_blacklisted = 0
  and c.fraudulent = 0;
select analyze_histogram('intent_media_sandbox_production.YB_SCA_ADV_ATT_CONV_C',100);

/* Base Table by joining Ad Calls, Impressions and Clicks */
drop table if exists intent_media_sandbox_production.YB_SCA_ADV_ATT_CONV_AIC;
create table intent_media_sandbox_production.YB_SCA_ADV_ATT_CONV_AIC as
select
  aci.ad_call_request_id,
  aci.external_id,
  aci.ad_call_webuser_id,
  aci.ad_call_timestamp,
  c.click_request_id,
  c.click_timestamp,
  c.click_webuser_id,
  c.single_click_rank
from intent_media_sandbox_production.YB_SCA_ADV_ATT_CONV_ACI aci
left join intent_media_sandbox_production.YB_SCA_ADV_ATT_CONV_C c
on aci.external_id = c.external_impression_id;
select analyze_histogram('intent_media_sandbox_production.YB_SCA_ADV_ATT_CONV_AIC',100);

/* Load deduped conversions */
drop table if exists intent_media_sandbox_production.YB_SCA_ADV_ATT_CONV_CON;
create table intent_media_sandbox_production.YB_SCA_ADV_ATT_CONV_CON as
select
  con.entity_id,
  con.webuser_id as conversion_webuser_id,
  con.request_id as conversion_request_id,
  con.requested_at_in_et as conversion_timestamp
from
(
  select
    entity_id,
    order_id,
    min(requested_at_in_et) as min_timestamp
  from intent_media_log_data_production.conversions
  where requested_at_date_in_et >= date(current_timestamp at timezone 'EST') - interval '30 days'
    and requested_at_date_in_et <= date(current_timestamp at timezone 'EST')
    and ip_address_blacklisted = 0
    and order_id is not null
  group by
    entity_id,
    order_id
) distinct_con
inner join intent_media_log_data_production.conversions con
on distinct_con.entity_id = con.entity_id
and distinct_con.order_id = con.order_id
and distinct_con.min_timestamp = con.requested_at_in_et
union
select
  entity_id,
  webuser_id as conversion_webuser_id,
  request_id as conversion_request_id,
  requested_at_date_in_et as conversion_timestamp
from intent_media_log_data_production.conversions
where requested_at_date_in_et >= date(current_timestamp at timezone 'EST') - interval '30 days'
  and requested_at_date_in_et <= date(current_timestamp at timezone 'EST')
  and ip_address_blacklisted = 0
  and order_id is null;
select analyze_histogram('intent_media_sandbox_production.YB_SCA_ADV_ATT_CONV_CON',100);

/* Attribute Conversion to Click */
drop table if exists intent_media_sandbox_production.YB_SCA_ADV_ATT_CONV_CCON;
create table intent_media_sandbox_production.YB_SCA_ADV_ATT_CONV_CCON as
select *
from
(
  select
    con.*,
    c.click_request_id,
    rank() over (partition by con.conversion_request_id order by c.click_timestamp desc) as click_rank
  from intent_media_sandbox_production.YB_SCA_ADV_ATT_CONV_CON con
  cross join intent_media_sandbox_production.YB_SCA_ADV_ATT_CONV_C c
  where c.single_click_rank = 1
    and c.click_webuser_id = con.conversion_webuser_id
    and c.advertiser_id = con.entity_id
    and c.click_timestamp < con.conversion_timestamp
    and (c.click_timestamp + interval '30 day') >= con.conversion_timestamp
) ccon
where click_rank = 1;
select analyze_histogram('intent_media_sandbox_production.YB_SCA_ADV_ATT_CONV_CCON',100);

/* left join back to base table */
drop table if exists intent_media_sandbox_production.YB_SCA_ADV_ATT_CONV_Base;
create table intent_media_sandbox_production.YB_SCA_ADV_ATT_CONV_Base as
select
  aic.ad_call_request_id,
  aic.external_id,
  aic.click_request_id,
  ccon.conversion_request_id
from intent_media_sandbox_production.YB_SCA_ADV_ATT_CONV_AIC aic
left join intent_media_sandbox_production.YB_SCA_ADV_ATT_CONV_CCON ccon
on aic.click_request_id = ccon.click_request_id;
select analyze_histogram('intent_media_sandbox_production.YB_SCA_ADV_ATT_CONV_Base',100);