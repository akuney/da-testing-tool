-- pull a day of ad_calls into a separate table. only pull necessary fields.
drop table if exists intent_media_sandbox_production.SJ_ad_calls;
create table intent_media_sandbox_production.SJ_ad_calls as
select
        request_id,
        ad_unit_id,
        outcome_type,
        requested_at_date_in_et
from intent_media_log_data_production.ad_calls
where requested_at_date_in_et = '2013-12-21'
        and ip_address_blacklisted = 0
        and site_type = 'HOTWIRE';

-- pull two days of clicks into a separate table. you need two days because a click 
-- can be attributed to an ad_call for up to 24 hours. only pull necessary fields.
drop table if exists intent_media_sandbox_production.SJ_clicks;
create table intent_media_sandbox_production.SJ_clicks as
select
        ad_call_request_id,
        actual_cpc      
from intent_media_log_data_production.clicks
where ip_address_blacklisted = 0
        and fraudulent = 0
        and requested_at_date_in_et in ('2013-12-21','2013-12-22');
        
-- create a table of just ad_calls that were served
drop table if exists intent_media_sandbox_production.SJ_ad_calls_served;
create table intent_media_sandbox_production.SJ_ad_calls_served as
select
        request_id,
        ad_unit_id,
        requested_at_date_in_et
from intent_media_sandbox_production.SJ_ad_calls
where outcome_type = 'SERVED';

-- join ad_calls and clicks to get full funnel (metrics from pages available to spend)
select
        total_ad_calls.requested_at_date_in_et as Date,
        total_ad_calls.ad_unit_id as "Ad Unit ID",
        total_ad_calls.ad_calls as "Pages Available",
        total_ad_calls.ad_calls_served as "Pages Served",
        performance.interactions as Interactions,
        performance.clicks as Clicks,
        performance.sum_actual_cpc as Spend
from
        (select
                requested_at_date_in_et,
                ad_unit_id,
                count(1) as ad_calls,
                sum(case when outcome_type = 'SERVED' then 1 else 0 end) as ad_calls_served
        from intent_media_sandbox_production.SJ_ad_calls
        group by 
              ad_unit_id,
              requested_at_date_in_et) total_ad_calls
left join
        (select
                requested_at_date_in_et,
                ad_unit_id,
                count(ad_call_request_id) as clicks,
                count(distinct(ad_call_request_id)) as interactions,
                round(sum(actual_cpc),2) as sum_actual_cpc
        from intent_media_sandbox_production.SJ_ad_calls_served ad_calls
        left join intent_media_sandbox_production.SJ_clicks clicks on ad_calls.request_id = clicks.ad_call_request_id
        group by 
              ad_unit_id,
              requested_at_date_in_et) performance
on total_ad_calls.ad_unit_id = performance.ad_unit_id
   and total_ad_calls.requested_at_date_in_et = performance.requested_at_date_in_et;
