/* Copy Ad Schedules Table from MySQL */
drop table if exists intent_media_sandbox_production.YB_ad_schedules;
create table intent_media_sandbox_production.YB_ad_schedules
(
      id int,
      bid_override decimal,
      start_date date,
      end_date date
);
copy intent_media_sandbox_production.YB_ad_schedules
(
      id,
      bid_override,
      start_date,
      end_date
)
from local '/Users/yoojong.bang/Desktop/ad_schedules.csv'
with delimiter ',' null as 'NULL';

/* Copy Advertisements Table from MySQL */
drop table if exists intent_media_sandbox_production.YB_advertisements;
create table intent_media_sandbox_production.YB_advertisements
(
      id int,
      created_at datetime,
      updated_at datetime,
      ad_group_id int,
      paused tinyint,
      review_status_type varchar,
      deleted tinyint,
      advertisement_type varchar,
      optimized tinyint,
      creative_id int,
      ad_copy_id int,
      ssn_ad_copy_id int,
      publishers_hotel_properties_image_id int,
      ad_schedule_id int,
      language_id int
);
copy intent_media_sandbox_production.YB_advertisements
(
      id,
      created_at,
      updated_at,
      ad_group_id,
      paused,
      review_status_type,
      deleted,
      advertisement_type,
      optimized,
      creative_id,
      ad_copy_id,
      ssn_ad_copy_id,
      publishers_hotel_properties_image_id,
      ad_schedule_id,
      language_id
)
from local '/Users/yoojong.bang/Desktop/advertisements.csv'
with delimiter ',' null as 'NULL';

/* Number of impressions for scheduled ads in August */
drop table if exists intent_media_sandbox_production.YB_sa_ssr_imps_0801_0930;
create table intent_media_sandbox_production.YB_sa_ssr_imps_0801_0930 as
select
  i.advertiser_id,
  min(e.name) as advertiser_name,
  min(adv.id) as advertisement_id,
  min(adv.ad_schedule_id) as ad_schedule_id,
  count(i.request_id) as no_of_impressions
from intent_media_log_data_production.impressions i
inner join intent_media_production.entities e on e.id = i.advertiser_id
inner join intent_media_sandbox_production.YB_advertisements adv on i.advertisement_id = adv.id
inner join intent_media_production.ad_units au on i.ad_unit_id = au.id
where au.product_category_type = 'HOTELS' /* publisher product category */
  and au.ad_type = 'SSR' /* publisher ad unit type */
  and au.active = 1
  and i.requested_at_date_in_et >= '2014-08-01'
  and i.requested_at_date_in_et <= '2014-09-30'
  and i.ip_address_blacklisted = 0
  and adv.ad_schedule_id is not null /* non-NULL ad schedule */
group by
  i.advertiser_id;

/* Verification */
-- 60,336,557
select count(1)
from intent_media_log_data_production.impressions i
inner join intent_media_log_data_production.ad_calls ac on ac.request_id = i.request_id
where i.requested_at_date_in_et >= '2014-08-01'
  and i.requested_at_date_in_et <= '2014-09-30'
  and i.ip_address_blacklisted = 0
  and ac.requested_at_date_in_et >= '2014-08-01'
  and ac.requested_at_date_in_et <= '2014-09-30'
  and ac.ip_address_blacklisted = 0
  and ac.product_category_type = 'HOTELS'
  and ac.ad_unit_type = 'SSR'
  and ac.outcome_type = 'SERVED'
  and ac.positions_filled > 0;

-- 60,308,538
select count(1)
from intent_media_log_data_production.impressions i
where i.requested_at_date_in_et >= '2014-08-01'
  and i.requested_at_date_in_et <= '2014-09-30'
  and i.ip_address_blacklisted = 0
  and i.request_id in
    (
    select request_id
    from  intent_media_log_data_production.ad_calls
    where requested_at_date_in_et >= '2014-08-01'
      and requested_at_date_in_et <= '2014-09-30'
      and ip_address_blacklisted = 0
      and product_category_type = 'HOTELS'
      and ad_unit_type = 'SSR'
      and outcome_type = 'SERVED'
      and positions_filled > 0
    );

-- 60,323,416
select count(1)
from intent_media_log_data_production.impressions i
inner join intent_media_production.ad_units au on i.ad_unit_id = au.id
where i.requested_at_date_in_et >= '2014-08-01'
  and i.requested_at_date_in_et <= '2014-09-30'
  and i.ip_address_blacklisted = 0
  and au.product_category_type = 'HOTELS'
  and au.ad_type = 'SSR'
  and au.active = 1;

/*
  Number of clicks for scheduled ads in Aug-Sep
  (including scheduled ads that were initiated in a previous month that run for a few days in Aug)
*/
drop table if exists intent_media_sandbox_production.YB_sa_ssr_clicks_0801_0930;
create table intent_media_sandbox_production.YB_sa_ssr_clicks_0801_0930 as
select
  i.advertiser_id,
  min(e.name) as advertiser_name,
  adv.id as advertisement_id,
  adv.ad_group_id,
  adv.ad_schedule_id,
  count(c.request_id) as clicks,
  sum(c.actual_cpc) as sum_actual_cpc
from intent_media_log_data_production.clicks c
inner join intent_media_log_data_production.impressions i on i.external_id = c.external_impression_id
inner join intent_media_production.entities e on e.id = i.advertiser_id
inner join intent_media_sandbox_production.YB_advertisements adv on c.advertisement_id = adv.id
inner join intent_media_production.ad_units au on i.ad_unit_id = au.id
where au.product_category_type = 'HOTELS'
  and au.ad_type = 'SSR'
  and au.active = 1
  and i.requested_at_date_in_et >= '2014-08-01'
  and i.requested_at_date_in_et <= '2014-09-30'
  and i.ip_address_blacklisted = 0
  and adv.ad_schedule_id is not NULL
  and c.requested_at_date_in_et >= '2014-08-01'
  and c.requested_at_date_in_et <= '2014-10-01'
  and c.ip_address_blacklisted = 0
  and c.fraudulent = 0
  and i.requested_at <= c.requested_at
  and i.requested_at + interval '24 hours' >= c.requested_at
group by
  i.advertiser_id,
  adv.id,
  adv.ad_group_id,
  adv.ad_schedule_id;

-- no of clicks for scheduled ads in Aug (these include scheduled ads that started and ended in August)
drop table if exists intent_media_sandbox_production.PS_sa_ssr_clicks_Aug1toAug31_strict;
create table intent_media_sandbox_production.PS_sa_ssr_clicks_Aug1toAug31_strict as
select min(a.id) as id, min(a.ad_group_id) as ad_group_id, min(a.ad_schedule_id) as ad_schedule_id, min(c.actual_cpc) as actual_cpc, count(c.external_impression_id) as no_of_clicks, i.advertiser_id, min(e.name) as advertiser_name
from intent_media_log_data_production.clicks c
join intent_media_log_data_production.impressions i on i.external_id = c.external_impression_id
join intent_media_sandbox_production.ads a on c.advertisement_id = a.id
join intent_media_production.ad_units au on i.ad_unit_id = au.id
join intent_media_production.entities e on e.id = i.advertiser_id
where au.product_category_type = 'HOTELS'
and au.ad_type = 'SSR'
and i.requested_at_date_in_et >= '2014-08-01'
and i.requested_at_date_in_et <= '2014-08-31'
and i.ip_address_blacklisted = 0
and c.requested_at_date_in_et >= '2014-08-01'
and c.requested_at_date_in_et <= '2014-09-01'
and c.fraudulent = 0
and i.requested_at < c.requested_at
and a.ad_schedule_id in

(
select id
from intent_media_sandbox_production.ad_schedules
where start_date >= '2014-08-01'
and end_date <= '2014-08-31'

)
group by i.advertiser_id

-- total revenue from scheduled ads in august
-- 87605.94000000
-- 1506.47000000

-- My old version using ad_units
select sum(c.actual_cpc)
from intent_media_log_data_production.clicks c
join intent_media_log_data_production.impressions i on i.external_id = c.external_impression_id
join intent_media_sandbox_production.ads a on c.advertisement_id = a.id
join intent_media_production.ad_units au on i.ad_unit_id = au.id
join intent_media_production.entities e on e.id = i.advertiser_id
where c.product_category_type = 'HOTELS'
and au.ad_type = 'SSR'
and i.requested_at_date_in_et >= '2014-08-01'
and i.requested_at_date_in_et <= '2014-08-31'
and i.ip_address_blacklisted = 0
and a.ad_schedule_id is not NULL
and c.requested_at_date_in_et >= '2014-08-01'
and c.requested_at_date_in_et <= '2014-09-01'
and c.fraudulent = 0
and i.requested_at < c.requested_at

-- Yoojongs version using ad_calls
-- 2897.42000000
select sum(c.actual_cpc)
from intent_media_log_data_production.clicks c
join intent_media_log_data_production.ad_calls ac on ac.request_id = c.ad_call_request_id
join intent_media_sandbox_production.ads a on c.advertisement_id = a.id
where c.product_category_type = 'HOTELS'
and ac.product_category_type = 'HOTELS'
and ac.ad_unit_type = 'SSR'
and ac.requested_at_date_in_et >= '2014-08-01'
and ac.requested_at_date_in_et <= '2014-08-31'
and ac.ip_address_blacklisted = 0
and a.ad_schedule_id is not NULL
and c.requested_at_date_in_et >= '2014-08-01'
and c.requested_at_date_in_et <= '2014-09-01'
and c.fraudulent = 0
and ac.requested_at < c.requested_at


-- compute revenue by summing cpc for scheduled ads in August
-- 189.10000000
select sum(c.actual_cpc)
from intent_media_log_data_production.clicks c
--join intent_media_log_data_production.ad_calls ac on ac.request_id = c.ad_call_request_id
--join intent_media_sandbox_production.ads a on c.advertisement_id = a.id
--c.product_category_type = 'HOTELS'
--and ac.product_category_type = 'HOTELS'
/* ac.ad_unit_type = 'SSR'
and ac.requested_at_date_in_et >= '2014-08-01'
and ac.requested_at_date_in_et <= '2014-08-31'
and ac.ip_address_blacklisted = 0 */
--and a.ad_schedule_id is not NULL
where c.requested_at_date_in_et >= '2014-08-01'
and c.requested_at_date_in_et <= '2014-09-01'
and c.fraudulent = 0
--and ac.requested_at < c.requested_at
and c.advertisement_id in
                       (
                                select id
                                from intent_media_sandbox_production.ads
                                where ad_schedule_id is not NULL
                                and ad_schedule_id in

                                                        (
                                                                select id
                                                                from intent_media_sandbox_production.ad_schedules
                                                                where start_date >= '2014-08-01'
                                                                and end_date <= '2014-08-31'
                                                        )
                        )

-- 302411.23000000
select sum(actual_cpc_sum)
from intent_media_production.advertiser_ad_report_aggregations
where date(created_at) >= '2014-08-01'
and date(updated_at) <= '2014-09-01'

select count(distinct ssn_ad_copy_id)
from intent_media_sandbox_production.ads

select id
from intent_media_sandbox_production.ad_schedules
where start_date >= '2014-08-01'
and end_date <= '2014-08-31'

select sum(actual_cpc * no_of_clicks)
from intent_media_sandbox_production.PS_sa_ssr_clicks_Aug1toAug31




-- compute no of clicks and group by cpc for scheduled ads in august

-- select c.actual_cpc, count(c.external_impression_id) as no_of_clicks, min(i.advertiser_id) as advertiser_id, min(e.name) as advertiser_name
select c.actual_cpc, count(c.request_id) as no_of_clicks
from intent_media_log_data_production.clicks c
--join intent_media_log_data_production.ad_calls ac on ac.request_id = c.ad_call_request_id
--join intent_media_production.entities e on e.id = i.advertiser_id
where --ac.product_category_type = 'HOTELS'
--and c.product_category_type = 'HOTELS'
--ac.ad_unit_type = 'SSR'
/%and ac.requested_at_date_in_et >= '2014-08-01'
and ac.requested_at_date_in_et <= '2014-08-31'
and ac.ip_address_blacklisted = 0%/
and c.requested_at_date_in_et >= '2014-08-01'
and c.requested_at_date_in_et <= '2014-09-01'
and c.fraudulent = 0
and c.ip_address_blacklisted = 0
--and ac.requested_at < c.requested_at
and c.advertisement_id in
                       (
                                select id
                                from intent_media_sandbox_production.ads
                                where ad_schedule_id is not NULL
                                and ad_schedule_id in

                                                        (
                                                                select id
                                                                from intent_media_sandbox_production.ad_schedules
                                                                where start_date >= '2014-08-01'
                                                                and end_date <= '2014-08-31'
                                                        )
                        )
group by c.actual_cpc

-- verifying sub query in the above sql
select  count(distinct ad_schedule_id)
from intent_media_sandbox_production.ads a
where a.ad_schedule_id is not null
and a.ad_schedule_id in

                                                        (
                                                                select id
                                                                from intent_media_sandbox_production.ad_schedules
                                                                where start_date >= '2014-08-01'
                                                                and end_date <= '2014-08-31'
                                                        )

-- trying to verify from aggregations

select sum(actual_cpc_sum)
from intent_media_production.advertiser_ad_report_aggregations
where date(aggregation_level) >= '2014-08-01'
and date(aggregation_level) <= '2014-08-31'
and ssn_ad_copy_id in

                        (

                        select a.ssn_ad_copy_id
                        from intent_media_sandbox_production.ads a
                        where a.ad_schedule_id is not null
                        and a.ad_schedule_id in

                                                        (
                                                                select id
                                                                from intent_media_sandbox_production.ad_schedules
                                                                where start_date >= '2014-08-01'
                                                                and end_date <= '2014-08-31'
                                                        )


                        )


select min(requested_at_date_in_et),    max(requested_at_date_in_et)
from intent_media_log_data_production.clicks