/* Create a base table with flags to indicate pixel types */
drop table if exists intent_media_sandbox_production.YB_pixel_range_join_practice_base;
create table intent_media_sandbox_production.YB_pixel_range_join_practice_base as
select
  browser,
  requested_at_in_et,
  case when request_url not like '%data%' then ip_address end as request_url_reference,
  case when request_url like '%data1%gif%' then ip_address end as request_url_flag_1,
  case when request_url like '%data2%gif%' then ip_address end as request_url_flag_2,
  case when request_url like '%data3%gif%' then ip_address end as request_url_flag_3,
  request_id
from intent_media_log_data_production.pixels
where requested_at_in_et between '2015-02-12 00:00:00' and '2015-02-13 00:00:10'
  and ip_address_blacklisted = 0
  and entity_id = 112
  and request_url like 'http://a.intentmedia.net/adServer/pixels?entity_id=112&group=im&product=flights%'
  and referrer_url like 'http://www.travelzoo.com/supersearch/%';
select analyze_histogram('intent_media_sandbox_production.YB_pixel_range_join_practice_base',100);

/* Sessionize the linkage between reference pixel and 3 other ones to be within 10 seconds and get the average lag 
   aggregated by browser */

select p1.browser, p1_num_ref_pixel, p1_num_matches, p1_avg_timestamp, p2_num_ref_pixel, p2_num_matches, p2_avg_timestamp, p3_num_ref_pixel, p3_num_matches, p3_avg_timestamp
from
(
  select browser, count(distinct base_request_id) as p1_num_ref_pixel, count(1) as p1_num_matches, round(avg(day(tdiff)*86400+hour(tdiff)*3600+minute(tdiff)*60+second(tdiff)),2) as p1_avg_timestamp
  from
  (
    select browser, base_request_id, case when t_flag - ifnull(lagged_flag, 0) = 1 then t_timestamp - ref_timestamp end as tdiff
    from
    (
      select *, lag(t_flag) over (partition by base_request_id order by t_timestamp desc) as lagged_flag
      from
      (
        select
          base.browser, base.request_id as base_request_id, base.requested_at_in_et as ref_timestamp, t.requested_at_in_et as t_timestamp,
          conditional_true_event(base.requested_at_in_et < t.requested_at_in_et and base.requested_at_in_et + interval '10 seconds' >= t.requested_at_in_et) over(partition by base.request_id order by base.requested_at_in_et, t.requested_at_in_et desc) as t_flag
        from intent_media_sandbox_production.YB_pixel_range_join_practice_base base
        inner join intent_media_sandbox_production.YB_pixel_range_join_practice_base t on base.request_url_reference = t.request_url_flag_1
        and base.request_url_reference is not null and t.request_url_flag_1 is not null
      ) intermediate
    ) final
  ) pixel
  where tdiff is not null
  group by browser
) p1
inner join
(
  select browser, count(distinct base_request_id) as p2_num_ref_pixel, count(1) as p2_num_matches, round(avg(day(tdiff)*86400+hour(tdiff)*3600+minute(tdiff)*60+second(tdiff)),2) as p2_avg_timestamp
  from
  (
    select browser, base_request_id, case when t_flag - ifnull(lagged_flag, 0) = 1 then t_timestamp - ref_timestamp end as tdiff
    from
    (
      select *, lag(t_flag) over (partition by base_request_id order by t_timestamp desc) as lagged_flag
      from
      (
        select
          base.browser, base.request_id as base_request_id, base.requested_at_in_et as ref_timestamp, t.requested_at_in_et as t_timestamp,
          conditional_true_event(base.requested_at_in_et < t.requested_at_in_et and base.requested_at_in_et + interval '10 seconds' >= t.requested_at_in_et) over(partition by base.request_id order by base.requested_at_in_et, t.requested_at_in_et desc) as t_flag
        from intent_media_sandbox_production.YB_pixel_range_join_practice_base base
        inner join intent_media_sandbox_production.YB_pixel_range_join_practice_base t on base.request_url_reference = t.request_url_flag_2
        and base.request_url_reference is not null and t.request_url_flag_2 is not null
      ) intermediate
    ) final
  ) pixel
  where tdiff is not null
  group by browser
) p2
on p1.browser = p2.browser
inner join
(
  select browser, count(distinct base_request_id) as p3_num_ref_pixel, count(1) as p3_num_matches, round(avg(day(tdiff)*86400+hour(tdiff)*3600+minute(tdiff)*60+second(tdiff)),2) as p3_avg_timestamp
  from
  (
    select browser, base_request_id, case when t_flag - ifnull(lagged_flag, 0) = 1 then t_timestamp - ref_timestamp end as tdiff
    from
    (
      select *, lag(t_flag) over (partition by base_request_id order by t_timestamp desc) as lagged_flag
      from
      (
        select
          base.browser, base.request_id as base_request_id, base.requested_at_in_et as ref_timestamp, t.requested_at_in_et as t_timestamp,
          conditional_true_event(base.requested_at_in_et < t.requested_at_in_et and base.requested_at_in_et + interval '10 seconds' >= t.requested_at_in_et) over(partition by base.request_id order by base.requested_at_in_et, t.requested_at_in_et desc) as t_flag
        from intent_media_sandbox_production.YB_pixel_range_join_practice_base base
        inner join intent_media_sandbox_production.YB_pixel_range_join_practice_base t on base.request_url_reference = t.request_url_flag_3
        and base.request_url_reference is not null and t.request_url_flag_3 is not null
      ) intermediate
    ) final
  ) pixel
  where tdiff is not null
  group by browser
) p3
on p1.browser = p3.browser
order by 1;
