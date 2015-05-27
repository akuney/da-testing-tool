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

drop table if exists intent_media_sandbox_production.YB_pixel_range_join_practice_intermediate;
create table intent_media_sandbox_production.YB_pixel_range_join_practice_intermediate as
select
  base.browser,
  base.request_id,
  case when conditional_true_event(base.requested_at_in_et < t1.requested_at_in_et) over(partition by base.request_url_reference order by base.requested_at_in_et, t1.requested_at_in_et) > 0 then t1.requested_at_in_et - base.requested_at_in_et end as tdiff1,
  case when conditional_true_event(base.requested_at_in_et < t2.requested_at_in_et) over(partition by base.request_url_reference order by base.requested_at_in_et, t2.requested_at_in_et) > 0 then t2.requested_at_in_et - base.requested_at_in_et end as tdiff2,
  case when conditional_true_event(base.requested_at_in_et < t3.requested_at_in_et) over(partition by base.request_url_reference order by base.requested_at_in_et, t3.requested_at_in_et) > 0 then t3.requested_at_in_et - base.requested_at_in_et end as tdiff3
from intent_media_sandbox_production.YB_pixel_range_join_practice_base base
inner join intent_media_sandbox_production.YB_pixel_range_join_practice t1 on base.request_url_reference = t1.request_url_flag_1 and base.requested_at_in_et <= t1.requested_at_in_et
inner join intent_media_sandbox_production.YB_pixel_range_join_practice t2 on base.request_url_reference = t2.request_url_flag_2 and base.requested_at_in_et <= t2.requested_at_in_et
inner join intent_media_sandbox_production.YB_pixel_range_join_practice t3 on base.request_url_reference = t3.request_url_flag_3 and base.requested_at_in_et <= t3.requested_at_in_et;

select
  browser,
  count(request_id),
  round(avg(day(tdiff1)*86400+hour(tdiff1)*3600+minute(tdiff1)*60+second(tdiff1)),2),
  round(avg(day(tdiff2)*86400+hour(tdiff2)*3600+minute(tdiff2)*60+second(tdiff2)),2),
  round(avg(day(tdiff3)*86400+hour(tdiff3)*3600+minute(tdiff3)*60+second(tdiff3)),2)
from intent_media_sandbox_production.YB_pixel_range_join_practice_intermediate
group by browser
order by browser;
