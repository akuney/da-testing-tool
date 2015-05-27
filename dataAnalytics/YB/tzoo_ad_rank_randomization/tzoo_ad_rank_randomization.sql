DROP TABLE IF EXISTS intent_media_sandbox_production.YB_tzoo_randomization_aic;
CREATE TABLE intent_media_sandbox_production.YB_tzoo_randomization_aic
(
      ac_request_id varchar,
      ac_publisher_user_id varchar,
      ac_webuser_id varchar,
      ac_date date,
      ac_unix_timestamp int,
      i_advertiser_id varchar,
      i_auction_position int,
      c_request_id varchar,
      c_actual_cpc decimal,
      c_date date,
      c_unix_timestamp int
);

COPY intent_media_sandbox_production.YB_tzoo_randomization_aic
(
      ac_request_id,
      ac_publisher_user_id,
      ac_webuser_id,
      ac_date,
      ac_unix_timestamp,
      i_advertiser_id,
      i_auction_position,
      c_request_id,
      c_actual_cpc,
      c_date,
      c_unix_timestamp
)
FROM LOCAL '/Users/yoojong.bang/Desktop/tzoo_randomization/part-r-all'
WITH DELIMITER E'\t'
NULL as 'NULL';

DROP TABLE IF EXISTS intent_media_sandbox_production.YB_tzoo_randomization_deduped_conversions;
CREATE TABLE intent_media_sandbox_production.YB_tzoo_randomization_deduped_conversions
(
      request_id varchar,
      entity_id int,
      order_id varchar,
      webuser_id varchar,
      conversion_value decimal,
      net_conversion_value decimal,
      requested_at_date_in_et date,
      unix_requested_at int
);

COPY intent_media_sandbox_production.YB_tzoo_randomization_deduped_conversions
(
      request_id,
      entity_id,
      order_id,
      webuser_id,
      conversion_value,
      net_conversion_value,
      requested_at_date_in_et,
      unix_requested_at
)
FROM LOCAL '/Users/yoojong.bang/Desktop/tzoo_randomization_con/part-m-all'
WITH DELIMITER E'\t'
NULL as 'NULL';

/* Convert Unix Timestamp to Vertica Timestamp */
drop table if exists intent_media_sandbox_production.YB_tzoo_randomization_aic_ts;
create table intent_media_sandbox_production.YB_tzoo_randomization_aic_ts as
select
  *,
  TO_TIMESTAMP(left(cast(ac_unix_timestamp as varchar),10)) as ac_timestamp,
  TO_TIMESTAMP(left(cast(c_unix_timestamp as varchar),10)) as c_timestamp
from intent_media_sandbox_production.YB_tzoo_randomization_aic;

drop table if exists intent_media_sandbox_production.YB_tzoo_randomization_dc_ts;
create table intent_media_sandbox_production.YB_tzoo_randomization_dc_ts as
select
  *,
  TO_TIMESTAMP(left(cast(unix_requested_at as varchar),10)) as con_timestamp
from intent_media_sandbox_production.YB_tzoo_randomization_deduped_conversions;

/* Conversion Attribution */
drop table if exists intent_media_sandbox_production.YB_tzoo_randomization_deduped_conversions_per_click_ordered;
create table intent_media_sandbox_production.YB_tzoo_randomization_deduped_conversions_per_click_ordered as
select
  dc.*,
  aic.click_request_id,
  rank() over (partition by dc.request_id order by aic.click_timestamp desc) as click_rank
from intent_media_sandbox_production.YB_tzoo_randomization_deduped_conversions dc
cross join intent_media_sandbox_production.YB_tzoo_randomization_aic aic
where aic.ac_webuser_id = dc.webuser_id
  and aic.advertiser_id = dc.entity_id
  and (aic.click_timestamp + interval '30 days') >= dc.requested_at_in_et
  and aic.click_timestamp < dc.requested_at_in_et;

/* Get Last Click Only */
drop table if exists intent_media_sandbox_production.YB_tzoo_randomization_dc_per_click;
create table intent_media_sandbox_production.YB_tzoo_randomization_dc_per_click as
select *
from intent_media_sandbox_production.YB_tzoo_randomization_deduped_conversions_per_click_ordered
where click_rank = 1;

/* Aggregate Conversions and Clicks */
drop table if exists intent_media_sandbox_production.YB_tzoo_randomization_dc_by_click;
create table intent_media_sandbox_production.YB_tzoo_randomization_dc_by_click as
select
  c_request_id,
  count(request_id) as conversion_count_total,
  sum(conversion_value) as conversion_value_sum_total,
  sum(net_conversion_value) as net_conversion_value_sum_total
from intent_media_sandbox_production.YB_tzoo_randomization_dc_per_click
group by
  click_request_id;

/* Left Join Back to Valid Clicks */
drop table if exists intent_media_sandbox_production.YB_tzoo_randomization_clicks_with_conversions;
create table intent_media_sandbox_production.YB_tzoo_randomization_clicks_with_conversions as
select
  aic.*,
  d.conversion_count_total,
  d.conversion_value_sum_total,
  d.net_conversion_value_sum_total
from intent_media_sandbox_production.YB_tzoo_randomization_aic_ts aic
left join intent_media_sandbox_production.YB_tzoo_randomization_dc_by_click d
on aic.c_request_id = d.c_request_id;

@export on;
@export set format="csv";
@export set filename "/Users/yoojong.bang/Desktop/tzoo_randomization.csv";
select * from intent_media_sandbox_production.YB_tzoo_randomization_clicks_with_conversions;
@export off;