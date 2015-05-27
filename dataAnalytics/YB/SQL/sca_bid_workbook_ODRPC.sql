select
  dim.airport_code,
  t2.days_to_arrival,
  t1.origin_clicks,
  t1.origin_gross_revenue,
  t1.origin_rpc,
  t2.origin_clicks as origin_dta_clicks,
  t2.origin_gross_revenue as origin_dta_gross_revenue,
  t2.origin_rpc as origin_dta_rpc,
  t1.destination_clicks,
  t1.destination_gross_revenue,
  t1.destination_rpc,
  t2.destination_clicks as destination_dta_clicks,
  t2.destination_gross_revenue as destination_dta_gross_revenue,
  t2.destination_rpc as destination_dta_rpc
from
(
  select distinct Origin as airport_code from intent_media_sandbox_production.GC_BWB_Final
  union
  select distinct Destination as airport_code from intent_media_sandbox_production.GC_BWB_Final
) dim
left join
(
/* single O/D */
select
  origin.*,
  origin.origin_gross_revenue/origin.origin_clicks as origin_rpc,
  destination.destination_clicks,
  destination.destination_gross_revenue,
  destination.destination_gross_revenue/destination.destination_clicks as destination_rpc
from(
select
  dim.airport_code,
  --days_to_arrival,
  count(click_request_id) as origin_clicks,
  sum(conversion_value_sum_total) as origin_gross_revenue
from
(
  select distinct Origin as airport_code from intent_media_sandbox_production.GC_BWB_Final
  union
  select distinct Destination as airport_code from intent_media_sandbox_production.GC_BWB_Final
) dim
left join intent_media_sandbox_production.GC_BWB_Final f on dim.airport_code = f.origin
group by 1--,2
order by 1/*,2*/) origin
inner join(
select
  dim.airport_code,
  --days_to_arrival,
  count(click_request_id) as destination_clicks,
  sum(conversion_value_sum_total) as destination_gross_revenue
from
(
  select distinct Origin as airport_code from intent_media_sandbox_production.GC_BWB_Final
  union
  select distinct Destination as airport_code from intent_media_sandbox_production.GC_BWB_Final
) dim
left join intent_media_sandbox_production.GC_BWB_Final f on dim.airport_code = f.Destination
group by 1--,2
order by 1/*,2*/) destination
on origin.airport_code = destination.airport_code
--and origin.days_to_arrival = destination.days_to_arrival
order by 1/*,2*/
) t1
on dim.airport_code = t1.airport_code
left join
(
/* single O/D + DTA */
select
  origin.*,
  origin.origin_gross_revenue/origin.origin_clicks as origin_rpc,
  destination.destination_clicks,
  destination.destination_gross_revenue,
  destination.destination_gross_revenue/destination.destination_clicks as destination_rpc
from(
select
  dim.airport_code,
  days_to_arrival,
  count(click_request_id) as origin_clicks,
  sum(conversion_value_sum_total) as origin_gross_revenue
from
(
  select distinct Origin as airport_code from intent_media_sandbox_production.GC_BWB_Final
  union
  select distinct Destination as airport_code from intent_media_sandbox_production.GC_BWB_Final
) dim
left join intent_media_sandbox_production.GC_BWB_Final f on dim.airport_code = f.origin
group by 1,2
order by 1,2) origin
inner join(
select
  dim.airport_code,
  days_to_arrival,
  count(click_request_id) as destination_clicks,
  sum(conversion_value_sum_total) as destination_gross_revenue
from
(
  select distinct Origin as airport_code from intent_media_sandbox_production.GC_BWB_Final
  union
  select distinct Destination as airport_code from intent_media_sandbox_production.GC_BWB_Final
) dim
left join intent_media_sandbox_production.GC_BWB_Final f on dim.airport_code = f.Destination
group by 1,2
order by 1,2) destination
on origin.airport_code = destination.airport_code
and origin.days_to_arrival = destination.days_to_arrival
order by 1,2
) t2
on dim.airport_code = t2.airport_code;
