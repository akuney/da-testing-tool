/*
    YB: Because we are using customized table in production to build this dashboard,
    the length of each query is very short. So, I put all of them in this file
*/

/* data source 1 - media (vsql) */
select * from intent_media_production.mvt_c13n_media_experiment_results

/* data source 2 - reference (mysql) */
select * from mvt_c13n_experiments

/* data source 3 - revenue share (vsql) */
select
  s.id as site_id,
  s.name as site_type,
  e.revenue_share
from intent_media_production.sites s
inner join intent_media_production.entities e on s.publisher_id = e.id

/* data source 4 - transaction (vsql) */
select * from intent_media_production.mvt_c13n_transaction_experiment_results