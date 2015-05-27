select
    table_name, 
    sum(column_used_bytes) as table_size_in_bytes,
    trim(trailing '0' from (cast(round(SUM(column_used_bytes) / 1024, 2) as varchar))) as table_size_in_KB,
    trim(trailing '0' from (cast(round(SUM(column_used_bytes) / 1024 /1024, 2) as varchar))) as table_size_in_MB,
    trim(trailing '0' from (cast(round(SUM(column_used_bytes) / 1024 /1024 /1024, 2) as varchar))) as table_size_in_GB,
    trim(trailing '0' from (cast(round(SUM(column_used_bytes) / 1024 /1024 /1024 /1024, 2) as varchar))) as table_size_in_TB
from
(
    select
        anchor_table_name as table_name, 
        anchor_table_schema, 
        sum(used_bytes) as column_used_bytes
    from column_storage 
    where anchor_table_schema = 'intent_media_log_data_production'
        and node_name in (select node_name from nodes)
    group BY 
        anchor_table_name, 
        anchor_table_schema 
    having anchor_table_schema = 'intent_media_log_data_production'
) sub
group by 
    table_name
order by
    table_name;
