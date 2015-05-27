------look into Full Outer join incase Error does not have an Ad Call. Could happen in the future

select
        t1.ad_unit_id,
        CASE au.ad_Type
                WHEN 'CT' THEN 'SC'
                WHEN 'META'THEN 'PPA'
                WHEN 'SSR' THEN 'SSN'
                END AS Ad_Type,
        au.Product_Category_Type,
        au.name as ad_unit_name,
        s.Name as Site,
        e.Name as Publisher,
        e.publisher_tier as "Publisher Tier",
        lpt.page_type as "Type of Ad Unit",
        t1.aggregation_level_date_in_et as Date,
        t1.ad_calls,
        isnull(t2.sum_request_error_count,0) as errors,
        isnull(cast(round(isnull(t2.sum_request_error_count,0)/(t1.ad_calls+isnull(t2.sum_request_error_count,0)),3) as numeric(8,3)),1) as error_ratio,
        ISNULL(travel_date_end_before_start,0) as travel_date_end_before_start,
        ISNULL(uncaught_exception,0) as uncaught_exception,
        ISNULL(parsing_error,0) as parsing_error,
        ISNULL(inactive_entity,0) as inactive_entity,
        ISNULL(derived_intent_mismatch,0) as derived_intent_mismatch,
        ISNULL(unknown_entity,0) as unknown_entity,
        ISNULL(unknown_hotel_property,0) as unknown_hotel_property,
        ISNULL(unknown_product_category,0) as unknown_product_category,
        ISNULL(unknown_ad_unit,0) as unknown_ad_unit,
        ISNULL(unknown_market,0) as unknown_market,
        ISNULL(unknown_airport_code,0) as unknown_airport_code
from 
( -- air_ct 
 select
        ad_unit_id,
        aggregation_level_date_in_et,
        sum(ad_call_count) as ad_calls
 from
        intent_media_production.air_ct_media_performance_aggregations
where 
        aggregation_level_date_in_et >= To_date('06/15/2014','mm/dd/yyyy')
     --   aggregation_level_date_in_et  > cast (current_date as timestamp)-16
       AND aggregation_level_date_in_et  < cast (current_date as timestamp)
group by
        1,2
union
 select -- hotel_ct
        ad_unit_id,
        aggregation_level_date_in_et,
        sum(ad_call_count) as ad_calls
 from
        intent_media_production.hotel_ct_media_performance_aggregations
where 
        aggregation_level_date_in_et >= To_date('06/15/2014','mm/dd/yyyy')
       -- aggregation_level_date_in_et > cast (current_date as timestamp)-16
        AND aggregation_level_date_in_et  < cast (current_date as timestamp)
group by
        1,2
union
select -- ssn
        ad_unit_id,
        aggregation_level_date_in_et,
        sum(ad_call_count) as ad_calls
 from
        intent_media_production.publisher_performance_report_aggregations
where 
        aggregation_level_date_in_et >= To_date('06/15/2014','mm/dd/yyyy')
      --  aggregation_level_date_in_et > cast(current_date as timestamp)-16
       AND aggregation_level_date_in_et  < cast (current_date as timestamp)
group by
        1,2
      
 -- need to union in meta (PPA) ad calls 
union        
select 
        ad_unit_id,
        aggregation_level_date_in_et,
        sum(ad_call_count) as ad_calls
 from
         intent_media_production.hotel_meta_media_performance_aggregations
where 
        aggregation_level_date_in_et >= To_date('06/15/2014','mm/dd/yyyy')
       -- aggregation_level_date_in_et > cast (current_date as timestamp)-16
       AND aggregation_level_date_in_et  < cast (current_date as timestamp)
group by
        1,2

        ) t1
left join

( 
 select
        ad_unit_id,
        date_in_et,
        request_error_count as sum_request_error_count,
        CASE travel_date_end_before_start WHEN TRUE THEN 1 ELSE 0 END * request_error_count AS travel_date_end_before_start,
        CASE uncaught_exception WHEN TRUE THEN 1 ELSE 0 END * request_error_count AS uncaught_exception,
        CASE parsing_error WHEN TRUE THEN 1 ELSE 0 END * request_error_count AS parsing_error,
        CASE inactive_entity WHEN TRUE THEN 1 ELSE 0 END * request_error_count AS inactive_entity,
        CASE derived_intent_mismatch WHEN TRUE THEN 1 ELSE 0 END * request_error_count AS derived_intent_mismatch,
        CASE unknown_entity WHEN TRUE THEN 1 ELSE 0 END * request_error_count AS unknown_entity,
        CASE unknown_hotel_property WHEN TRUE THEN 1 ELSE 0 END * request_error_count AS unknown_hotel_property,
        CASE unknown_product_category WHEN TRUE THEN 1 ELSE 0 END * request_error_count AS unknown_product_category,
        CASE unknown_ad_unit WHEN TRUE THEN 1 ELSE 0 END * request_error_count AS unknown_ad_unit,
        CASE unknown_market WHEN TRUE THEN 1 ELSE 0 END * request_error_count AS unknown_market,
        CASE unknown_airport_code WHEN TRUE THEN 1 ELSE 0 END * request_error_count AS unknown_airport_code
 from
        intent_media_production.request_error_aggregations
 where
        date_in_et >= To_date('06/15/2014','mm/dd/yyyy') AND
       -- date_in_et > cast(current_date as timestamp)-16
       date_in_et  < cast (current_date as timestamp) and 
       request_type in ('meta', 'impressions')
        and unknown_market<>1
 ) t2
 
 on t1.ad_unit_id=t2.ad_unit_id and t1.aggregation_level_date_in_et=t2.date_in_et

join intent_media_production.ad_units au on t1.ad_unit_id=au.id
join intent_media_production.legal_page_types lpt on lpt.id = au.legal_page_type_id
JOIN Intent_media_production.Sites s ON au.Site_ID = s.ID
JOIN intent_media_production.entities e ON s.Publisher_ID = e.ID
WHERE entity_type = 'Publisher'

order by 
        aggregation_level_date_in_et, ad_unit_id
 