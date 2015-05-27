getQuery=function(start_date,end_date) {
  query<-paste(
    "
    drop table if exists intent_media_sandbox_production.GC_BWB_Imp;
    create table intent_media_sandbox_production.GC_BWB_Imp as
    select ent.name as 'Advertiser',codes1.code as 'Origin',codes2.code as 'Destination',fav1.name as 'Fav. Origin',fav2.name as 'Fav. Destination',ctry.name as 'Hotel Country',hot.name as 'Hotel City',
    imp.auction_position,imp.request_id as imp_request_id,imp.external_id,imp.advertiser_id,imp.campaign_id,imp.ad_group_id
    from intent_media_log_data_production.impressions imp
    inner join intent_media_production.entities ent on imp.advertiser_id=ent.id
    left join intent_media_production.airport_codes codes1 on imp.matched_origination_airport_code_id=codes1.id
    left join intent_media_production.airport_codes codes2 on imp.matched_destination_airport_code_id=codes2.id
    left join intent_media_production.countries ctry on imp.matched_hotel_country_id=ctry.id
    left join intent_media_production.air_ct_favorites fav1 on imp.matched_origination_air_ct_favorite_id=fav1.id 
    left join intent_media_production.air_ct_favorites fav2 on imp.matched_destination_air_ct_favorite_id=fav2.id 
    left join intent_media_production.hotel_cities hot on imp.matched_hotel_country_id=hot.id
    where imp.requested_at_date_in_et between ",start_date," and ",end_date,"
    and imp.ip_address_blacklisted = 'false'
    and imp.prechecked='false'
    and ent.name IN ('Orbitz','Expedia','Hotels.com','Trivago','United Airlines');
    
    
    drop table if exists intent_media_sandbox_production.GC_BWB_AC;
    create table intent_media_sandbox_production.GC_BWB_AC as
    select market_id,device_family,os_family,browser_family,product_category_type,
    days_to_arrival,dayofweek(requested_at_in_et) as 'Day',datediff(d,travel_date_start,travel_date_end) as 'LoS',
    (Case when datediff(d,travel_date_start,travel_date_end)>=7 OR dayofweek(travel_date_end)<dayofweek(travel_date_start) OR dayofweek(travel_date_start)=6 OR dayofweek(travel_date_end)=6 then 1 else 0 end) as 'Has Saturday',
    hour(case when time_zone_offset is null then requested_at_in_et else requested_at_in_et+ time_zone_offset*interval '1 minute' end) as hour,
    (Case when requested_at_date_in_et<date(",start_date,")+datediff(d,date(",start_date,"),date(",end_date,"))/2 then 'Train' else 'Test' end) as Period,
    webuser_id,request_id as ad_call_request_id,requested_at_in_et as ad_call_timestamp,requested_at_date_in_et as ad_call_date
    from intent_media_log_data_production.ad_calls
    where requested_at_date_in_et between ",start_date," and ",end_date,"
    and ip_address_blacklisted = 'false'
    and outcome_type='SERVED'
    and ad_unit_type = 'CT'
    and display_format_type='DESKTOP'
    and device_family IN ('COMPUTER','TABLET')
    and site_country='US';
    
    drop table if exists intent_media_sandbox_production.GC_BWB_Clicks;
    create table intent_media_sandbox_production.GC_BWB_Clicks as
    select external_impression_id,request_id as click_request_id,
    actual_cpc,rank() over(partition by external_impression_id order by requested_at_in_et) as single_click_rank,
    requested_at_in_et as click_timestamp,
    requested_at_date_in_et as click_date,
    webuser_id as click_webuser_id
    from intent_media_log_data_production.clicks
    where requested_at_date_in_et between ",start_date," and (date(",end_date,") + interval '24 hours')
    and ip_address_blacklisted = 0
    and fraudulent = 0;
    
    drop table if exists intent_media_sandbox_production.GC_BWB_Base;
    create table intent_media_sandbox_production.GC_BWB_Base as
    select ac.*, imp.*,c.*
    from intent_media_sandbox_production.GC_BWB_AC ac
    inner join intent_media_sandbox_production.GC_BWB_Imp imp on ac.ad_call_request_id=imp.imp_request_id
    inner join intent_media_sandbox_production.GC_BWB_Clicks c on imp.external_id = c.external_impression_id and ac.ad_call_timestamp < c.click_timestamp and c.click_timestamp <= (ac.ad_call_timestamp + interval '24 hours');
    
    drop table if exists intent_media_sandbox_production.GC_BWB_Conv_raw;
    create table intent_media_sandbox_production.GC_BWB_Conv_raw as
    select entity_id,order_id,request_id,requested_at_in_et,requested_at_date_in_et,
    product_category_type as conversion_product_category_type,
    webuser_id as conversion_webuser_id,
    conversion_value
    from intent_media_log_data_production.conversions
    where requested_at_date_in_et between ",start_date," and (date(",end_date,") + interval '30 days')
    and ip_address_blacklisted = 0;
    
    drop table if exists intent_media_sandbox_production.GC_BWB_Conv_dedup;
    create table intent_media_sandbox_production.GC_BWB_Conv_dedup as
    select
    con.*
    from 
    (
    select
    entity_id,
    order_id,
    min(requested_at_in_et) as min_requested_at_in_et
    from intent_media_sandbox_production.GC_BWB_Conv_raw
    where order_id is not null
    group by
    entity_id,
    order_id
    ) distinct_con
    left join intent_media_sandbox_production.GC_BWB_Conv_raw con
    on con.entity_id = distinct_con.entity_id
    and con.order_id = distinct_con.order_id
    and con.requested_at_in_et = distinct_con.min_requested_at_in_et
    union
    select *
    from intent_media_sandbox_production.GC_BWB_Conv_raw
    where order_id is null;
    
    drop table if exists intent_media_sandbox_production.GC_BWB_Conv_attrib;
    create table intent_media_sandbox_production.GC_BWB_Conv_attrib as
    select
    dc.*,
    base.click_request_id,
    rank() over (partition by dc.request_id order by base.click_timestamp desc) as click_rank
    from intent_media_sandbox_production.GC_BWB_Conv_dedup dc
    cross join intent_media_sandbox_production.GC_BWB_Base base
    where base.click_webuser_id = dc.conversion_webuser_id
    and base.advertiser_id = dc.entity_id
    and (base.click_timestamp + interval '30 days') >= dc.requested_at_in_et
    and base.click_timestamp < dc.requested_at_in_et;
    
    drop table if exists intent_media_sandbox_production.GC_BWB_Conv_lastclick;
    create table intent_media_sandbox_production.GC_BWB_Conv_lastclick as
    select *
    from intent_media_sandbox_production.GC_BWB_Conv_attrib
    where click_rank = 1;
    
    drop table if exists intent_media_sandbox_production.GC_BWB_Conv;
    create table intent_media_sandbox_production.GC_BWB_Conv as
    select click_request_id,
    sum(case when conversion_product_category_type = 'FLIGHTS' then conversion_value end) as conversion_value_sum_flights,
    sum(case when conversion_product_category_type = 'HOTELS' then conversion_value end) as conversion_value_sum_hotels,
    sum(case when conversion_product_category_type = 'PACKAGES' then conversion_value end) as conversion_value_sum_packages,
    sum(case when conversion_product_category_type = 'CARS' then conversion_value end) as conversion_value_sum_cars,
    sum(conversion_value) as conversion_value_sum_total
    from intent_media_sandbox_production.GC_BWB_Conv_lastclick
    group by click_request_id;
    
    drop table if exists intent_media_sandbox_production.GC_BWB_Final;
    create table intent_media_sandbox_production.GC_BWB_Final as
    select
    base.*,
    conv.conversion_value_sum_total,conv.conversion_value_sum_flights,conv.conversion_value_sum_hotels,conv.conversion_value_sum_packages,conv.conversion_value_sum_cars
    from intent_media_sandbox_production.GC_BWB_base base
    left join intent_media_sandbox_production.GC_BWB_Conv conv on base.click_request_id = conv.click_request_id;
    select * from intent_media_sandbox_production.GC_BWB_Final limit 100000
    ",sep="")
list_query<-strsplit(query,';')[[1]]
list_query<-list_query[nchar(list_query)>1]
return(list_query)
}