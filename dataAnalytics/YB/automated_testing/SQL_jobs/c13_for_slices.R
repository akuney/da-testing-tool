library(RJDBC)

#Read .ad_hoc_mvt.properties to load the database credential
credential <- scan(file="~/.adhoc_mvt.properties", what="character")
username <- credential[3]
password <- credential[6]

#Load DB Connection Information
scriptDir <- getwd()
vDriverPath <- paste(scriptDir, "drivers", "vertica-jdk5-6.1.3-0.jar", sep="/")

vDriver <- JDBC(driverClass="com.vertica.jdbc.Driver", classPath=vDriverPath)
verticaProduction <- dbConnect(vDriver,"jdbc:vertica://production-vertica-cluster-1.internal.intentmedia.net:5433/intent_media?SearchPath=intent_media_production",username,password)
verticaLogProduction <- dbConnect(vDriver,"jdbc:vertica://production-vertica-cluster-1.internal.intentmedia.net:5433/intent_media?SearchPath=intent_media_log_data_production",username,password)

#Parameters to read in query
siteType <- commandArgs(TRUE)[1]
productCategoryType <- commandArgs(TRUE)[2]
adType <- commandArgs(TRUE)[3]
if(commandArgs(TRUE)[4] == null) {
    adUnit <- paste("is not ",commandArgs(TRUE)[4])
} else {
    adUnit <- paste("= ",commandArgs(TRUE)[4])
}

c13_for_slices <- function(siteType, productCategoryType, adType, adUnit) {

    print("QA process for c13_for_slices started...")
    start_time <- Sys.time()
    test_query <- paste(
    "
    delete table if exists intent_media_sandbox_production.YB_model_slice_experiment_results;
    insert into intent_media_sandbox_production.YB_model_slice_experiment_results
    (
      experiment_id,
      publisher_type,
      clicked_type,
      site_reporting_value_01,
      site_reporting_value_02,
      site_reporting_value_03,
      site_reporting_value_04,
      site_reporting_value_05,
      site_reporting_value_06,
      site_reporting_value_07,
      pure_group_type,
      user_segmentation_type,
      model_slice_id,
      treatment,
      segmentation_model_percentile,
      number_users_total,
      number_product_specific_ad_calls,
      net_media_revenue,
      has_conversion_flights,
      has_conversion_hotels,
      has_conversion_cars,
      has_conversion_packages,
      has_conversion_total,
      number_conversions_flights,
      number_conversions_hotels,
      number_conversions_cars,
      number_conversions_packages,
      number_conversions_total,
      net_value_conversions_flights,
      net_value_conversions_hotels,
      net_value_conversions_cars,
      net_value_conversions_packages,
      net_value_conversions_total,
      number_flight_attach_conversions_flights,
      number_flight_attach_conversions_hotels,
      number_flight_attach_conversions_cars,
      number_flight_attach_conversions_packages,
      number_flight_attach_conversions_total,
      number_hotel_attach_conversions_flights,
      number_hotel_attach_conversions_hotels,
      number_hotel_attach_conversions_cars,
      number_hotel_attach_conversions_packages,
      number_hotel_attach_conversions_total,
      has_insurance_conversion_flights,
      has_insurance_conversion_hotels,
      has_insurance_conversion_cars,
      has_insurance_conversion_packages,
      has_insurance_conversion_total,
      number_insurance_conversions_flights,
      number_insurance_conversions_hotels,
      number_insurance_conversions_cars,
      number_insurance_conversions_packages,
      number_insurance_conversions_total,
      net_value_insurance_conversions_flights,
      net_value_insurance_conversions_hotels,
      net_value_insurance_conversions_cars,
      net_value_insurance_conversions_packages,
      net_value_insurance_conversions_total,
      created_at,
      updated_at
    )
    select
      -- :experimentId as experiment_name,
      ", siteType," as publisher_type,
      case when click.clicked_type is null then 'NOT_CLICKED' else click.clicked_type end as clicked_type,
      ad_call.site_reporting_value_01,
      ad_call.site_reporting_value_02,
      ad_call.site_reporting_value_03,
      ad_call.site_reporting_value_04,
      ad_call.site_reporting_value_05,
      ad_call.site_reporting_value_06,
      ad_call.site_reporting_value_07,
      ad_call.pure_group_type,
      ad_call.user_segmentation_type,
      ad_call.model_slice_id,
      ad_call.treatment,
      null as segmentation_model_percentile,
      count(distinct ad_call.publisher_user_id) as number_users_total,
      sum(ad_call.user_number_product_specific_ad_calls) as number_product_specific_ad_calls,
      sum(click.actual_cpc_sum * rev_share.air_ct_revenue_share) as net_media_revenue,
      count(distinct(case when conversion.user_number_conversion_flights > 0 then ad_call.publisher_user_id end)) as has_conversion_flights,
      count(distinct(case when conversion.user_number_conversion_hotels > 0 then ad_call.publisher_user_id end)) as has_conversion_hotels,
      count(distinct(case when conversion.user_number_conversion_cars > 0 then ad_call.publisher_user_id end)) as has_conversion_cars,
      count(distinct(case when conversion.user_number_conversion_packages > 0 then ad_call.publisher_user_id end)) as has_conversion_packages,
      count(distinct(case when (conversion.user_number_conversion_flights > 0 or conversion.user_number_conversion_hotels > 0 or conversion.user_number_conversion_cars > 0 or conversion.user_number_conversion_packages > 0) then ad_call.publisher_user_id end)) as has_conversion_total,
      -- now lets all sum the conversions nums
      sum(conversion.user_number_conversion_flights) as number_conversions_flights,
      sum(conversion.user_number_conversion_hotels) as number_conversions_hotels,
      sum(conversion.user_number_conversion_cars) as number_conversions_cars,
      sum(conversion.user_number_conversion_packages) as number_conversions_packages,
      sum(conversion.user_number_conversion_packages + conversion.user_number_conversion_flights + conversion.user_number_conversion_cars + conversion.user_number_conversion_hotels) as number_conversions_total,
      -- net values of conversions
      sum(conversion.user_net_conversion_flights) as net_value_conversions_flights,
      sum(conversion.user_net_conversion_hotels) as net_value_conversions_hotels,
      sum(conversion.user_net_conversion_cars) as net_value_conversions_cars,
      sum(conversion.user_net_conversion_packages) as net_value_conversions_packages,
      sum(conversion.user_net_conversion_flights + conversion.user_net_conversion_cars + conversion.user_net_conversion_packages + conversion.user_net_conversion_hotels) as net_value_conversions_total,
      -- FLIGHTS attach conversions ---
      sum(conversion.user_flights_attach_flights) as number_flight_attach_conversions_flights,
      sum(conversion.user_flights_attach_hotels) as number_flight_attach_conversions_hotels,
      sum(conversion.user_flights_attach_cars) as number_flight_attach_conversions_cars,
      sum(conversion.user_flights_attach_packages) as number_flight_attach_conversions_packages,
      sum(conversion.user_flights_attach_flights + conversion.user_flights_attach_cars + conversion.user_flights_attach_packages + conversion.user_flights_attach_hotels) as number_flight_attach_conversions_total,
      -- HOTELS attach conversions ---
      sum(conversion.user_hotels_attach_flights) as number_hotel_attach_conversions_flights,
      sum(conversion.user_hotels_attach_hotels) as number_hotel_attach_conversions_hotels,
      sum(conversion.user_hotels_attach_cars) as number_hotel_attach_conversions_cars,
      sum(conversion.user_hotels_attach_packages) as number_hotel_attach_conversions_packages,
      sum(conversion.user_hotels_attach_flights + conversion.user_hotels_attach_cars + conversion.user_hotels_attach_packages + conversion.user_hotels_attach_hotels) as number_hotel_attach_conversions_total,
      ---- insurance --
      sum(case when conversion.user_number_insurance_conversion_flights > 0 then 1 else 0 end) as has_insurance_conversion_flights,
      sum(case when conversion.user_number_insurance_conversion_hotels > 0 then 1 else 0 end) as has_insurance_conversion_hotels,
      sum(case when conversion.user_number_insurance_conversion_cars > 0 then 1 else 0 end) as has_insurance_conversion_cars,
      sum(case when conversion.user_number_insurance_conversion_packages > 0 then 1 else 0 end) as has_insurance_conversion_packages,
      sum(case when conversion.user_number_insurance_conversion_flights > 0 or conversion.user_number_insurance_conversion_cars > 0 or conversion.user_number_insurance_conversion_hotels > 0 or conversion.user_number_insurance_conversion_packages > 0 then 1 else 0 end) as has_insurance_conversion_total,
      sum(conversion.user_number_insurance_conversion_flights) as number_insurance_conversions_flights,
      sum(conversion.user_number_insurance_conversion_hotels) as number_insurance_conversions_hotels,
      sum(conversion.user_number_insurance_conversion_cars) as number_insurance_conversions_cars,
      sum(conversion.user_number_insurance_conversion_packages) as number_insurance_conversions_packages,
      sum(conversion.user_number_insurance_conversion_packages + conversion.user_number_insurance_conversion_hotels + conversion.user_number_insurance_conversion_cars + conversion.user_number_insurance_conversion_flights) as number_insurance_conversions_total,
      -- insurance net --
      sum(conversion.user_net_insurance_conversion_flights) as net_value_insurance_conversions_flights,
      sum(conversion.user_net_insurance_conversion_hotels) as net_value_insurance_conversions_hotels,
      sum(conversion.user_net_insurance_conversion_cars) as net_value_insurance_conversions_cars,
      sum(conversion.user_net_insurance_conversion_packages) as net_value_insurance_conversions_packages,
      sum(conversion.user_net_insurance_conversion_packages + conversion.user_net_insurance_conversion_flights + conversion.user_net_insurance_conversion_cars + conversion.user_net_insurance_conversion_hotels) as net_value_insurance_conversions_total,
      current_timestamp,
      current_timestamp
    from
    ----------------------
    ----ad calls ---------
    ----------------------
      (
        select
          ad_unit_id,
          show_ads,
          site_reporting_value_01,
          site_reporting_value_02,
          site_reporting_value_03,
          site_reporting_value_04,
          site_reporting_value_05,
          site_reporting_value_06,
          site_reporting_value_07,
          publisher_user_id,
          case when count(distinct(traffic_share_type)) > 1 then 'MULTIPLE' else min(traffic_share_type) end as traffic_share_type,
          case when count(distinct(pure_group_type)) > 1 then 'MULTIPLE' else min(pure_group_type) end as pure_group_type,
          case when count(distinct(model_slice_id)) > 1 then 0 else min(model_slice_id) end as model_slice_id,
          case when count(distinct(segmentation_type)) > 1 then 'DIRTY' else min(segmentation_type) end as user_segmentation_type,
          case when count(distinct(treatment)) > 1 then '0' else min(treatment) end as treatment,
          count(*) as user_number_product_specific_ad_calls,
          sum(case when outcome_type = 'SERVED' then 1 end) as user_number_served_ad_calls_flights
        from intent_media_log_data_production.ad_calls
        where requested_at_date_in_et >= '2014-09-01'
          and requested_at_date_in_et <= '2014-09-01'
          and ip_address_blacklisted = 0
          and site_type = ", siteType,"
          and publisher_user_id is not null
          --and publisher_id = :publisherId
          and ad_unit_type = ", adType,"
          and product_category_type like ", productCategoryType,"
          and ad_unit_id = ", adUnit,"
        group by
          ad_unit_id,
          show_ads,
          site_reporting_value_01,
          site_reporting_value_02,
          site_reporting_value_03,
          site_reporting_value_04,
          site_reporting_value_05,
          site_reporting_value_06,
          site_reporting_value_07,
          publisher_user_id
      ) ad_call
    left join
    ----------------------
    ----rev share --------
    ----------------------
      (
        select
          publisher_user_id,
          air_ct_revenue_share
        from intent_media_log_data_production.ad_calls
        where requested_at_date_in_et >= '2014-09-01'
          and requested_at_date_in_et <= '2014-09-01'
          and ip_address_blacklisted = 0
          and site_type = ", siteType,"
          and publisher_user_id is not null
          --and publisher_id = :publisherId
          and ad_unit_type = ", adType,"
          and product_category_type like ", productCategoryType,"
        group by
          publisher_user_id,
          air_ct_revenue_share
      ) rev_share
    on ad_call.publisher_user_id = rev_share.publisher_user_id
    left join
    -------------------------
    ----conversions ---------
    -------------------------
      (
        select
          deduped_conversion.publisher_user_id,
          -- how many HOTELS, CARS and FLIGHTS conversions are attached to the first flight conversions
          sum(case when deduped_conversion.product_category_type ='HOTELS' and deduped_conversion.requested_at > min_date_by_publisher_user_id.min_flight_conversion then 1 else 0 end) as user_flights_attach_hotels,
          sum(case when deduped_conversion.product_category_type ='CARS' and deduped_conversion.requested_at > min_date_by_publisher_user_id.min_flight_conversion then 1 else 0 end) as user_flights_attach_cars,
          sum(case when deduped_conversion.product_category_type ='PACKAGES' and deduped_conversion.requested_at > min_date_by_publisher_user_id.min_flight_conversion then 1 else 0 end) as user_flights_attach_packages,
          sum(case when deduped_conversion.product_category_type ='FLIGHTS' and deduped_conversion.requested_at > min_date_by_publisher_user_id.min_flight_conversion then 1 else 0 end) as user_flights_attach_flights,
          -- how many HOTELS, CARS and FLIGHTS conversions are attached to the first HOTEL conversions
          sum(case when deduped_conversion.product_category_type ='HOTELS' and deduped_conversion.requested_at > min_date_by_publisher_user_id.min_hotel_conversion then 1 else 0 end) as user_hotels_attach_hotels,
          sum(case when deduped_conversion.product_category_type ='CARS' and deduped_conversion.requested_at > min_date_by_publisher_user_id.min_hotel_conversion then 1 else 0 end) as user_hotels_attach_cars,
          sum(case when deduped_conversion.product_category_type ='PACKAGES' and deduped_conversion.requested_at > min_date_by_publisher_user_id.min_hotel_conversion then 1 else 0 end) as user_hotels_attach_packages,
          sum(case when deduped_conversion.product_category_type ='FLIGHTS' and deduped_conversion.requested_at > min_date_by_publisher_user_id.min_hotel_conversion then 1 else 0 end) as user_hotels_attach_flights,
          -- just counts of each type of conversions
          sum(case when deduped_conversion.product_category_type ='FLIGHTS' then 1 else 0 end) as user_number_conversion_flights,
          sum(case when deduped_conversion.product_category_type ='HOTELS' then 1 else 0 end) as user_number_conversion_hotels,
          sum(case when deduped_conversion.product_category_type ='CARS' then 1 else 0 end) as user_number_conversion_cars,
          sum(case when deduped_conversion.product_category_type ='PACKAGES' then 1 else 0 end) as user_number_conversion_packages,
          --now conversions net value
          sum(case when deduped_conversion.product_category_type ='FLIGHTS' then deduped_conversion.net_conversion_value else 0 end) as user_net_conversion_flights,
          sum(case when deduped_conversion.product_category_type ='HOTELS' then deduped_conversion.net_conversion_value else 0 end) as user_net_conversion_hotels,
          sum(case when deduped_conversion.product_category_type ='CARS' then deduped_conversion.net_conversion_value else 0 end) as user_net_conversion_cars,
          sum(case when deduped_conversion.product_category_type ='PACKAGES' then deduped_conversion.net_conversion_value else 0 end) as user_net_conversion_packages,
          --now insurance counts
          sum(case when (deduped_conversion.product_category_type ='FLIGHTS' and deduped_conversion.net_insurance_value >0) then 1 else 0 end) as user_number_insurance_conversion_flights,
          sum(case when (deduped_conversion.product_category_type ='HOTELS' and deduped_conversion.net_insurance_value >0) then 1 else 0 end) as user_number_insurance_conversion_hotels,
          sum(case when (deduped_conversion.product_category_type ='CARS' and deduped_conversion.net_insurance_value >0) then 1 else 0 end) as user_number_insurance_conversion_cars,
          sum(case when (deduped_conversion.product_category_type ='PACKAGES' and deduped_conversion.net_insurance_value >0) then 1 else 0 end) as user_number_insurance_conversion_packages,
          --now insurance net value
          sum(case when (deduped_conversion.product_category_type ='FLIGHTS' and deduped_conversion.net_insurance_value >0) then deduped_conversion.net_insurance_value else 0 end) as user_net_insurance_conversion_flights,
          sum(case when (deduped_conversion.product_category_type ='HOTELS' and deduped_conversion.net_insurance_value >0) then deduped_conversion.net_insurance_value else 0 end) as user_net_insurance_conversion_hotels,
          sum(case when (deduped_conversion.product_category_type ='CARS' and deduped_conversion.net_insurance_value >0) then deduped_conversion.net_insurance_value else 0 end) as user_net_insurance_conversion_cars,
          sum(case when (deduped_conversion.product_category_type ='PACKAGES' and deduped_conversion.net_insurance_value >0) then deduped_conversion.net_insurance_value else 0 end) as user_net_insurance_conversion_packages
        from
          (
            select
              publisher_user_id,
              order_id,
              round(net_conversion_value, 2) as groupable_net_conversion_value,
              min(requested_at) as requested_at,
              min(net_conversion_value) as net_conversion_value,
              min(product_category_type) as product_category_type,
              min(net_insurance_value) as net_insurance_value
            from intent_media_log_data_production.conversions
            where requested_at_date_in_et >= '2014-09-01'
              and requested_at_date_in_et <= '2014-09-01'
              and ip_address_blacklisted = 0
              and site_type = ", siteType,"
              --and entity_id = :publisherId
              and publisher_user_id is not null
            group by
              publisher_user_id,
              order_id,
              round(net_conversion_value, 2)
            order by
              requested_at
          ) deduped_conversion
        join
          (
            select
              conversions.publisher_user_id,
              min(case when conversions.product_category_type = 'FLIGHTS' then conversions.requested_at else null end) as min_flight_conversion,
              min(case when conversions.product_category_type = 'HOTELS' then conversions.requested_at else null end) as min_hotel_conversion,
              min(min_date_by_publisher_user_id.requested_at) as min_any_conversion
            from intent_media_log_data_production.conversions
            join
              (
                select
                  publisher_user_id,
                  min(requested_at) as requested_at
                from intent_media_log_data_production.ad_calls
                where requested_at_date_in_et >= '2014-09-01'
                  and requested_at_date_in_et <= '2014-09-01'
                  and ip_address_blacklisted = 0
                  and site_type = ", siteType,"
                  and ad_unit_type = ", adType,"
                  and publisher_user_id is not null
                  --and publisher_id = :publisherId
                  and product_category_type like", productCategoryType,"
                group by
                  publisher_user_id
              ) min_date_by_publisher_user_id
              on conversions.publisher_user_id = min_date_by_publisher_user_id.publisher_user_id
              and conversions.requested_at > min_date_by_publisher_user_id.requested_at
              where conversions.requested_at_date_in_et >= '2014-09-01'
                and conversions.requested_at_date_in_et <= '2014-09-01'
                and ip_address_blacklisted = 0
                and site_type = ", siteType,"
                --and entity_id = :publisherId
              group by
                conversions.publisher_user_id
          ) min_date_by_publisher_user_id
        on deduped_conversion.publisher_user_id = min_date_by_publisher_user_id.publisher_user_id
        and deduped_conversion.requested_at > min_date_by_publisher_user_id.min_any_conversion
        group by
          deduped_conversion.publisher_user_id
      ) conversion
    on conversion.publisher_user_id = ad_call.publisher_user_id
    ----------------------
    ----clicks -----------
    ----------------------
    left join
      (
        select
          ac.publisher_user_id,
          case when count(*) > 0 then 'CLICKED' else 'NOT_CLICKED' end as clicked_type,
          sum(c.actual_cpc) as actual_cpc_sum
        from intent_media_log_data_production.clicks c
        join intent_media_log_data_production.ad_calls ac on c.ad_call_request_id = ac.request_id
        where c.fraudulent = 0
          and c.ip_address_blacklisted =0
          and ac.requested_at_date_in_et >= '2014-09-01'
          and ac.requested_at_date_in_et <= '2014-09-01'
          and c.requested_at_date_in_et  >= '2014-09-01'
          and c.requested_at_date_in_et  <= (date('2014-09-01') + interval '1 day')
          and ac.ip_address_blacklisted = 0
          and ac.site_type = ", siteType,"
          and ad_unit_type = ", adType,"
          --and ac.publisher_id = :publisherId
          and ac.product_category_type like :productCategoryType
        group by
          ac.publisher_user_id
      ) click
    on click.publisher_user_id = ad_call.publisher_user_id
    group by
      click.clicked_type,
      ad_call.site_reporting_value_01,
      ad_call.site_reporting_value_02,
      ad_call.site_reporting_value_03,
      ad_call.site_reporting_value_04,
      ad_call.site_reporting_value_05,
      ad_call.site_reporting_value_06,
      ad_call.site_reporting_value_07,
      ad_call.pure_group_type,
      ad_call.user_segmentation_type,
      ad_call.model_slice_id,
      ad_call.treatment", sep="")

    query_object <- dbGetQuery(verticaLogProduction, test_query)
    timestamp <- strftime(Sys.time(), "%Y%m%d%H%M%S")
    end_time <- Sys.time()
    print(end_time - start_time)
    print("Finished!")
}

c13_for_slices(siteType, productCategoryType, adType, adUnit)
