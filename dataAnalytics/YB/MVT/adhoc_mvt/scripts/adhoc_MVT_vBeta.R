#!/usr/bin/env Rscript

#Load Libraries
install_package <- function(package) {
  if (!require(package, character.only = TRUE)) {
    cran = 'http://cran.us.r-project.org'
    install.packages(package, repos=cran)
    require(package)
  }
}

packages <- list('rjson', 'gtools', 'plyr', 'RJDBC')

for(i in 1:length(packages)){
  install_package(packages[[i]])
}

#Read .ad_hoc_mvt.properties to load the database credential
credential <- scan(file="~/.adhoc_mvt.properties", what="character")
username <- credential[3]
password <- credential[6]

#Load DB Connection Information
scriptDir <- getwd()
vDriverPath <- paste(scriptDir, "drivers", "vertica-jdbc-7.0.1-0.jar", sep="/")
vDriver <- JDBC(driverClass="com.vertica.jdbc.Driver", classPath=vDriverPath)
verticaProduction <- dbConnect(vDriver,"jdbc:vertica://production-vertica-cluster-with-failover.internal.intentmedia.net:5433/intent_media?ConnectionLoadBalance=1?SearchPath=intent_media_production",username,password)

#Function to read in query and output the result in csv
site <- commandArgs(TRUE)[1]
adUnitType <- commandArgs(TRUE)[2]
productCategoryType <- commandArgs(TRUE)[3]
attribute <- commandArgs(TRUE)[4]
startPointer <- commandArgs(TRUE)[5]
endPointer <- commandArgs(TRUE)[6]
pubSettingsActive <- commandArgs(TRUE)[7]
placeholderAttribute <- commandArgs(TRUE)[8]

Adhoc_MVT <- function(site, adUnitType, productCategoryType, attribute, startPointer, endPointer, pubSettingsActive, placeholderAttribute) {

    print("MVT query processing...")
    start_time <- Sys.time()

    if(as.character(pubSettingsActive) == 'TRUE') {
        startMVTVersion <- 0
        endMVTVersion <- 0
        startDate <- startPointer
        if(as.character(endPointer) == 'NULL'){
            endDate <- strftime(Sys.time(), "%Y-%m-%d")
        } else {
            endDate <- endPointer
        }
        pubAttribute <- paste("ac.", placeholderAttribute,"",sep="")
    } else {
        startMVTVersion <- startPointer
        endMVTVersion <- endPointer
        startDate <- '2011-01-01'
        endDate <- '2011-01-01'
        pubAttribute <- placeholderAttribute
    }
    if(as.character(productCategoryType) == 'ALL') {
        pathProductCategoryType <- '%'
    } else {
        pathProductCategoryType <- productCategoryType
    }

    mvt_query_with_SF <- paste(
    "
        select
          page_type,
          browser_family,
          placement_type,
          mvt_value,
          case when ac_publisher_user_id is null then 'without_ad_call' else 'with_ad_call' end as ac_flag,
          count(sfe_publisher_user_id) as users,
          sum(page_loads) as page_loads,
          sum(page_loads_2_u) as page_loads_2_u,
          sum(served_ad_calls) as served_ad_calls,
          sum(served_ad_calls_2_sfe) as served_ad_calls_2_sfe,
          sum(served_ad_calls_2_u) as served_ad_calls_2_u,
          sum(interactions) as interactions,
          sum(interactions_2_sfe) as interactions_2_sfe,
          sum(interactions_2_u) as interactions_2_u,
          sum(clicks) as clicks,
          sum(clicks_2_ac) as clicks_2_ac,
          sum(clicks_2_sfe) as clicks_2_sfe,
          sum(clicks_2_u) as clicks_2_u,
          sum(revenue) as revenue,
          sum(revenue_2_ac) as revenue_2_ac,
          sum(revenue_2_sfe) as revenue_2_sfe,
          sum(revenue_2_u) as revenue_2_u
        from
        (
          select
            mvt_value,
            placement_type,
            sfe_publisher_user_id,
            ac_publisher_user_id,
            page_type,
            browser_family,
            count(sfe_request_id) as page_loads,
            count(sfe_request_id)^2 as page_loads_2_u,
            sum(served_ad_calls) as served_ad_calls,
            sum(served_ad_calls_2_sfe) as served_ad_calls_2_sfe,
            sum(served_ad_calls)^2 as served_ad_calls_2_u,
            sum(interactions) as interactions,
            sum(interactions_2_sfe) as interactions_2_sfe,
            sum(interactions)^2 as interactions_2_u,
            sum(clicks) as clicks,
            sum(clicks_2_ac) as clicks_2_ac,
            sum(clicks_2_sfe) as clicks_2_sfe,
            sum(clicks)^2 as clicks_2_u,
            sum(revenue) as revenue,
            sum(revenue_2_ac) as revenue_2_ac,
            sum(revenue_2_sfe) as revenue_2_sfe,
            sum(revenue)^2 as revenue_2_u
          from
          (
            select
              mvt_value,
              placement_type,
              sfe_publisher_user_id,
              ac_publisher_user_id,
              sfe_request_id,
              min(page_type) as page_type,
              min(browser_family) as browser_family,
              (case
                when '", adUnitType,"' = 'CT' then count(ac_request_id)
                when '", adUnitType,"' = 'SSR' then count(case when ac_positions_filled > 0 then ac_request_id end)
                when '", adUnitType,"' = 'META' then count(distinct ac_request_correlation_id)
              end) as served_ad_calls,
              (case
                when '", adUnitType,"' = 'CT' then count(ac_request_id)
                when '", adUnitType,"' = 'SSR' then count(case when ac_positions_filled > 0 then ac_request_id end)
                when '", adUnitType,"' = 'META' then count(distinct ac_request_correlation_id)
              end)^2 as served_ad_calls_2_sfe,
              sum(interactions) as interactions,
              sum(interactions)^2 as interactions_2_sfe,
              sum(clicks) as clicks,
              sum(clicks_2_ac) as clicks_2_ac,
              sum(clicks)^2 as clicks_2_sfe,
              sum(revenue) as revenue,
              sum(revenue_2_ac) as revenue_2_ac,
              sum(revenue)^2 as revenue_2_sfe
            from
            (
              select
                mvt_value,
                placement_type,
                sfe_publisher_user_id,
                ac_publisher_user_id,
                sfe_request_id,
                ac_request_id,
                min(page_type) as page_type,
                min(browser_family) as browser_family,
                min(ac_positions_filled) as ac_positions_filled,
                min(ac_request_correlation_id) as ac_request_correlation_id,
                count(distinct c_interaction_flag) as interactions,
                count(c_request_id) as clicks,
                count(c_request_id)^2 as clicks_2_ac,
                sum(actual_cpc) as revenue,
                sum(actual_cpc)^2 as revenue_2_ac
              from
              (
                select
                  (case
                    when ", pubSettingsActive," then ", pubAttribute,"
                    else ifnull(trim(trailing ", "'", '"', "'", " from regexp_substr(sfe.multivariate_test_attributes_variable, '", '"', attribute, '"', ":", '"', "(.*?", '"', ")[,}]", "', 1, 1, '', 1)), 'Not Found')
                  end) as mvt_value,
                  lpt.page_type,
                  c.placement_type,
                  sfe.browser_family,
                  sfe.publisher_user_id as sfe_publisher_user_id,
                  ac.publisher_user_id as ac_publisher_user_id,
                  sfe.request_id as sfe_request_id,
                  sfe.requested_at_in_et as sfe_timestamp,
                  ac.request_id as ac_request_id,
                  ac.request_correlation_id as ac_request_correlation_id,
                  ac.positions_filled as ac_positions_filled,
                  ac.requested_at_in_et as ac_timestamp,
                  c.ad_call_request_id as c_interaction_flag,
                  c.request_id as c_request_id,
                  c.actual_cpc,
                  c.requested_at_in_et as c_timestamp,
                  count(*) over(partition by sfe.publisher_user_id) as sfe_count,
                  count(*) over(partition by sfe.request_id) as ac_count,
                  rank() over(partition by sfe.publisher_user_id order by sfe.requested_at_in_et) as sfe_rank,
                  rank() over(partition by sfe.request_id order by ac.requested_at_in_et) as ac_rank
                from intent_media_log_data_production.search_compare_form_events sfe
                left join intent_media_production.ad_units au on sfe.ad_unit_id = au.id
                left join intent_media_production.legal_page_types lpt on au.legal_page_type_id = lpt.id
                left join intent_media_log_data_production.ad_calls ac
                on sfe.publisher_user_id = ac.publisher_user_id
                and sfe.requested_at_in_et <= ac.requested_at_in_et
                and ac.ip_address_blacklisted = 0
                and ac.outcome_type = 'SERVED'
                and ac.ad_unit_type = '", adUnitType, "'
                and ac.site_type = '", site, "'
                and ac.product_category_type like '", pathProductCategoryType, "'
                and (case when ", pubSettingsActive ," then true else ac.multivariate_version_id >= ", startMVTVersion," end)
                and (case when ", pubSettingsActive ," then true when cast(", endMVTVersion," as numeric) is null then true else ac.multivariate_version_id < ", endMVTVersion," end)
                and (case when not(", pubSettingsActive ,") then true else ac.requested_at_date_in_et >= '", startDate,"' end)
                and (case when not(", pubSettingsActive ,") then true when cast('", endDate,"' as date) is null then true else ac.requested_at_date_in_et < '", endDate,"' end)
                left join intent_media_log_data_production.clicks c
                on ac.request_id = c.ad_call_request_id
                and c.ip_address_blacklisted = 0
                and c.fraudulent = 0
                and c.site_type = '", site, "'
                and c.product_category_type like '", pathProductCategoryType, "'
                and (case when ", pubSettingsActive ," then true else c.multivariate_version_id >= ", startMVTVersion," end)
                and (case when ", pubSettingsActive ," then true when cast(", endMVTVersion," as numeric) is null then true else c.multivariate_version_id < ", endMVTVersion," end)
                and (case when not(", pubSettingsActive ,") then true else c.requested_at_date_in_et >= '", startDate,"' end)
                and (case when not(", pubSettingsActive ,") then true when cast('", endDate,"' as date) is null then true else c.requested_at_date_in_et < '", endDate,"' end)
                where sfe.ip_address_blacklisted = 0
                  and sfe.site_type = '", site, "'
                  and sfe.product_category_type like '", pathProductCategoryType, "'
                  and (case when ", pubSettingsActive ," then true else sfe.multivariate_version_id >= ", startMVTVersion," end)
                  and (case when ", pubSettingsActive ," then true when cast(", endMVTVersion," as numeric) is null then true else sfe.multivariate_version_id < ", endMVTVersion," end)
                  and (case when not(", pubSettingsActive ,") then true else sfe.requested_at_date_in_et >= '", startDate,"' end)
                  and (case when not(", pubSettingsActive ,") then true when cast('", endDate,"' as date) is null then true else sfe.requested_at_date_in_et < '", endDate,"' end)
                  and sfe.publisher_user_id not in
                      (
                          select publisher_user_id
                          from intent_media_log_data_production.ad_calls
                          where ip_address_blacklisted = 0
                              and outcome_type = 'SERVED'
                              and site_type = '", site,"'
                              and ad_unit_type = '", adUnitType,"'
                              and product_category_type like '", pathProductCategoryType,"'
                              and publisher_user_id is not null
                              and (case when ", pubSettingsActive ," then true else multivariate_version_id >= ", startMVTVersion," end)
                              and (case when ", pubSettingsActive ," then true when cast(", endMVTVersion," as numeric) is null then true else multivariate_version_id < ", endMVTVersion," end)
                              and (case when not(", pubSettingsActive ,") then true else requested_at_date_in_et >= '", startDate,"' end)
                              and (case when not(", pubSettingsActive ,") then true when cast('", endDate,"' as date) is null then true else requested_at_date_in_et < '", endDate,"' end)
                          group by publisher_user_id
                          having count(distinct browser_family) > 1
                      )
              ) base
              where (case when sfe_count = ac_count then sfe_rank = 1 else ac_rank = 1 end)
              group by 1,2,3,4,5,6
            ) per_ac
            group by 1,2,3,4,5
          ) per_sfe
          group by 1,2,3,4,5,6
        ) per_user
        group by 1,2,3,4,5
    ", sep="")

    mvt_query_without_SF <- paste(
    "
        select
            page_type,
            mvt_value,
            browser_family,
            placement_type,
            count(publisher_user_id) as users,
            sum(served_ad_calls) as served_ad_calls,
            sum(interactions) as interactions,
            sum(interactions_2_u) as interactions_2_u,
            sum(clicks) as clicks,
            sum(clicks_2_ac) as clicks_2_ac,
            sum(clicks_2_u) as clicks_2_u,
            sum(revenue) as revenue,
            sum(revenue_2_ac) as revenue_2_ac,
            sum(revenue_2_u) as revenue_2_u
        from
        (
            select
                lpt.page_type,
                (case
                    when ", pubSettingsActive," then ", pubAttribute,"
                    else ifnull(trim(trailing ", "'", '"', "'", " from regexp_substr(ac.multivariate_test_attributes_variable, '", '"', attribute, '"', ":", '"', "(.*?", '"', ")[,}]", "', 1, 1, '', 1)), 'Not Found')
                end) as mvt_value,
                ac.publisher_user_id,
                min(ac.browser_family) as browser_family,
                case when count(distinct c.placement_type) > 1 then 'Mixed' else min(c.placement_type) end as placement_type,
                (case
                    when '", adUnitType,"' = 'CT' then count(ac.request_id)
                    when '", adUnitType,"' = 'SSR' then count(case when ac.positions_filled > 0 then ac.request_id end)
                    when '", adUnitType,"' = 'META' then count(distinct ac.request_correlation_id)
                end) as served_ad_calls,
                sum(interactions) as interactions,
                (sum(interactions) * sum(interactions)) as interactions_2_u,
                sum(clicks) as clicks,
                sum(clicks_2_ac) as clicks_2_ac,
                (sum(clicks) * sum(clicks)) as clicks_2_u,
                sum(revenue) as revenue,
                sum(revenue_2_ac) as revenue_2_ac,
                (sum(revenue) * sum(revenue)) as revenue_2_u
            from intent_media_log_data_production.ad_calls ac
            left join intent_media_production.ad_units au on ac.ad_unit_id = au.id
            left join intent_media_production.legal_page_types lpt on au.legal_page_type_id = lpt.id
            left join
            (
                 select
                     ad_call_request_id,
                     case when count(distinct placement_type) > 1 then 'Mixed' else min(placement_type) end as placement_type,
                     case when count(request_id) > 0 then 1 else 0 end as interactions,
                     count(request_id) as clicks,
                     (count(request_id) * count(request_id)) as clicks_2_ac,
                     sum(actual_cpc) as revenue,
                     (sum(actual_cpc) * sum(actual_cpc)) as revenue_2_ac
                 from intent_media_log_data_production.clicks
                 where ip_address_blacklisted = 0
                     and fraudulent = 0
                     and site_type = '", site, "'
                     and product_category_type like '", pathProductCategoryType,"'
                     -- we are an intent defined experiment: filter by multivariate versions
                     and (case when ", pubSettingsActive ," then true else multivariate_version_id >= ", startMVTVersion," end)
                     and (case when ", pubSettingsActive ," then true when cast(", endMVTVersion," as numeric) is null then true else multivariate_version_id < ", endMVTVersion," end)
                     -- we are a publisher defined experiment: filter by requested date
                     and (case when not(", pubSettingsActive ,") then true else requested_at_date_in_et >= '", startDate,"' end)
                     and (case when not(", pubSettingsActive ,") then true when cast('", endDate,"' as date) is null then true else requested_at_date_in_et < '", endDate,"' end)
                 group by
                     ad_call_request_id
            ) c
            on ac.request_id = c.ad_call_request_id
            where ac.ip_address_blacklisted = 0
                 and ac.outcome_type = 'SERVED'
                 and ac.site_type = '", site,"'
                 and ac.ad_unit_type = '", adUnitType,"'
                 and ac.product_category_type like '", pathProductCategoryType,"'
                 and ac.publisher_user_id is not null
                 -- we are an intent defined experiment: filter by multivariate versions
                 and (case when ", pubSettingsActive ," then true else ac.multivariate_version_id >= ", startMVTVersion," end)
                 and (case when ", pubSettingsActive ," then true when cast(", endMVTVersion," as numeric) is null then true else ac.multivariate_version_id < ", endMVTVersion," end)
                 -- we are a publisher defined experiment: filter by requested date
                 and (case when not(", pubSettingsActive ,") then true else ac.requested_at_date_in_et >= '", startDate,"' end)
                 and (case when not(", pubSettingsActive ,") then true when cast('", endDate,"' as date) is null then true else ac.requested_at_date_in_et < '", endDate,"' end)
                 and ac.publisher_user_id not in
                    (
                        select publisher_user_id
                        from intent_media_log_data_production.ad_calls
                        where ip_address_blacklisted = 0
                            and outcome_type = 'SERVED'
                            and site_type = '", site,"'
                            and ad_unit_type = '", adUnitType,"'
                            and product_category_type like '", pathProductCategoryType,"'
                            and publisher_user_id is not null
                            -- we are an intent defined experiment: filter by multivariate versions
                            and (case when ", pubSettingsActive ," then true else multivariate_version_id >= ", startMVTVersion," end)
                            and (case when ", pubSettingsActive ," then true when cast(", endMVTVersion," as numeric) is null then true else multivariate_version_id < ", endMVTVersion," end)
                            -- we are a publisher defined experiment: filter by requested date
                            and (case when not(", pubSettingsActive ,") then true else requested_at_date_in_et >= '", startDate,"' end)
                            and (case when not(", pubSettingsActive ,") then true when cast('", endDate,"' as date) is null then true else requested_at_date_in_et < '", endDate,"' end)
                        group by publisher_user_id
                        having count(distinct browser_family) > 1
                    )
            group by 1,2,3
        ) acc
        group by 1,2,3,4
    ", sep="")


    # Hardcoded rules to use which funnel metrics (include/exclude Form IR)
    if(length(grep("SEARCH_FORM", attribute)) > 0 || length(grep("FLIGHTS_EXIT_OVERLAY_TRIGGER", attribute)) > 0 || length(grep("SUPERSEARCH_DESIGN", attribute)) > 0 || length(grep("FLIGHTS_ADVERTISEMENT_SELECT_ALL_TEXT", attribute)) > 0) {
             print("Search Form Version Triggered")
             mvt_query <- dbGetQuery(verticaProduction, mvt_query_with_SF)
         } else {
             print("Non Search Form Version Triggered")
             mvt_query <- dbGetQuery(verticaProduction, mvt_query_without_SF)
         }

    # Create an exception when the query returns an empty result set
    if(nrow(mvt_query) == 0) stop("There are no results for your query. Please double check the input parameters and try again.")

    # Add an index in case we need to compute something in future
    ind <- seq(1, nrow(mvt_query), 1)
    mvt_query <- cbind(ind, mvt_query)

    ### Deprecated as we do regex in SQL ###
    # Remove stuff from mis-parsed output, such as blank spaces and quotation mark
#    for(i in 1:nrow(mvt_query)) {
#        mvt_query[i,3] <- gsub('"', '', mvt_query[i,3])
#        mvt_query[i,3] <- gsub('^\\s+|\\s+$', '', mvt_query[i,3])
#    }

    print("Adhoc MVT Table Load Completed")

    #Output to CSV
    timestamp <- strftime(Sys.time(), "%Y%m%d%H%M%S")
    if(as.character(pubSettingsActive) == 'TRUE') {
        csvPath <- paste(paste("~/Desktop/adhoc_mvt", site, placeholderAttribute, timestamp, sep="_"), ".csv",sep="")
    } else {
        csvPath <- paste(paste("~/Desktop/adhoc_mvt", site, attribute, timestamp, sep="_"), ".csv",sep="")
    }
    print(paste("Writing CSV to", csvPath, sep=" "))
    write.csv(mvt_query, file = csvPath, row.names=FALSE)
    print("Adhoc MVT processing completed.")
    end_time <- Sys.time()
    print(end_time - start_time)
    print("Finished!")
}

Adhoc_MVT(site, adUnitType, productCategoryType, attribute, startPointer, endPointer, pubSettingsActive, placeholderAttribute)
