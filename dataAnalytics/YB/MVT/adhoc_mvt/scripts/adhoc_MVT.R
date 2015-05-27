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
verticaLogProduction <- dbConnect(vDriver,"jdbc:vertica://production-vertica-cluster-with-failover.internal.intentmedia.net:5433/intent_media?ConnectionLoadBalance=1?SearchPath=intent_media_log_data_production",username,password)

#Function to read in query and output the result in csv
site <- commandArgs(TRUE)[1]
adUnitType <- commandArgs(TRUE)[2]
productCategoryType <- commandArgs(TRUE)[3]
attribute <- commandArgs(TRUE)[4]
startMVTVersion <- commandArgs(TRUE)[5]
endMVTVersion <- commandArgs(TRUE)[6]
pubSettingsActive <- commandArgs(TRUE)[7]
placeholderAttribute <- commandArgs(TRUE)[8]

Adhoc_MVT <- function(site, adUnitType, productCategoryType, attribute, startMVTVersion, endMVTVersion, pubSettingsActive, placeholderAttribute) {

    print("MVT query processing...")
    start_time <- Sys.time()

    mvt_query_with_SF <- paste(
    "
        select
            ifnull(sfe.page_type,acc.page_type) as page_type,
            ifnull(sfe.mvt_value,acc.mvt_value) as mvt_value,
            ifnull(sfe.browser_family,acc.browser_family) as browser_family,
            case ifnull(sfe.browser_family,acc.browser_family) when 'CHROME' then 'Single' when 'IE' then 'Single' else 'Multi' end as click_type,
            acc.placement_type,
            count(sfe.publisher_user_id) as users,
            sum(sfe.page_load_count) as page_load_count,
            sum(acc.served_ad_calls) as served_ad_calls,
            sum(acc.served_ad_calls / sfe.page_load_count) as sum_form_ir,
            sum((acc.served_ad_calls / sfe.page_load_count) * (acc.served_ad_calls / sfe.page_load_count)) as sum_form_ir_2_u,
            sum(acc.interactions) as interactions,
            sum(acc.interactions * acc.interactions) as interactions_2_u,
            sum(acc.clicks) as clicks,
            sum(acc.clicks_2_ac) as clicks_2_ac,
            sum(acc.clicks_2_u) as clicks_2_u,
            sum(acc.revenue) as revenue,
            sum(acc.revenue_2_ac) as revenue_2_ac,
            sum(acc.revenue_2_u) as revenue_2_u
        from
        (
            select
                lpt.page_type,
                (case
                    when ", pubSettingsActive," then null
                    else ifnull(trim(trailing ", "'", '"', "'", " from regexp_substr(sfe.multivariate_test_attributes_variable, '", '"', attribute, '"', ":", '"', "(.*?", '"', ")[,}]", "', 1, 1, '', 1)), 'Not Found')
                end) as mvt_value,
                sfe.publisher_user_id,
                sfe.requested_at_in_et,
                min(sfe.browser_family) as browser_family,
                count(sfe.request_id) as page_load_count
            from intent_media_log_data_production.search_compare_form_events sfe
            left join intent_media_production.ad_units au on sfe.ad_unit_id = au.id
            left join intent_media_production.legal_page_types lpt on au.legal_page_type_id = lpt.id
            where sfe.multivariate_version_id >= ", startMVTVersion, "
                and (case when ", endMVTVersion," is null then true else sfe.multivariate_version_id < ", endMVTVersion, " end)
                and sfe.ip_address_blacklisted = 0
                and sfe.site_type = '", site, "'
                and sfe.multivariate_test_attributes_variable like '%", attribute, "%'
                and sfe.product_category_type = '", productCategoryType,"'
                and sfe.publisher_user_id is not null
                and sfe.publisher_user_id not in
                (
                    select publisher_user_id
                    from intent_media_log_data_production.search_compare_form_events
                    where multivariate_version_id >= ", startMVTVersion, "
                        and (case when ", endMVTVersion," is null then true else multivariate_version_id < ", endMVTVersion, " end)
                        and ip_address_blacklisted = 0
                        and site_type = '", site, "'
                        and multivariate_test_attributes_variable like '%", attribute, "%'
                        and product_category_type = '", productCategoryType,"'
                        and publisher_user_id is not null
                    group by publisher_user_id
                    having count(distinct browser_family) > 1
                )
            group by
                lpt.page_type,
                (case
                    when ", pubSettingsActive," then null
                    else ifnull(trim(trailing ", "'", '"', "'", " from regexp_substr(sfe.multivariate_test_attributes_variable, '", '"', attribute, '"', ":", '"', "(.*?", '"', ")[,}]", "', 1, 1, '', 1)), 'Not Found')
                end),
                sfe.publisher_user_id,
                sfe.requested_at_in_et
        ) sfe
        full outer join
        (
            select
                lpt.page_type,
                (case
                    when ", pubSettingsActive," then ", placeholderAttribute,"
                    else ifnull(trim(trailing ", "'", '"', "'", " from regexp_substr(ac.multivariate_test_attributes_variable, '", '"', attribute, '"', ":", '"', "(.*?", '"', ")[,}]", "', 1, 1, '', 1)), 'Not Found')
                end) as mvt_value,
                ac.publisher_user_id,
                ac.requested_at_in_et,
                min(ac.browser_family) as browser_family,
                case when count(distinct c.placement_type) > 1 then 'Mixed' else min(c.placement_type) end as placement_type,
                (case
                    when '", adUnitType,"' = 'CT' then count(ac.request_id)
                    when '", adUnitType,"' = 'SSR' then count(case when ac.positions_filled > 0 then ac.request_id end)
                    when '", adUnitType,"' = 'META' then count(distinct ac.request_correlation_id)
                end) as served_ad_calls,
                sum(c.interactions) as interactions,
                sum(c.interactions * c.interactions) as interactions_2_u,
                sum(c.clicks) as clicks,
                sum(c.clicks_2_ac) as clicks_2_ac,
                sum(c.clicks * c.clicks) as clicks_2_u,
                sum(c.revenue) as revenue,
                sum(c.revenue_2_ac) as revenue_2_ac,
                sum(c.revenue * c.revenue) as revenue_2_u
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
                where multivariate_version_id >= ", startMVTVersion, "
                    and (case when ", endMVTVersion," is null then true else multivariate_version_id < ", endMVTVersion, " end)
                    and publisher_user_id is not null
                    and ip_address_blacklisted = 0
                    and fraudulent = 0
                    and site_type = '", site, "'
                    and product_category_type = '", productCategoryType,"'
                group by
                    ad_call_request_id
            ) c
            on ac.request_id = c.ad_call_request_id
            where ac.multivariate_version_id >= ", startMVTVersion, "
                and (case when ", endMVTVersion," is null then true else ac.multivariate_version_id < ", endMVTVersion, " end)
                and ac.ip_address_blacklisted = 0
                and ac.outcome_type = 'SERVED'
                and ac.site_type = '", site, "'
                and ac.ad_unit_type = '", adUnitType, "'
                and ac.product_category_type = '", productCategoryType,"'
                and ac.multivariate_test_attributes_variable like '%", attribute, "%'
                and ac.publisher_user_id is not null
                and ac.publisher_user_id not in
                (
                    select publisher_user_id
                    from intent_media_log_data_production.ad_calls
                    where multivariate_version_id >= ", startMVTVersion, "
                        and (case when ", endMVTVersion," is null then true else multivariate_version_id < ", endMVTVersion, " end)
                        and ip_address_blacklisted = 0
                        and outcome_type = 'SERVED'
                        and site_type = '", site, "'
                        and ad_unit_type = '", adUnitType, "'
                        and product_category_type = '", productCategoryType,"'
                        and multivariate_test_attributes_variable like '%", attribute, "%'
                        and publisher_user_id is not null
                    group by publisher_user_id
                    having count(distinct browser_family) > 1
                )
            group by
                lpt.page_type,
                (case
                    when ", pubSettingsActive," then ", placeholderAttribute,"
                    else ifnull(trim(trailing ", "'", '"', "'", " from regexp_substr(ac.multivariate_test_attributes_variable, '", '"', attribute, '"', ":", '"', "(.*?", '"', ")[,}]", "', 1, 1, '', 1)), 'Not Found')
                end),
                ac.publisher_user_id,
                ac.requested_at_in_et
        ) acc
        on sfe.page_type = acc.page_type
        and sfe.mvt_value = acc.mvt_value
        and sfe.publisher_user_id = acc.publisher_user_id
        and sfe.browser_family = acc.browser_family
        and sfe.requested_at_in_et <= acc.requested_at_in_et
        group by
            ifnull(sfe.page_type,acc.page_type),
            ifnull(sfe.mvt_value,acc.mvt_value),
            ifnull(sfe.browser_family,acc.browser_family),
            case ifnull(sfe.browser_family,acc.browser_family) when 'CHROME' then 'Single' when 'IE' then 'Single' else 'Multi' end,
            acc.placement_type
    ", sep="")

    mvt_query_without_SF <- paste(
    "
        select
            page_type,
            mvt_value,
            browser_family,
            case browser_family when 'CHROME' then 'Single' when 'IE' then 'Single' else 'Multi' end as click_type,
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
                    when ", pubSettingsActive," then ", placeholderAttribute,"
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
                 where multivariate_version_id >= ", startMVTVersion,"
                     and (case when ", endMVTVersion," is null then true else multivariate_version_id < ", endMVTVersion," end)
                     and ip_address_blacklisted = 0
                     and fraudulent = 0
                     and site_type = '", site, "'
                     and product_category_type = '", productCategoryType,"'
                 group by
                     ad_call_request_id
            ) c
            on ac.request_id = c.ad_call_request_id
            where ac.multivariate_version_id >= ", startMVTVersion,"
                 and (case when ", endMVTVersion," is null then true else ac.multivariate_version_id < ", endMVTVersion," end)
                 and ac.ip_address_blacklisted = 0
                 and ac.outcome_type = 'SERVED'
                 and ac.site_type = '", site,"'
                 and ac.ad_unit_type = '", adUnitType,"'
                 and ac.product_category_type = '", productCategoryType,"'
                 and ac.multivariate_test_attributes_variable like '%", attribute,"%'
                 and ac.publisher_user_id is not null
                 and ac.publisher_user_id not in
                    (
                        select publisher_user_id
                        from intent_media_log_data_production.ad_calls
                        where multivariate_version_id >= ", startMVTVersion,"
                            and (case when ", endMVTVersion," is null then true else multivariate_version_id < ", endMVTVersion," end)
                            and ip_address_blacklisted = 0
                            and outcome_type = 'SERVED'
                            and site_type = '", site,"'
                            and ad_unit_type = '", adUnitType,"'
                            and product_category_type = '", productCategoryType,"'
                            and multivariate_test_attributes_variable like '%", attribute,"%'
                            and publisher_user_id is not null
                        group by publisher_user_id
                        having count(distinct browser_family) > 1
                    )
            group by
                lpt.page_type,
                (case
                    when ", pubSettingsActive," then ", placeholderAttribute,"
                    else ifnull(trim(trailing ", "'", '"', "'"," from regexp_substr(ac.multivariate_test_attributes_variable, '", '"', attribute, '"', ":", '"', "(.*?", '"', ")[,}]", "', 1, 1, '', 1)), 'Not Found')
                end),
                ac.publisher_user_id
        ) acc
        group by
            page_type,
            mvt_value,
            browser_family,
            case browser_family when 'CHROME' then 'Single' when 'IE' then 'Single' else 'Multi' end,
            placement_type
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

    ### Deprecated as we do regex in SQL
    # Remove stuff from mis-parsed output, such as blank spaces and quotation mark
#    for(i in 1:nrow(mvt_query)) {
#        mvt_query[i,3] <- gsub('"', '', mvt_query[i,3])
#        mvt_query[i,3] <- gsub('^\\s+|\\s+$', '', mvt_query[i,3])
#    }

    print("Adhoc MVT Table Load Completed")

    #Output to CSV
    timestamp <- strftime(Sys.time(), "%Y%m%d%H%M%S")
    csvPath <- paste(paste("~/Desktop/adhoc_mvt", site, attribute, timestamp, sep="_"), ".csv",sep="")
    print(paste("Writing CSV to", csvPath, sep=" "))
    write.csv(mvt_query, file = csvPath, row.names=FALSE)
    print("Adhoc MVT processing completed.")
    end_time <- Sys.time()
    print(end_time - start_time)
    print("Finished!")
}

Adhoc_MVT(site, adUnitType, productCategoryType, attribute, startMVTVersion, endMVTVersion, pubSettingsActive, placeholderAttribute)
