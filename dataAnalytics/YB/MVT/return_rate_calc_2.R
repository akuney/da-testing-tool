#!/usr/bin/env Rscript

#Load Libraries
install_package <- function(package) {
  if (!require(package, character.only = TRUE)) {
    cran = 'http://cran.us.r-project.org';
    install.packages(package, repos=cran);
    require(package);
  }
}

packages <- list('rjson', 'gtools', 'plyr', 'RJDBC')

for(i in 1:length(packages)){
  install_package(packages[[i]]);
}

#Read .ad_hoc_mvt.properties to load the database credential
credential <- scan(file="~/.adhoc_mvt.properties", what="character")
username <- credential[3]
password <- credential[6]

#Load DB Connection Information
scriptDir <- getwd();
mDriverPath <- paste(scriptDir, "drivers", "mysql-connector-java-5.1.31-bin.jar", sep="/");
vDriverPath <- paste(scriptDir, "drivers", "vertica-jdk5-6.1.3-0.jar", sep="/");

mDriver <- JDBC(driverClass="com.mysql.jdbc.Driver", classPath=mDriverPath);
mysqlProduction <- dbConnect(mDriver,"jdbc:mysql://production-slave-db-server-1.internal.intentmedia.net:3306/intent_media_production?SearchPath=intent_media_production",username,password);

vDriver <- JDBC(driverClass="com.vertica.jdbc.Driver", classPath=vDriverPath);
verticaProduction <- dbConnect(vDriver,"jdbc:vertica://production-vertica-cluster-1.internal.intentmedia.net:5433/intent_media?SearchPath=intent_media_production",username,password);
verticaLogProduction <- dbConnect(vDriver,"jdbc:vertica://production-vertica-cluster-1.internal.intentmedia.net:5433/intent_media?SearchPath=intent_media_log_data_production",username,password);

#Input Arrays
siteType <- c("ORBITZ_GLOBAL", "EBOOKERS", "EXPEDIA", "EXPEDIA", "KAYAK_UK", "LASTMINUTE_DOT_COM", "CHEAPTICKETS", "BOOKIT", "TRAVELZOO", "TRAVELZOO")
productCategoryType <- c("FLIGHTS", "FLIGHTS", "FLIGHTS", "HOTELS", "FLIGHTS", "HOTELS", "FLIGHTS", "HOTELS", "FLIGHTS", "HOTELS")
timeWindow <- 14

#Result DataFrame
out <- as.data.frame(matrix(0, 140, 16))
colnames(out) <- c("Site", "Product Category Type", "Test Window", "Date", "Total Visitors", "Return Rate", "Total Ad Calls", "Ad Call Ratio", "Total Interactions", "Interaction Ratio",
                   "Total Clicks", "Click Ratio", "Total Revenue", "Revenue Ratio", "Total Conversions", "Conversion Ratio")

Return_Rate_Calc <- function() {

    print("return rate calculation initiated...")
    start_time <- Sys.time()

    count <- 1
    iter <- 0
    for(i in 1:length(siteType)) {
        for(j in 1:length(timeWindow)) {
            for(k in 14:(13+timeWindow[j])) {
                # single day UV for duration of n days where n = timeWindow
                entire_populations <- dbGetQuery(verticaLogProduction, paste(
                    "
                        select
                            acc.requested_at_date_in_et,
                            count(acc.publisher_user_id) as users,
                            sum(served_ad_calls) as served_ad_calls,
                            sum(interactions) as interactions,
                            sum(clicks) as clicks,
                            sum(revenue) as revenue,
                            sum(conversions) as conversions
                        from
                        (
                            select
                                ac_c.requested_at_date_in_et,
                                ac_c.publisher_user_id,
                                count(ac_c.request_id) as served_ad_calls,
                                sum(interactions) as interactions,
                                sum(clicks) as clicks,
                                sum(revenue) as revenue
                            from
                            (
                                select
                                    min(ac.requested_at_date_in_et) as requested_at_date_in_et,
                                    min(ac.publisher_user_id) as publisher_user_id,
                                    ac.request_id,
                                    max(interactions) as interactions,
                                    sum(clicks) as clicks,
                                    sum(revenue) as revenue
                                from intent_media_log_data_production.ad_calls ac
                                left join
                                (
                                    select
                                        ad_call_request_id,
                                        requested_at_in_et,
                                        case when count(request_id) > 0 then 1 else 0 end as interactions,
                                        count(request_id) as clicks,
                                        sum(actual_cpc) as revenue
                                    from intent_media_log_data_production.clicks
                                    where requested_at_date_in_et between date(current_timestamp at timezone 'America/New_York') - interval '", k + 1, " days' and date(current_timestamp at timezone 'America/New_York') - interval '", k, " days'
                                        and ip_address_blacklisted = 0
                                        and fraudulent = 0
                                        and site_type = '", siteType[i], "'
                                        and product_category_type = '", productCategoryType[i], "'
                                    group by
                                        ad_call_request_id,
                                        requested_at_in_et
                                ) c
                                on ac.request_id = c.ad_call_request_id
                                and ac.requested_at_in_et < c.requested_at_in_et
                                and ac.requested_at_in_et + interval '24 hours' >= c.requested_at_in_et
                                where ac.requested_at_date_in_et = date(current_timestamp at timezone 'America/New_York') - interval '", k + 1, " days'
                                    and ac.ip_address_blacklisted = 0
                                    and ac.outcome_type = 'SERVED'
                                    and ac.site_type = '", siteType[i], "'
                                    and ac.product_category_type = '", productCategoryType[i], "'
                                    and ac.ad_unit_type = 'CT'
                                    and ac.publisher_user_id is not null
                                group by
                                    ac.request_id
                            ) ac_c
                          group by
                              ac_c.requested_at_date_in_et,
                              ac_c.publisher_user_id
                        ) acc
                        left join
                        (
                            select
                                publisher_user_id,
                                count(request_id) as conversions
                            from intent_media_log_data_production.conversions
                            where requested_at_date_in_et between date(current_timestamp at timezone 'America/New_York') - interval '", k + 1, " days' and date(current_timestamp at timezone 'America/New_York') - interval '", k - 14, " days'
                                and ip_address_blacklisted = 0
                                and site_type = '", siteType[i], "'
                            group by
                                publisher_user_id
                        ) con
                        on acc.publisher_user_id = con.publisher_user_id
                        group by
                            acc.requested_at_date_in_et
                    ",sep=""))

                return_populations <- dbGetQuery(verticaLogProduction, paste(
                    "
                        select
                            acc.requested_at_date_in_et,
                            count(acc.publisher_user_id) as users,
                            sum(served_ad_calls) as served_ad_calls,
                            sum(interactions) as interactions,
                            sum(clicks) as clicks,
                            sum(revenue) as revenue,
                            sum(conversions) as conversions
                        from
                        (
                            select
                                ac_c.requested_at_date_in_et,
                                ac_c.publisher_user_id,
                                count(ac_c.request_id) as served_ad_calls,
                                sum(interactions) as interactions,
                                sum(clicks) as clicks,
                                sum(revenue) as revenue
                            from
                            (
                                select
                                    min(ac.requested_at_date_in_et) as requested_at_date_in_et,
                                    min(ac.publisher_user_id) as publisher_user_id,
                                    ac.request_id,
                                    max(interactions) as interactions,
                                    sum(clicks) as clicks,
                                    sum(revenue) as revenue
                                from intent_media_log_data_production.ad_calls ac
                                left join
                                (
                                    select
                                        ad_call_request_id,
                                        requested_at_in_et,
                                        case when count(request_id) > 0 then 1 else 0 end as interactions,
                                        count(request_id) as clicks,
                                        sum(actual_cpc) as revenue
                                    from intent_media_log_data_production.clicks
                                    where requested_at_date_in_et between date(current_timestamp at timezone 'America/New_York') - interval '", k + 1, " days' and date(current_timestamp at timezone 'America/New_York') - interval '", k, " days'
                                        and ip_address_blacklisted = 0
                                        and fraudulent = 0
                                        and site_type = '", siteType[i], "'
                                        and product_category_type = '", productCategoryType[i], "'
                                    group by
                                        ad_call_request_id,
                                        requested_at_in_et
                                ) c
                                on ac.request_id = c.ad_call_request_id
                                and ac.requested_at_in_et < c.requested_at_in_et
                                and ac.requested_at_in_et + interval '24 hours' >= c.requested_at_in_et
                                where ac.requested_at_date_in_et = date(current_timestamp at timezone 'America/New_York') - interval '", k + 1, " days'
                                    and ac.ip_address_blacklisted = 0
                                    and ac.outcome_type = 'SERVED'
                                    and ac.site_type = '", siteType[i], "'
                                    and ac.product_category_type = '", productCategoryType[i], "'
                                    and ac.ad_unit_type = 'CT'
                                    and ac.publisher_user_id is not null
                                group by
                                    ac.request_id
                            ) ac_c
                          group by
                              ac_c.requested_at_date_in_et,
                              ac_c.publisher_user_id
                        ) acc
                        inner join
                        (
                            select
                                t.publisher_user_id
                            from
                            (
                                select distinct publisher_user_id
                                from intent_media_log_data_production.ad_calls
                                where requested_at_date_in_et = date(current_timestamp at timezone 'America/New_York') - interval '", k + 1, " days'
                                    and ip_address_blacklisted = 0
                                    and outcome_type = 'SERVED'
                                    and site_type = '", siteType[i], "'
                                    and product_category_type = '", productCategoryType[i], "'
                                    and ad_unit_type = 'CT'
                                    and publisher_user_id is not null
                            ) lag
                            inner join
                            (
                                select distinct publisher_user_id
                                from intent_media_log_data_production.ad_calls
                                where requested_at_date_in_et = date(current_timestamp at timezone 'America/New_York') - interval '", k, " days'
                                    and ip_address_blacklisted = 0
                                    and outcome_type = 'SERVED'
                                    and site_type = '", siteType[i], "'
                                    and product_category_type = '", productCategoryType[i], "'
                                    and ad_unit_type = 'CT'
                                    and publisher_user_id is not null
                            ) t
                            on lag.publisher_user_id = t.publisher_user_id
                        ) r
                        on acc.publisher_user_id = r.publisher_user_id
                        left join
                        (
                            select
                                publisher_user_id,
                                count(request_id) as conversions
                            from intent_media_log_data_production.conversions
                            where requested_at_date_in_et between date(current_timestamp at timezone 'America/New_York') - interval '", k + 1, " days' and date(current_timestamp at timezone 'America/New_York') - interval '", k - 14, " days'
                                and ip_address_blacklisted = 0
                                and site_type = '", siteType[i], "'
                            group by
                                publisher_user_id
                        ) con
                        on acc.publisher_user_id = con.publisher_user_id
                        group by
                            acc.requested_at_date_in_et
                    ",sep=""))

                out[count, 1] <- siteType[i]
                out[count, 2] <- productCategoryType[i]
                out[count, 3] <- timeWindow
                out[count, 4] <- entire_populations[1, 1]
                out[count, 5] <- entire_populations[1, 2]
                out[count, 6] <- return_populations[1, 2] / entire_populations[1, 2]
                out[count, 7] <- entire_populations[1, 3]
                out[count, 8] <- return_populations[1, 3] / entire_populations[1, 3]
                out[count, 9] <- entire_populations[1, 4]
                out[count, 10] <- return_populations[1, 4] / entire_populations[1, 4]
                out[count, 11] <- entire_populations[1, 5]
                out[count, 12] <- return_populations[1, 5] / entire_populations[1, 5]
                out[count, 13] <- entire_populations[1, 6]
                out[count, 14] <- return_populations[1, 6] / entire_populations[1, 6]
                out[count, 15] <- entire_populations[1, 7]
                out[count, 16] <- return_populations[1, 7] / entire_populations[1, 7]
                count <- count + 1
                print(paste("inner loop completed: ", k-13, " steps"))
            }
        }
    print(paste("outer loop completed: ", i, " steps"))
    }

    #Output to CSV
    timestamp <- strftime(Sys.time(), "%Y%m%d%H%M%S")
    csvPath <- paste(paste("~/Desktop/return_rate_calc", timestamp, sep="_"), ".csv",sep="")
    write.csv(out, file = csvPath, row.names=FALSE)
    print("return rate calculation completed.")
    end_time <- Sys.time()
    print(end_time - start_time)
    print("Finished!")
}

Return_Rate_Calc()
