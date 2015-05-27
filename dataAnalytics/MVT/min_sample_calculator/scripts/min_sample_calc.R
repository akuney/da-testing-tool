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

#Function to read in query and output the result in csv
siteType <- commandArgs(TRUE)[1]
adUnitType <- commandArgs(TRUE)[2]
productCategoryType <- commandArgs(TRUE)[3]

timeWindow <- 14

#Result DataFrame
out_return <- as.data.frame(matrix(0, 14, 14))
colnames(out_return) <- c("Site", "Product Category Type", "Test Window", "Date", "Total Visitors", "Return Rate", "Total Ad Calls", "Ad Call Ratio", "Total Interactions", "Interaction Ratio",
                   "Total Clicks", "Click Ratio", "Total Revenue", "Revenue Ratio")

Min_Sample_Calculator <- function(siteType, adUnitType, productCategoryType) {

    print("data extraction process initiated...")
    start_time <- Sys.time()

    query <- paste(
        "
            select
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
              ac.publisher_user_id,
              count(ac.request_id) as served_ad_calls,
              sum(c.interactions) as interactions,
              sum(c.interactions) * sum(c.interactions) as interactions_2_u,
              sum(c.clicks) as clicks,
              sum(c.clicks_2_ac) as clicks_2_ac,
              sum(c.clicks) * sum(c.clicks) as clicks_2_u,
              sum(c.revenue) as revenue,
              sum(c.revenue_2_ac) as revenue_2_ac,
              sum(c.revenue) * sum(c.revenue) as revenue_2_u
            from intent_media_log_data_production.ad_calls ac
            left join
            (
              select
                ad_call_request_id,
                case when count(request_id) > 0 then 1 else 0 end as interactions,
                count(request_id) as clicks,
                count(request_id) * count(request_id) as clicks_2_ac,
                sum(actual_cpc) as revenue,
                sum(actual_cpc) * sum(actual_cpc) as revenue_2_ac
              from intent_media_log_data_production.clicks
              where requested_at_date_in_et >= date(current_timestamp at timezone 'America/New_York') - interval '14 days'
                and ip_address_blacklisted = 0
                and fraudulent = 0
                and site_type = '", siteType, "'
                and product_category_type = '", productCategoryType, "'
              group by
                ad_call_request_id
            ) c
            on ac.request_id = c.ad_call_request_id
            where ac.requested_at_date_in_et >= date(current_timestamp at timezone 'America/New_York') - interval '14 days'
              and ac.ip_address_blacklisted = 0
              and ac.site_type = '", siteType, "'
              and ac.product_category_type = '", productCategoryType, "'
              and ac.ad_unit_type = '", adUnitType, "'
              and ac.outcome_type = 'SERVED'
            group by
              ac.publisher_user_id
            ) per_user
        ", sep="")

    metrics <- dbGetQuery(verticaLogProduction, query)

    # Get information for metrics
    IR <-  metrics$interactions / metrics$served_ad_calls
    IR_VAR <- IR * (1-IR)
    IR_STDEV <- sqrt(IR_VAR / metrics$served_ad_calls)

    IPU <- metrics$interactions / metrics$users
    IPU_VAR <- (metrics$interactions_2_u / metrics$users) - IPU^2
    IPU_STDEV <- sqrt(IPU_VAR / metrics$users)

    CTR <- metrics$clicks / metrics$served_ad_calls
    CTR_VAR <- (metrics$clicks_2_ac / metrics$served_ad_calls) - CTR^2
    CTR_STDEV <- sqrt(CTR_VAR / metrics$served_ad_calls)

    CPU <- metrics$clicks / metrics$users
    CPU_VAR <- (metrics$clicks_2_u / metrics$users) - CPU^2
    CPU_STDEV <- sqrt(CPU_VAR / metrics$users)

    RPAC <- metrics$revenue / metrics$served_ad_calls
    RPAC_VAR <- (metrics$revenue_2_ac / metrics$served_ad_calls) - RPAC^2
    RPAC_STDEV <- sqrt(RPAC_VAR / metrics$served_ad_calls)

    RPU <- metrics$revenue / metrics$users
    RPU_VAR <- (metrics$revenue_2_u / metrics$users) - RPU^2
    RPU_STDEV <- sqrt(RPU_VAR / metrics$users)

    count <- 1
    for(i in 1:timeWindow) {
        # single day UV for duration of n days where n = timeWindow
        entire_populations <- dbGetQuery(verticaLogProduction, paste(
            "
                select
                    acc.requested_at_date_in_et,
                    count(acc.publisher_user_id) as users,
                    sum(served_ad_calls) as served_ad_calls,
                    sum(interactions) as interactions,
                    sum(clicks) as clicks,
                    sum(revenue) as revenue
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
                            case when count(c.request_id) > 0 then 1 else 0 end as interactions,
                            count(c.request_id) as clicks,
                            sum(c.actual_cpc) as revenue
                        from intent_media_log_data_production.ad_calls ac
                        left join intent_media_log_data_production.clicks c
                        on ac.request_id = c.ad_call_request_id
                        and ac.requested_at_in_et < c.requested_at_in_et
                        and ac.requested_at_in_et + interval '24 hours' >= c.requested_at_in_et
                        where ac.requested_at_date_in_et = date(current_timestamp at timezone 'America/New_York') - interval '", i + 1, " days'
                            and ac.ip_address_blacklisted = 0
                            and ac.outcome_type = 'SERVED'
                            and ac.site_type = '", siteType, "'
                            and ac.product_category_type = '", productCategoryType, "'
                            and ac.ad_unit_type = '", adUnitType, "'
                            and ac.publisher_user_id is not null
                            and (c.requested_at_date_in_et is null or c.requested_at_date_in_et between date(current_timestamp at timezone 'America/New_York') - interval '", i + 1, " days' and date(current_timestamp at timezone 'America/New_York') - interval '", i, " days')
                            and (c.ip_address_blacklisted is null or c.ip_address_blacklisted = 0)
                            and (c.fraudulent is null or c.fraudulent = 0)
                            and (c.site_type is null or c.site_type = '", siteType, "')
                            and (c.product_category_type is null or c.product_category_type = '", productCategoryType, "')
                        group by
                            ac.request_id
                    ) ac_c
                  group by
                      ac_c.requested_at_date_in_et,
                      ac_c.publisher_user_id
                ) acc
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
                    sum(revenue) as revenue
                from
                (
                    select
                        requested_at_date_in_et,
                        publisher_user_id,
                        count(request_id) as served_ad_calls,
                        sum(interactions) as interactions,
                        sum(clicks) as clicks,
                        sum(revenue) as revenue
                    from
                    (
                        select
                            min(ac.requested_at_date_in_et) as requested_at_date_in_et,
                            min(ac.publisher_user_id) as publisher_user_id,
                            ac.request_id,
                            case when count(c.request_id) > 0 then 1 else 0 end as interactions,
                            count(c.request_id) as clicks,
                            sum(c.actual_cpc) as revenue
                        from intent_media_log_data_production.ad_calls ac
                        left join intent_media_log_data_production.clicks c
                        on ac.request_id = c.ad_call_request_id
                        and ac.requested_at_in_et < c.requested_at_in_et
                        and ac.requested_at_in_et + interval '24 hours' >= c.requested_at_in_et
                        where ac.requested_at_date_in_et = date(current_timestamp at timezone 'America/New_York') - interval '", i + 1, " days'
                            and ac.ip_address_blacklisted = 0
                            and ac.outcome_type = 'SERVED'
                            and ac.site_type = '", siteType, "'
                            and ac.product_category_type = '", productCategoryType, "'
                            and ac.ad_unit_type = '", adUnitType, "'
                            and ac.publisher_user_id is not null
                            and (c.requested_at_date_in_et is null or c.requested_at_date_in_et between date(current_timestamp at timezone 'America/New_York') - interval '", i + 1, " days' and date(current_timestamp at timezone 'America/New_York') - interval '", i, " days')
                            and (c.ip_address_blacklisted is null or c.ip_address_blacklisted = 0)
                            and (c.fraudulent is null or c.fraudulent = 0)
                            and (c.site_type is null or c.site_type = '", siteType, "')
                            and (c.product_category_type is null or c.product_category_type = '", productCategoryType, "')
                        group by
                            ac.request_id
                    ) ac_c
                    group by
                        requested_at_date_in_et,
                        publisher_user_id
                ) acc
                inner join
                (
                    select
                        t.publisher_user_id
                    from
                    (
                        select distinct publisher_user_id
                        from intent_media_log_data_production.ad_calls
                        where requested_at_date_in_et = date(current_timestamp at timezone 'America/New_York') - interval '", i + 1, " days'
                            and ip_address_blacklisted = 0
                            and outcome_type = 'SERVED'
                            and site_type = '", siteType, "'
                            and product_category_type = '", productCategoryType, "'
                            and ad_unit_type = '", adUnitType, "'
                            and publisher_user_id is not null
                    ) lag
                    inner join
                    (
                        select distinct publisher_user_id
                        from intent_media_log_data_production.ad_calls
                        where requested_at_date_in_et = date(current_timestamp at timezone 'America/New_York') - interval '", i, " days'
                            and ip_address_blacklisted = 0
                            and outcome_type = 'SERVED'
                            and site_type = '", siteType, "'
                            and product_category_type = '", productCategoryType, "'
                            and ad_unit_type = '", adUnitType, "'
                            and publisher_user_id is not null
                    ) t
                    on lag.publisher_user_id = t.publisher_user_id
                ) r
                on acc.publisher_user_id = r.publisher_user_id
                group by
                    acc.requested_at_date_in_et
            ",sep=""))

        out_return[count, 1] <- siteType
        out_return[count, 2] <- productCategoryType
        out_return[count, 3] <- timeWindow
        out_return[count, 4] <- entire_populations[1, 1]
        out_return[count, 5] <- entire_populations[1, 2]
        out_return[count, 6] <- return_populations[1, 2] / entire_populations[1, 2]
        out_return[count, 7] <- entire_populations[1, 3]
        out_return[count, 8] <- return_populations[1, 3] / entire_populations[1, 3]
        out_return[count, 9] <- entire_populations[1, 4]
        out_return[count, 10] <- return_populations[1, 4] / entire_populations[1, 4]
        out_return[count, 11] <- entire_populations[1, 5]
        out_return[count, 12] <- return_populations[1, 5] / entire_populations[1, 5]
        out_return[count, 13] <- entire_populations[1, 6]
        out_return[count, 14] <- return_populations[1, 6] / entire_populations[1, 6]
        count <- count + 1
        print(paste("loop completed: ", i, " steps"))
    }

    REV_AVG <- mean(out_return[,14])

    out <- matrix(0,21,2)

    out[,1] <- c("Users","Served Ad Calls","IR","IR Var","IR Stdev","IPU","IPU Var","IPU Stdev",
                "CTR","CTR Var","CTR Stdev","CPU","CPU Var","CPU Stdev","RPAC","RPAC Var","RPAC Stdev",
                "RPU","RPU Var","RPU Stdev", "Revenue Ratio")

    out[,2] <- c(metrics$users,metrics$served_ad_calls,IR,IR_VAR,IR_STDEV,IPU,IPU_VAR,IPU_STDEV,
                CTR,CTR_VAR,CTR_STDEV,CPU,CPU_VAR,CPU_STDEV,RPAC,RPAC_VAR,RPAC_STDEV,RPU,RPU_VAR,RPU_STDEV,REV_AVG)

    out <- as.data.frame(out)
    colnames(out) <- c("Metrics", "Values")

    print("data extraction process completed...")

    #Output to CSV
    timestamp <- strftime(Sys.time(), "%Y%m%d%H%M%S")
    csvPath <- paste(paste("~/Desktop/min_sample_calc", siteType, adUnitType, productCategoryType, timestamp, sep="_"), ".csv",sep="")
    print(paste("Writing CSV to", csvPath, sep=" "))
    write.csv(out, file = csvPath, row.names=FALSE)
    end_time <- Sys.time()
    print(end_time - start_time)
    print("minimum sample size estimation process completed...")

}

Min_Sample_Calculator(siteType, adUnitType, productCategoryType)
