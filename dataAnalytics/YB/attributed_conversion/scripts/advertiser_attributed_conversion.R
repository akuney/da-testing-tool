#!/usr/bin/env Rscript

#Load Libraries
install_package <- function(package) {
  if (!require(package, character.only = TRUE)) {
    cran = 'http://cran.us.r-project.org';
    install.packages(package, repos=cran);
    library(package);
  }
}

packages <- list('rjson', 'gtools', 'plyr', 'RJDBC', 'MASS', 'car')

for(i in 1:length(packages)){
  install_package(packages[[i]]);
}

#Read .ad_hoc_mvt.properties to load the database credential
credential <- scan(file="~/.adhoc_mvt.properties", what="character");
username <- credential[3];
password <- credential[6];

#Load DB Connection Information
scriptDir <- getwd();
mDriverPath <- paste(scriptDir, "drivers", "mysql-connector-java-5.1.31-bin.jar", sep="/");
vDriverPath <- paste(scriptDir, "drivers", "vertica-jdk5-6.1.3-0.jar", sep="/");

mDriver <- JDBC(driverClass="com.mysql.jdbc.Driver", classPath=mDriverPath);
mysqlProduction <- dbConnect(mDriver,"jdbc:mysql://production-slave-db-server-1.internal.intentmedia.net:3306/intent_media_production?SearchPath=intent_media_production",username,password);

vDriver <- JDBC(driverClass="com.vertica.jdbc.Driver", classPath=vDriverPath);
verticaProduction <- dbConnect(vDriver,"jdbc:vertica://production-vertica-cluster-1.internal.intentmedia.net:5433/intent_media?SearchPath=intent_media_production",username,password);
verticaLogProduction <- dbConnect(vDriver,"jdbc:vertica://production-vertica-cluster-1.internal.intentmedia.net:5433/intent_media?SearchPath=intent_media_log_data_production",username,password);
verticaSandboxProduction <- dbConnect(vDriver,"jdbc:vertica://production-vertica-cluster-1.internal.intentmedia.net:5433/intent_media?SearchPath=intent_media_sandbox_production",username,password);

Advertiser_Attributed_Conversion <- function() {

    print("Advertiser Attributed Conversion process initiated...")
    start_time <- Sys.time()

    #Meta Advertisers that we need to exclude from the analysis
    #148139 - Momondo
    #136872 - Skyscanner - UK
    #178168 - Skyscanner - US
    #137541 - Trivago
    #150934 - Trivago - UK
    #XXXXXX - Fly.com

#    raw_data <- dbGetQuery(verticaSandboxProduction,
#        "
#            select
#                auction_position,
#                ifnull(actual_cpc,0) as actual_cpc,
#                ifnull(conversion_count_total,0) as conversion_count_total,
#                ifnull(conversion_value_sum_total,0) as conversion_value_sum_total,
#                publisher_id,
#                site_id,
#                ad_unit_id,
#                advertiser_id,
#                ad_call_product_category_type,
#                device_family,
#                browser_family,
#                os_family,
#                segmentation_score
#            from intent_media_sandbox_production.YB_ad_call_click_conversion
#        ")

    raw_data <- dbGetQuery(verticaSandboxProduction,
            "
                select
                  1 as clicks,
                  ifnull(c.actual_cpc,0) as actual_cpc,
                  ifnull(c.conversion_count_total,0) as conversion_count_total,
                  ifnull(c.conversion_value_sum_total,0) as conversion_value_sum_total,
                  c.publisher_id,
                  e_pub.name as publisher_name,
                  c.site_id,
                  s.display_name as site_name,
                  c.ad_unit_id,
                  au.name as ad_unit_name,
                  c.advertiser_id,
                  e_adv.name as advertiser_name,
                  c.ad_call_product_category_type,
                  c.device_family,
                  c.browser_family,
                  c.os_family,
                  c.segmentation_score,
                  c.placement_type
                from intent_media_sandbox_production.YB_ad_call_click_conversion c
                inner join intent_media_production.entities e_adv on c.advertiser_id = e_adv.id
                inner join intent_media_production.entities e_pub on c.publisher_id = e_pub.id
                inner join intent_media_production.ad_units au on c.ad_unit_id = au.id
                inner join intent_media_production.sites s on c.site_id = s.id
            ")

    table_load_time <- Sys.time()
    print(table_load_time - start_time)

    print(summary(raw_data))
    print(paste("Total number of rows: ", nrow(raw_data)))
    print(paste("Complete number of rows: ", nrow(raw_data[complete.cases(raw_data),])))

    # Take a subset of data
    no_null_data <- na.omit(raw_data)
    print(summary(no_null_data$conversion_count_total))

    # Linear Regression Model to predict the count of conversion
    print("Linear Regression process initiated")

    flights_data <- raw_data[raw_data$ad_call_product_category_type == "FLIGHTS",]
    lm_base_flights <- lm(conversion_count_total ~ actual_cpc + publisher_name + site_name + ad_unit_name + advertiser_name + device_family + browser_family + os_family + segmentation_score + placement_type, flights_data)
    print(summary(lm_base_flights))
#    #step <- stepAIC(lm_base, direction="both")
#    #print(step$anova)
#
#    lm_stepAIC <- lm(conversion_count_total ~ . - conversion_value_sum_total - publisher_id, data)
#    print(summary(lm_stepAIC))
#    # Normality of Residuals
#        # qq plot for studentized resid
#        print(qqPlot(lm_stepAIC, main="QQ Plot"))
#        # distribution of studentized residuals
#        print(hist(studres(lm_stepAIC), freq=FALSE, main="Distribution of Studentized Residuals"))
#        xfit<-seq(min(studres(lm_stepAIC)), max(studres(lm_stepAIC)), length=40)
#        yfit<-dnorm(xfit)
#        print(lines(xfit, yfit))
#    # Non-constant error variance
#    print(ncvTest(lm_stepAIC))
#    # Plot studentized residuals vs. fitted values
#    print(spreadLevelPlot(lm_stepAIC))
#    # Test for multi-collinearity
#    print(vif(lm_stepAIC))
#    # Test for autocorrelated errors
#    print(durbinWatsonTest(lm_stepAIC))


    print("Linear Regression process completed")

    print("Advertiser Attributed Conversion process completed.")
    end_time <- Sys.time()
    print(end_time - start_time)
    print("Finished!")

}

Advertiser_Attributed_Conversion()
