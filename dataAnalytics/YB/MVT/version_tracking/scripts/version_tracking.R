#!/usr/bin/env Rscript

#Load Libraries
install_package <- function(package) {
  if (!require(package, character.only = TRUE)) {
    cran = 'http://cran.us.r-project.org';
    install.packages(package, repos=cran);
    library(package);
  }
}

packages <- list('rjson', 'gtools', 'plyr', 'RJDBC')

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

MVT_Version_Tracking <- function() {

    print("MVT version tracking process initiated...")
    start_time <- Sys.time()

    mvt_attributes <- dbGetQuery(mysqlProduction,
        "
            SELECT
            	mta.id AS multivariate_test_attribute_id,
                mta.name AS multivariate_test_attribute_name,
                mta.site_id,
                s.display_name,
                mta.ad_type
            FROM multivariate_test_attributes mta
            LEFT JOIN sites s ON mta.site_id = s.id
        ")
    ind <- seq(1, nrow(mvt_attributes), 1)
    mvt_attributes <- cbind(ind, mvt_attributes)
    print("MVT Attribute Table Load Completed")

    mvt_versions <- dbGetQuery(mysqlProduction,
        "
            SELECT
                date(mv.created_at) AS change_date,
                s.display_name AS site_type,
                (CASE mv.ad_type
                    WHEN 'CT' then 'SCA'
                    WHEN 'SSR' then 'SSN'
                    WHEN 'META' then 'PPA'
                    WHEN 'RETARGETING' then 'Retargeting'
                    ELSE 'Other'
                END) as ad_type,
                mv.id AS multivariate_version_id,
                mv.description AS multivariate_version_description,
                mv.multivariate_test_attributes_values_json
            FROM multivariate_versions mv
            INNER JOIN users u ON mv.user_id = u.id
            INNER JOIN sites s ON mv.site_id = s.id
        ");
    ind <- seq(1, nrow(mvt_versions), 1)
    mvt_versions <- cbind(ind, mvt_versions)
    print("MVT Versions Table Load Completed")

    mvt_versions_json <- data.frame(matrix(nrow=1,ncol=9))
    colnames(mvt_versions_json)<-c("date", "month", "year", "site", "ad_type", "attribute", "multivariate_version_id", "value", "weight")

    #Loop through the list of MVT Versions table
    for(i in 1:nrow(mvt_versions)) {
        date <- mvt_versions[i,2]
        month <- substr(date,6,7)
        year <- substr(date,1,4)
        site <- mvt_versions[i,3]
        ad_type <- mvt_versions[i,4]
        multivariate_version_id <- mvt_versions[i,5]
        raw_json <- fromJSON(mvt_versions[i,7])
        if (length(raw_json) > 0) {
            for(j in 1:length(raw_json)) {
                experiment_json<-raw_json[[j]]
                if(length(experiment_json) > 0) {
                    for(k in 1:length(experiment_json)) {
                        value_json <- experiment_json[[k]]
                        if(!is.null(value_json$multivariate_test_attribute_id) && !is.null(value_json$id) && !is.null(value_json$value) && !is.null(value_json$weight) && date == substr(value_json$updated_at,1,10)) {
                            attribute <- mvt_attributes[mvt_attributes$id == value_json$multivariate_test_attribute_id,]$name
                            mvt_versions_json <- rbind(mvt_versions_json, c(date, month, year, site, ad_type, attribute, multivariate_version_id, value_json$value, value_json$weight))
                        }
                    }
                }
            }
        }
    }

    mvt_versions_json <- mvt_versions_json[-1,]
    mvt_versions_json <- unique(mvt_versions_json)

    #Output to CSV
    timestamp <- strftime(Sys.time(), "%Y%m%d%H%M%S")
    csvPath <- paste(paste("~/Desktop/mvt_version_tracking", timestamp, sep="_"), ".csv",sep="")
    write.csv(mvt_versions_json, file = csvPath, row.names=FALSE)
    print("MVT version tracking process completed.")
    end_time <- Sys.time()
    print(end_time - start_time)
    print("Finished!")
}

MVT_Version_Tracking()
