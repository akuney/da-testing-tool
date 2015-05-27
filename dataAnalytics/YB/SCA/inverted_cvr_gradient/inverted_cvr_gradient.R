#Add has_repeat_clicks / num_other_clicks

#Load Libraries
library(RJDBC)
library(rpart)
library(randomForest)

#Load DB Connecion Information
vDriver <- JDBC(driverClass="com.vertica.jdbc.Driver",classPath="/Users/yoojong.bang/Documents/IM_Vertica/vertica-jdbc-7.0.1-0.jar")
verticaSandboxProduction <- dbConnect(vDriver,
    "jdbc:vertica://production-vertica-cluster-with-failover.internal.intentmedia.net:5433/intent_media?SearchPath=intent_media_sandbox_production?ConnectionLoadBalance=1",
    "yoojong.bang", "cjswoBYJ1!")

#Load training set and test set using random assignment
raw_train_data_expedia <- dbGetQuery(verticaSandboxProduction, "select * from intent_media_sandbox_production.YB_Inverted_CVR_final where advertiser_id = 59777 and random <= 0.1")
raw_train_data_orbitz <- dbGetQuery(verticaSandboxProduction, "select * from intent_media_sandbox_production.YB_Inverted_CVR_final where advertiser_id = 61224 and random <= 0.1")
raw_train_data_cheaptickets <- dbGetQuery(verticaSandboxProduction, "select * from intent_media_sandbox_production.YB_Inverted_CVR_final where advertiser_id = 106574 and random <= 0.1")
raw_test_data_expedia <- dbGetQuery(verticaSandboxProduction, "select * from intent_media_sandbox_production.YB_Inverted_CVR_final where advertiser_id = 59777 and random > 0.1 and random <= 0.2")
raw_test_data_orbitz <- dbGetQuery(verticaSandboxProduction, "select * from intent_media_sandbox_production.YB_Inverted_CVR_final where advertiser_id = 61224 and random > 0.1 and random <= 0.2")
raw_test_data_cheaptickets <- dbGetQuery(verticaSandboxProduction, "select * from intent_media_sandbox_production.YB_Inverted_CVR_final where advertiser_id = 106574 and random > 0.1 and random <= 0.2")

#Subset dataset for linear regression
train_data_expedia <- raw_train_data_expedia[,c(2,3,5:12,14:17,19,26)]
train_data_expedia <- raw_train_data_expedia[,c(2,3,5:12,14:17,19,25)]
train_data_orbitz <- raw_train_data_orbitz[,c(2,3,5:12,14:17,19,26)]
train_data_cheaptickets <- raw_train_data_cheaptickets[,c(2,3,5:12,14:17,19,26)]
test_data_expedia <- raw_test_data_expedia[,c(2,3,5:12,14:17,19,26)]
test_data_orbitz <- raw_test_data_orbitz[,c(2,3,5:12,14:17,19,26)]
test_data_cheaptickets <- raw_test_data_cheaptickets[,c(2,3,5:12,14:17,19,26)]

#Convert datatype to factor for those categorical variables
for(i in names(train_data_expedia)) {
  if(!(i %in% c("click_count", "conversion_count_total"))){
    train_data_expedia[,i] <- as.factor(train_data_expedia[,i])
    test_data_expedia[,i] <- as.factor(test_data_expedia[,i])
    train_data_orbitz[,i] <- as.factor(train_data_orbitz[,i])
    test_data_orbitz[,i] <- as.factor(test_data_orbitz[,i])
    train_data_cheaptickets[,i] <- as.factor(train_data_cheaptickets[,i])
    test_data_cheaptickets[,i] <- as.factor(test_data_cheaptickets[,i])
  }
}

train_data$conversion_boolean <- ceiling(train_data$conversion_count_total / (train_data$conversion_count_total + 1))
train_data$conversion_boolean[is.na(train_data$conversion_boolean)] <- 0
test_data$conversion_boolean <- ceiling(test_data$conversion_count_total / (test_data$conversion_count_total + 1))
test_data$conversion_boolean[is.na(test_data$conversion_boolean)] <- 0

#Fit logistic regression on the training set
lm_base <- glm(conversion_count_boolean ~ .,train_data_expedia,family="binomial")
summary(lm_base)
lm <- lm(conversion_count_total ~ .,train_data_expedia)
summary(lm)
#Fit classification tree model
clt_base_expedia <- rpart(conversion_count_boolean ~ .,train_data_expedia,method="class")
clt_base_pruned <- prune(clt_base,cp=clt_base$cptable[which.min(clt_base$cptable[,"xerror"]),"CP"])
summary(clt_base_expedia)
printcp(clt_base)
plotcp(clt_base)
plot(clt_base,uniform=TRUE,main="Classification Tree for Conversion_Boolean")
text(clt_base,use.n=TRUE,all=TRUE,cex=0.8)
plot(clt_base_pruned,uniform=TRUE,main="Pruned Classification Tree for Conversion_Boolean")
text(clt_base_pruned,use.n=TRUE,all=TRUE,cex=0.8)

#Fit again to the test set
clt_test <- rpart(conversion_boolean ~ .-conversion_count_total,test_data,method="class")
clt_test_pruned <- prune(clt_test,cp=clt_test$cptable[which.min(clt_test$cptable[,"xerror"]),"CP"])
summary(clt_test)
printcp(clt_test)
plotcp(clt_test)
plot(clt_test,uniform=TRUE,main="Classification Tree for Conversion_Boolean (Test set)")
text(clt_test,use.n=TRUE,all=TRUE,cex=0.8)
plot(clt_test_pruned,uniform=TRUE,main="Pruned Classification Tree for Conversion_Boolean (Test set)")
text(clt_test_pruned,use.n=TRUE,all=TRUE,cex=0.8)

#Subset based on browser_family
for(i in levels(train_data$browser_family)) {
  subset_name <- paste("train_data",i,sep="_")
  assign(subset_name,train_data[train_data$browser_family==i,])
}

#Fit new subsets with classification tree model and see whether the fit deviates by browser_family
for(i in levels(train_data$browser_family)) {
  model_name <- paste("clt_base",i,sep="_")
  assign(model_name,rpart(conversion_boolean ~ . -conversion_count_total, data=eval(parse(text=paste("train_data","CHROME",sep="_"))), method="class"))
}

summary(clt_base_IE)
summary(clt_base_FIREFOX)


clf <- randomForest(conversion_count_boolean~.,train_data_expedia,na.action=na.omit)
table(test_data_expedia$conversion_count_boolean, predict(clf, test_data_expedia))