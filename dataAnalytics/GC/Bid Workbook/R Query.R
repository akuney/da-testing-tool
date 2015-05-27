## Set working directory and libraries
setwd("/Users/gregoire.crepy/Desktop/WIP/Bid Workbook")
source('R Raw Query.R')
library(RJDBC)
library(glmnet)
library(rpart)
#library(MASS)
#library(klaR)



## Sets connection and dates for the query
vDriver <- JDBC(driverClass="com.vertica.jdbc.Driver", classPath="/Users/gregoire.crepy/Documents/vertica-jdbc-7.0.1-0.jar")
verticaProduction <- dbConnect(vDriver,"jdbc:vertica://production-vertica-cluster-1.internal.intentmedia.net:5433/intent_media?SearchPath=intent_media_sandbox_production","gregoire.crepy","Uibuu1ni")
start_date<-"'2014-12-15'"
end_date<-"'2015-02-15'"

## Runs the queries and stores the output in the DF 'out'
print("Query processing...")
start_time <- Sys.time()
summ
end_time <- Sys.time()
print(end_time - start_time)
print("Finished!")

out<-dbGetQuery(verticaProduction,"select * from intent_media_sandbox_production.GC_BWB_Final limit 10000")


## Prepares matrixes for storing results
out[,'Conversion']<-ifelse(out$conversion_value_sum_total>0,"Yes","No")
n<-dim(out)[1]
results.ridge.mat<-matrix(,nrow=9,ncol=3)
results.lasso.mat<-matrix(,nrow=9,ncol=3)
results.gini.mat<-matrix(,nrow=9,ncol=3)
results.nb.mat<-matrix(,nrow=9,ncol=3)

col.labels<-c('False Positive','False Negative','Correct Classification')
colnames(results.ridge.mat)<-col.labels
colnames(results.lasso.mat)<-col.labels
colnames(results.gini.mat)<-col.labels
colnames(results.nb.mat)<-col.labels

## Prepares source data frame
#df.model<-out[sapply(out, class)=="numeric"]
df.model<-out[,setdiff(c(1:23,41),c(1,6,12:15,19:20))]
df.model[is.na(df.model[,'Conversion']),'Conversion']<-'No'
df.model[is.na(df.model)]<-0
for(i in setdiff(1:13,c(6))) {
  df.model[,i]<-as.factor(df.model[,i])
}
## Loops for different sizes of training set
for(j in 5:5){
## Defines testing/training set
#train.length=round(n*j/10)
#train.set<-sample(1:n,train.length)
#test.set<-setdiff(1:n,train.set)
df.train<-df.model[df.model[,'Period']=="Train",]
df.test<-df.model[df.model[,'Period']=="Test",]

## Scales the data
#df.train<-scale(df.train,center=TRUE,scale=TRUE)
#df.train[is.na(df.train)]<-0
#df.test<-scale(df.test,center=TRUE,scale=TRUE)
#df.test[is.na(df.test)]<-0

## Stores inputs/outputs
variables.train<-df.train[,-13]
variables.glmnet.train<-model.matrix(Conversion~.,data=df.train)
labels.train<-as.factor(df.train[,'Conversion'])
variables.test<-df.test[,-13]
variables.glmnet.test<-model.matrix(Conversion~.,data=df.test)
labels.test<-as.factor(df.test[,'Conversion'])

### Fits a lasso and ridge regression model on the training data
cv.lasso<-cv.glmnet(x=variables.glmnet.train,y=labels.train,family="binomial",alpha=1,type.measure="class",standardize=FALSE)
cv.ridge<-cv.glmnet(x=variables.glmnet.train,y=labels.train,family="binomial",alpha=0,type.measure="class",standardize=FALSE)
#lambda.lasso<-cv.lasso$lambda.min
#lambda.ridge<-cv.ridge$lambda.min
#model.lasso<-glmnet(x=variables.glmnet.train,y=labels.train,family="binomial",alpha=1)
#model.ridge<-glmnet(x=variables.glmnet.train,y=labels.train,family="binomial",alpha=0)
result.lasso<-predict(cv.lasso,variables.glmnet.test,type="class")
result.ridge<-predict(cv.ridge,variables.glmnet.test,type="class")

### Fits a tree classification
model.gini<-rpart(Conversion ~ .,data=df.train,method="class",parms=list(split = 'gini'))
result.gini<-as.matrix(predict(model.gini,df.test,type="class"))
#model.info<-rpart(Conversion ~ .,data=df.train,method="class",parms=list(split = 'gini'))
#result.info<-predict(model.info,df.test,type="class")

### Fits a naive bayes model
model.nb<-NaiveBayes(Conversion~.,data=df.train)
result.nb<-as.matrix(predict(model.nb,variables.test,type="class")$class)

results.ridge.mat[j,1]=mean(result.ridge[labels.test=="No"]=="Yes")
results.ridge.mat[j,2]=mean(result.ridge[labels.test=="Yes"]=="No")
results.ridge.mat[j,3]=mean(result.ridge==labels.test)
results.lasso.mat[j,1]=mean(result.lasso[labels.test=="No"]=="Yes")
results.lasso.mat[j,2]=mean(result.lasso[labels.test=="Yes"]=="No")
results.lasso.mat[j,3]=mean(result.lasso==labels.test)
results.gini.mat[j,1]=mean(result.gini[labels.test=="No"]=="Yes")
results.gini.mat[j,2]=mean(result.gini[labels.test=="Yes"]=="No")
results.gini.mat[j,3]=mean(result.gini==labels.test)
results.nb.mat[j,1]=mean(result.nb[labels.test=="No"]=="Yes")
results.nb.mat[j,2]=mean(result.nb[labels.test=="Yes"]=="No")
results.nb.mat[j,3]=mean(result.nb==labels.test)
}