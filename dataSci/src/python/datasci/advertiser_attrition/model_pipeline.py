import random
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.preprocessing import LabelEncoder
import os
import pylab as pl
import matplotlib.pyplot as plt
from sklearn.metrics import roc_curve, auc, confusion_matrix


def prepare_inputs(file_location):
    print "Preparing inputs ..."
    cv_data_split_share = 0.01
    df = pd.read_csv(file_location, header=None)
    df.columns = [# 0: month
                  'ADVERTISER_ID', 'MONTH', 'YEAR',
                  # 1 - 5: entity features
                  'EXCEEDED_CREDIT_THRESHOLD', 'PAYMENT_METHOD_ID', 'SPENDING CAP_ENABLED',
                  'ALLOW_ADS_ON_PACKAGE_PATH', 'SUSPENDED_FOR_NON_PAYMENT',
                  # 6: prior pause
                  'PRIOR_PAUSE_COUNT',
                  # 7: market
                  'INTENT_MEDIA_MARKET_ID',
                  # 8: report segment
                  'REPORT_SEGMENT_NAME',
                  # y-value
                  'Y_VALUE',
                  # ignore
                  'IGNORE_MONTH', 'IGNORE_YEAR', 'IGNORE_ADVERTISER_ID',
                  # 9 - 19: performance metrics
                  'IMPRESSIONS', 'CLICKS', 'CLICKED_CONVERSIONS', 'EXPOSED_CONVERSIONS',
                  'CLICKED_ROOM_NIGHTS', 'EXPOSED_ROOM_NIGHTS',
                  'CLICKED_CONVERSION_VALUE', 'EXPOSED_CONVERSION_VALUE',
                  'SPEND', 'CTR', 'ROI',
                  # 20 - 25: performance relative to past month
                  'IMPRESSIONS_RATIO', 'CLICKS_RATIO', 'CLICKED_CONVERSION_RATIO',
                  'SPEND_RATIO', 'CTR_RATIO', 'ROI_RATIO']

    df = pre_process(df)

    df_test = df[(df.MONTH == 1) & (df.YEAR == 2015)]
    df_train_all = df[~((df.MONTH == 1) & (df.YEAR == 2015))]

    cross_validation_rows = random.sample(df_train_all.index, int(len(df_train_all)*cv_data_split_share))
    df_cv = df_train_all.ix[cross_validation_rows]
    df_train = df_train_all.drop(cross_validation_rows)

    train_X = df_train.drop(['IGNORE_MONTH', 'IGNORE_YEAR', 'IGNORE_ADVERTISER_ID',
                             'ADVERTISER_ID', 'YEAR', 'Y_VALUE'], 1)
    test_X = df_test.drop(['IGNORE_MONTH', 'IGNORE_YEAR', 'IGNORE_ADVERTISER_ID',
                           'ADVERTISER_ID', 'YEAR', 'Y_VALUE'], 1)
    cv_X = df_cv.drop(['IGNORE_MONTH', 'IGNORE_YEAR', 'IGNORE_ADVERTISER_ID',
                           'ADVERTISER_ID', 'YEAR', 'Y_VALUE'], 1)

    train_y = df_train['Y_VALUE']
    test_y = df_test['Y_VALUE']
    cv_y = df_cv['Y_VALUE']

    test_adv = df_test['ADVERTISER_ID']
    cv_adv = df_cv['ADVERTISER_ID']


    return train_X, train_y, test_X, test_y, cv_X, cv_y, test_adv, cv_adv

def pre_process(df):
    print "Pre-processing ..."
    altered_df = df
    altered_df['PAYMENT_METHOD_ID'] = LabelEncoder().fit_transform(altered_df['PAYMENT_METHOD_ID'])
    altered_df['INTENT_MEDIA_MARKET_ID'] = LabelEncoder().fit_transform(altered_df['INTENT_MEDIA_MARKET_ID'])
    altered_df['REPORT_SEGMENT_NAME'] = LabelEncoder().fit_transform(altered_df['REPORT_SEGMENT_NAME'])
    # Un-comment line below to drop PRIOR_PAUSE_COUNT
    # altered_df = altered_df.drop('PRIOR_PAUSE_COUNT', 1)
    altered_df = altered_df.fillna(0)
    return altered_df

def fit_model(train_X, train_y, test_X, test_y, cv_X, cv_y, test_adv, cv_adv):
    print "Fitting random forest classifier ..."
    fit_random_forest(train_X, train_y, test_X, test_y, cv_X, cv_y, test_adv, cv_adv)
    # print "Fitting gradient boosting classifier ..."
    # fit_gradient_boost(train_X, train_y, test_X, test_y, cv_X, cv_y, test_adv, cv_adv)

def fit_random_forest(train_X, train_y, test_X, test_y, cv_X, cv_y, test_adv, cv_adv):
    rfclf = RandomForestClassifier(n_estimators=200, oob_score=True, max_features='sqrt',
                                   max_depth=20,  min_samples_split=10, n_jobs=6)
    rfclf.fit(train_X, train_y, sample_weight=np.array([5 if i == 1 else 1 for i in train_y]))
    importances = rfclf.feature_importances_
    std = np.std([tree.feature_importances_ for tree in rfclf.estimators_],
             axis=0)
    indices = np.argsort(importances)[::-1]

    # Print the feature ranking
    print("Feature ranking:")

    for f in range(len(importances)):
        print("%d. feature %d (%f)" % (f + 1, indices[f], importances[indices[f]]))
    # Plot the feature importances of the forest
    plt.figure()
    plt.title("Feature importances")
    plt.bar(range(len(importances)), importances[indices],
           color="r", yerr=std[indices], align="center")
    plt.xticks(range(len(importances)), indices)
    plt.xlim([-1, len(importances)])
    plt.show()

    print "Cross validating ..."
    preds_ = rfclf.predict(cv_X)
    probas_ = rfclf.predict_proba(cv_X)

    plot_confusion_matrix(cv_y, preds_)
    plot_roc(cv_y, probas_)

    print "Results..."
    preds_ = rfclf.predict(test_X)
    probas_ = rfclf.predict_proba(test_X)

    # print dict(zip(test_adv, probas_))

    plot_confusion_matrix(test_y, preds_)
    plot_roc(test_y, probas_)


def fit_gradient_boost(train_X, train_y, test_X, test_y, cv_X, cv_y, test_adv, cv_adv):
    gbclf = GradientBoostingClassifier(n_estimators=40, max_features='sqrt',
                                        max_depth=10,  min_samples_split=10)
    gbclf.fit(train_X, train_y)

    print gbclf.feature_importances_

    print "Cross validating ..."
    preds_ = gbclf.predict(cv_X)
    probas_ = gbclf.predict_proba(cv_X)

    plot_confusion_matrix(cv_y, preds_)
    plot_roc(cv_y, probas_)

    print "Results..."
    preds_ = gbclf.predict(test_X)
    probas_ = gbclf.predict_proba(test_X)

    # print dict(zip(test_adv, probas_))

    plot_confusion_matrix(test_y, preds_)
    plot_roc(test_y, probas_)

def plot_confusion_matrix(test_y, preds_):
    cm = confusion_matrix(test_y, preds_)

    print(cm)
    metrics = 'TPR: %0.2f' % ((cm[1][1]+0.0)/(cm[1][0]+cm[1][1])) + ' Precision: %0.2f' % ((cm[1][1]+0.0)/(cm[0][1]+cm[1][1]))

    # Show confusion matrix in a separate window
    cm = np.log10(cm)
    plt.matshow(cm, cmap=plt.cm.YlOrRd)
    plt.title('Confusion matrix (log10 colorscale)\n ' + metrics)
    plt.colorbar()
    plt.ylabel('True label')
    plt.xlabel('Predicted label')
    plt.show()

def plot_roc(test_y, probas_):
    # Compute ROC curve and area the curve
    fpr, tpr, thresholds = roc_curve(test_y, probas_[:, 1])
    roc_auc = auc(fpr, tpr)
    print "Area under the ROC curve : %f" % roc_auc

    # Plot ROC curve
    pl.clf()
    pl.plot(fpr, tpr, label='ROC curve (area = %0.2f)' % roc_auc)
    pl.plot([0, 1], [0, 1], 'k--')
    pl.xlim([0.0, 1.0])
    pl.ylim([0.0, 1.0])
    pl.xlabel('False Positive Rate')
    pl.ylabel('True Positive Rate')
    pl.title('Receiver operating characteristic\n')
    pl.legend(loc="lower right")
    pl.show()

if __name__ == '__main__':
    BASE_PATH = os.getenv('HOME') + '/Datasets/adv_attrition_model/'
    train_X, train_y, test_X, test_y, cv_X, cv_y, test_adv, cv_adv = prepare_inputs(BASE_PATH+'raw_data_cleanup.csv')
    fit_model(train_X, train_y, test_X, test_y, cv_X, cv_y, test_adv, cv_adv)