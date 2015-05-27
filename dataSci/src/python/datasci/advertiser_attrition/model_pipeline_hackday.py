import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
import os
import random
import pylab as pl
import matplotlib.pyplot as plt
from sklearn.metrics import roc_curve, auc, confusion_matrix


def prepare_inputs(file_location):
    print "Preparing inputs ..."
    test_data_split_share = 0.3
    df = pd.read_csv(file_location, header=None)
    df.columns = ['MONTH', 'EXCEEDED_CREDIT_THRESHOLD', 'PAYMENT_METHOD_ID', 'SPENDING CAP_ENABLED',
                  'ALLOW_ADS_ON_PACKAGE_PATH', 'SUSPENDED_FOR_NON_PAYMENT', 'PRIOR_PAUSE_COUNT',
                  'INTENT_MEDIA_MARKET_ID', 'REPORT_SEGMENT_NAME', 'DELTA_IMPRESSIONS',
                  'DELTA_CLICKS', 'DELTA_SPEND', 'DELTA_ROI', 'Y_VALUE']

    df = pre_process(df)

    test_rows = random.sample(df.index, int(len(df)*test_data_split_share))
    df_test = df.ix[test_rows]
    df_train = df.drop(test_rows)

    train_X = df_train.drop('Y_VALUE', 1)
    test_X = df_test.drop('Y_VALUE', 1)

    train_y = df_train['Y_VALUE']
    test_y = df_test['Y_VALUE']

    return train_X, train_y, test_X, test_y

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

def fit_model(train_X, train_y, test_X, test_y):
    print "Fitting model ..."
    fit_random_forest(train_X, train_y, test_X, test_y)

def fit_random_forest(train_X, train_y, test_X, test_y):
    rfclf = RandomForestClassifier(n_estimators=100, max_features='sqrt', n_jobs=4)
    rfclf.fit(train_X, train_y)
    print rfclf.feature_importances_

    preds_ = rfclf.predict(test_X)
    probas_ = rfclf.predict_proba(test_X)

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
    train_X, train_y, test_X, test_y = prepare_inputs(BASE_PATH+'raw_data.csv')
    fit_model(train_X, train_y, test_X, test_y)