__author__ = 'yoojong.bang'

import numpy as np
import pandas as pd
import vertica_python as vp
from sklearn import linear_model
from sklearn.preprocessing import Imputer
import statsmodels.api as sm
import pylab as pl

print 'inverted CVR analysis started..'

arr = []
col_name = ['date_in_et', 'site_name', 'ad_unit_type', 'path_category_type', 'ad_unit_name', 'trip_type',
            'browser_family', 'os_family', 'device_family', 'brand', 'market_id', 'predict_mode_type',
            'segmentation_model_type', 'logged_in_user', 'page_view_type', 'advertiser_id', 'auction_position',
            'click_placement_type', 'uv_count', 'click_count', 'actual_cpc_sum', 'conversion_count_flights',
            'conversion_count_hotels', 'conversion_count_packages', 'conversion_count_cars', 'conversion_count_total',
            'conversion_value_sum_flights', 'conversion_value_sum_hotels', 'conversion_value_sum_packages',
            'conversion_value_sum_cars', 'conversion_value_sum_total', 'net_conversion_value_sum_flights',
            'net_conversion_value_sum_hotels', 'net_conversion_value_sum_packages', 'net_conversion_value_sum_cars',
            'net_conversion_value_sum_total', 'random']


def row_handler(row):
    arr.append(np.asarray(row))

conn = vp.connect({
    'host': 'production-vertica-cluster-with-failover.internal.intentmedia.net',
    'port': 5433,
    'user': 'tableau',
    'password': '9WOGfffN',
    'database': 'intent_media',
    'option': 'CurrentLoadBalance=1'
})

cur = conn.cursor(row_handler=row_handler)
sql_string = "select *, random() as random from intent_media_sandbox_production.YB_Inverted_CVR_final"

cur.execute(sql_string)
conn.close()

print 'data fetch process completed..'

# putting the result set of the query to the pandas dataframe
raw_data = pd.DataFrame(arr, columns=col_name)

print 'data frame successfully created..'

# split data set into training set and test set using the random column
train = raw_data[raw_data['random'] < 0.1]
test = raw_data[(raw_data['random'] >= 0.1) & (raw_data['random'] < 0.2)]
print 'data subset process completed..'

# setting up linear regression inputs
arrX_train = train[['site_name', 'ad_unit_name', 'trip_type', 'browser_family', 'os_family', 'device_family',
                    'predict_mode_type', 'segmentation_model_type', 'logged_in_user', 'page_view_type',
                    'auction_position', 'click_placement_type']]
arrX_train_no_na = arrX_train.dropna()

arrX_test = test[['site_name', 'ad_unit_name', 'trip_type', 'browser_family', 'os_family', 'device_family',
                  'predict_mode_type', 'segmentation_model_type', 'logged_in_user', 'page_view_type',
                  'auction_position', 'click_placement_type']]
arrX_test_no_na = arrX_test.dropna()

Y_train = train.ix[arrX_train_no_na.index.values]['conversion_count_total']
Y_train = Y_train.fillna(0)
Y_test = test.ix[arrX_test_no_na.index.values]['conversion_count_total']
Y_test = Y_test.fillna(0)


# change the variable type to categorical variables
val_name_list_train = []
val_name_list_test = []
for item in arrX_train_no_na:
    val_name_train = 'dummy_train_' + item
    val_name_list_train.append(val_name_train)
    val_name_test = 'dummy_test_' + item
    val_name_list_test.append(val_name_test)
    exec "%s = pd.get_dummies(arrX_train_no_na['%s'], prefix='%s')" % (val_name_train, item, item)
    exec "%s = pd.get_dummies(arrX_test_no_na['%s'], prefix='%s')" % (val_name_test, item, item)

arrX_train_categorized = pd.DataFrame(index=arrX_train_no_na.index.values)
for item in val_name_list_train:
    exec "arrX_train_categorized = arrX_train_categorized.join(%s.ix[:, str(%s.keys()[1]):])" % (item, item)

arrX_test_categorized = pd.DataFrame(index=arrX_test_no_na.index.values)
for item in val_name_list_test:
    exec "arrX_test_categorized = arrX_test_categorized.join(%s.ix[:, str(%s.keys()[1]):])" % (item, item)

clt = linear_model.LinearRegression()
clt.fit(arrX_train_categorized.values, Y_train)
print 'intercepts: \n', clt.intercept_
print 'coefficients: \n', clt.coef_
print 'residual sum of squares: %.2f' % np.mean((clt.predict(arrX_test_categorized) - Y_test) ** 2)
print 'variance score: %.2f' % clt.score(arrX_test_categorized, Y_test)

# fit with statsmodels package
logit = sm.Logit(Y_train, arrX_train_categorized)
result = logit.fit()
print result.summary()