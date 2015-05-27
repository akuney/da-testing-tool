import datasci
import datasci.io
import datasci.sklearn_transformers
import matplotlib.pylab as pl
import numpy as np
import pdb
import pandas as pd
import operator
import os
import random
import scipy as sp
import sklearn
import sklearn.cluster
import sklearn.cross_validation
import sklearn.decomposition
import sklearn.ensemble
import sklearn.linear_model
import sklearn.manifold
import sklearn.metrics
import sklearn.preprocessing
import sklearn.svm
import statsmodels.api as sm
import warnings

warnings.filterwarnings("ignore")

MILLIS_IN_DAY = 86400000
TEST_PERCENTAGE = 0.2
N_FOLDS = 6
IMPRESSION_COLUMNS = ['clearing_price', 'campaign_id', 'targeting_group_id', 'offer_id', 'last_beacon_requested_at', 'zero']
NEW_COLUMNS = ['EXPEDIA_ASPP_EXPIRY_LT_30', 'EXPEDIA_ASPP_EXPIRY_BAD', 'EXPEDIA_ASPP_EXPIRY_EQ_30', 'EXPEDIA_IPSNF3_US', 'EXPEDIA_ASPP_EXPIRY_NONE', 'EXPEDIA_MEDIA_COOKIE_PRESENT']

"""
The Triggit HW impression dump has the following fields:
* user_id: Triggit user id
* time_stamp: Impression timestamp
* clearing_price: Market clearing price for the impression (*not the actual Triggit bid)
* campaign_id: Id of the campaign (3 unique IDs in the original dump.  Right Rail, NewsFeed, BlueKai campaigns.)
* targeting_group_id: Id of the targeting group (33 unique IDs in the original dump)
* offer_id: Id of the offer (79 unique IDs in the original dump, 1+ per targeting_group_id.  1 targeting_group_id per offer_id.)
* category: Text description of the targeting group - it is a 1 to 1 mapping.  (e.g. 'Cars FBX dynamic - Results 6-40 days')
* exchange_code: Exchange on which the ad was purchased (e.g. 'fb')
* site: Site on which the ad was placed (e.g. 'facebook.com')
"""

def print_regressor_diagnostics(model, path_name, model_name, df_test, y_value_test):
    if hasattr(model, 'coef_'):
        signals_sorted_by_importance = sorted(dict(zip(df_test.columns, model.coef_)).iteritems(), key=operator.itemgetter(1))
        signals_df = pd.DataFrame(signals_sorted_by_importance, columns=['signal_name', 'signal_coef'])
        signals_df.sort(column='signal_coef', ascending=False, inplace=True)
        signals_df.to_csv(path_name + '/Coefficients_' + model_name + '.csv', index=False, header=True)
    if hasattr(model, 'feature_importances_'):
        signals_sorted_by_importance = sorted(dict(zip(df_test.columns, model.feature_importances_)).iteritems(), key=operator.itemgetter(1))
        signals_df = pd.DataFrame(signals_sorted_by_importance, columns=['signal_name', 'signal_importance'])
        signals_df.sort(column='signal_importance', ascending=False, inplace=True)
        signals_df.to_csv(path_name + '/Importance_' + model_name + '.csv', index=False, header=True)
    pl.close()
    model_predictions = model.predict(df_test)
    r_squared = sklearn.metrics.r2_score(y_value_test, model_predictions)
    pl.scatter(model_predictions, y_value_test)
    pl.title('Out of sample model performance. R Squared: ' + '%.3f' % r_squared)
    pl.xlabel('model prediction')
    pl.ylabel('actual value')
    pl.savefig(path_name + '/Scatter_' + model_name + '.png', format='png')
    pl.close()

def print_classifier_diagnostics(model, path_name, model_name, column_names, y_value_pred, y_value_test):
    if hasattr(model, 'coef_'):
        signals_sorted_by_importance = sorted(dict(zip(column_names, model.coef_[0])).iteritems(), key=operator.itemgetter(1))
        signals_df = pd.DataFrame(signals_sorted_by_importance, columns=['signal_name', 'signal_coef'])
        signals_df.sort(column='signal_coef', ascending=False, inplace=True)
        signals_df.to_csv(path_name + '/Coefficients_' + model_name + '.csv', index=False, header=True)
    if hasattr(model, 'feature_importances_'):
        signals_sorted_by_importance = sorted(dict(zip(column_names, model.feature_importances_)).iteritems(), key=operator.itemgetter(1))
        signals_df = pd.DataFrame(signals_sorted_by_importance, columns=['signal_name', 'signal_importance'])
        signals_df.sort(column='signal_importance', ascending=False, inplace=True)
        signals_df.to_csv(path_name + '/Importance_' + model_name + '.csv', index=False, header=True)

    # TODO: add mean signal values conditional on y value

    # pred_probs = model.predict_proba(df_test)[:, 1]
    fpr, tpr, thresholds = sklearn.metrics.roc_curve(y_value_test, y_value_pred)
    roc_auc = sklearn.metrics.auc(fpr, tpr)

    pl.close()
    pl.clf()
    pl.plot(fpr, tpr, label='ROC curve (area = %0.2f)' % roc_auc)
    pl.plot([0, 1], [0, 1], 'k--')
    pl.xlim([0.0, 1.0])
    pl.ylim([0.0, 1.0])
    pl.xlabel('False Positive Rate')
    pl.ylabel('True Positive Rate')
    pl.title('ROC Plot, ' + model_name)
    pl.legend(loc="lower right")
    pl.show()
    pl.savefig(path_name + '/ROC_' + model_name + '.png', format='png')
    pl.close()

def fit_imp_bid_given_beacon(base_path, use_impression_columns=True):
    io = datasci.io.DataIO(os.path.join(base_path, 'part-all'), nrows=10000,
                           add_binary_columns=True) #nrows=200000; rows in df ~ 200k
    df = io.get_data()
    df = df[df['campaign_id'] == 7438]  # right rail campaigns only

    y_value = df.pop('clearing_price')

    if not use_impression_columns:
        for column in IMPRESSION_COLUMNS:
            if column in df.columns:
                del df[column]
    else:
        df['time_since_last_beacon'] = df['requested_at_iso'] - df['last_beacon_requested_at']
        del df['zero']
        df['time_since_midnight'] = df['requested_at_iso'] % MILLIS_IN_DAY
        df['time_since_sunday_midnight'] = df['requested_at_iso'] % (MILLIS_IN_DAY * 7)
        df['time_since_hour'] = df['requested_at_iso'] % (MILLIS_IN_DAY / 24)
        df['hour_of_day'] = (df['requested_at_iso'] % MILLIS_IN_DAY) / (MILLIS_IN_DAY / 24)
        df['day_of_week'] = (df['requested_at_iso'] % (MILLIS_IN_DAY * 7)) / MILLIS_IN_DAY


    del df['request_id']
    del df['requested_at_iso']

    #model = sklearn.ensemble.RandomForestRegressor(n_estimators=100, max_depth=8, n_jobs=6, compute_importances=True)
    model = sklearn.linear_model.Ridge(alpha=1e6, tol=1e-12)
    df_train, df_test, y_value_train, y_value_test = sklearn.cross_validation.train_test_split(df, y_value, test_size=TEST_PERCENTAGE)
    model.fit(df_train, y_value_train)
    print_regressor_diagnostics(model, base_path, 'imp_bid_given_beacon_logistic', pd.DataFrame(df_test, columns=df.columns), y_value_test)
    return model

def fit_click_given_imp(base_path, use_impression_columns=True):
    io = datasci.io.DataIO(os.path.join(base_path, 'part-all'), nrows=1e5, add_binary_columns=True)
    df = io.get_data()
    if not use_impression_columns:
        for column in IMPRESSION_COLUMNS:
            if column in df.columns:
                del df[column]
    else:
       df['time_since_last_beacon'] = df['requested_at_iso'] - df['last_beacon_requested_at']
    del df['request_id']
    del df['requested_at_iso']
    y_value = df.pop('click_count_all')
    y_value[y_value > 1] = 1

    #model = sklearn.ensemble.RandomForestClassifier(n_estimators=40, max_depth=5, n_jobs=6, compute_importances=True)
    model = sklearn.linear_model.LogisticRegression(tol=1e-12, intercept_scaling=1e6, C=1e9)
    df_train, df_test, y_value_train, y_value_test = sklearn.cross_validation.train_test_split(df, y_value, test_size=TEST_PERCENTAGE)
    model.fit(df_train, y_value_train)
    y_value_pred = model.predict_proba(df_test)[:, 1]
    print_classifier_diagnostics(model, base_path, 'click_given_imp', pd.DataFrame(df_test, columns=df.columns), y_value_pred, y_value_test)

    return model

def fit_prob_imp_given_beacon(base_path):
    io = datasci.io.DataIO(os.path.join(base_path, 'part-all'), nrows=1e5) # nrows=1e6
    df = io.get_data()
    y_value = df.pop('imp_num_total')
    y_value[y_value > 1] = 1
    del df['request_id']
    del df['requested_at_iso']
    del df['clearing_price']
    del df['campaign_id']
    del df['first_imp_requested_at']

    model_LOG = sklearn.linear_model.LogisticRegression(tol=1e-12, intercept_scaling=1e6, C=1e9)
    df_train, df_test, y_value_train, y_value_test = sklearn.cross_validation.train_test_split(df, y_value, test_size=TEST_PERCENTAGE)
    model_LOG.fit(df_train, y_value_train)
    y_value_pred = model_LOG.predict_proba(df_test)[:, 1]
    print_classifier_diagnostics(model_LOG, base_path, 'LOG_prob_imp_given_beacon', df.columns, y_value_pred, y_value_test)

    """
    model_RF = sklearn.ensemble.RandomForestClassifier(n_estimators=100, max_depth=8, n_jobs=6, compute_importances=True)
    df_train, df_test, y_value_train, y_value_test = sklearn.cross_validation.train_test_split(df, y_value, test_size=TEST_PERCENTAGE)
    model_RF.fit(df_train, y_value_train)
    y_value_pred = model_RF.predict_proba(df_test[:, 1])
    print_classifier_diagnostics(model_RF, base_path, 'RF_prob_imp_given_beacon', df_test.columns, y_value_pred, y_value_test)
    """

    return model_LOG

def fit_conv_given_click(base_path):
    io = datasci.io.DataIO(os.path.join(base_path, 'part-all'))
    df = io.get_data()
    y_value = df.pop('y_value')
    del df['request_id']
    del df['requested_at_iso']

    #model = sklearn.ensemble.RandomForestRegressor(n_estimators=40, max_depth=5, n_jobs=1, compute_importances=True)
    model = sklearn.linear_model.Ridge(alpha=1e5, tol=1e-12)
    df_train, df_test, y_value_train, y_value_test = sklearn.cross_validation.train_test_split(df, y_value, test_size=TEST_PERCENTAGE)
    model.fit(df_train, y_value_train)
    print_regressor_diagnostics(model, base_path, 'conv_given_click', pd.DataFrame(df_test, columns=df.columns), y_value_test)

    return model

def get_cross_validation_predictions(cross_validator, model, df, y_value):
    y_value_pred = pd.Series()
    y_value_test = pd.Series()
    for train_index, test_index in cross_validator:
        model.fit(df.iloc[train_index], y_value.iloc[train_index])
        y_value_pred = pd.Series.append(y_value_pred, pd.Series(model.predict_proba(df.iloc[test_index])[:,1]))
        y_value_test = pd.Series.append(y_value_test, y_value.iloc[test_index])
    return y_value_pred, y_value_test

def fit_attr_given_conv(base_path):
    io = datasci.io.DataIO(os.path.join(base_path, 'part-all'))
    df = io.get_data()
    y_value = df.pop('y_value')
    y_value[y_value > 1] = 1
    del df['request_id']
    del df['requested_at_iso']

    #model_RF = sklearn.ensemble.RandomForestClassifier(n_estimators=100, max_depth=8, n_jobs=6, compute_importances=True)
    #y_value_pred, y_value_test = get_cross_validation_predictions(sklearn.cross_validation.KFold(len(y_value), N_FOLDS),
    #                                                       model_RF, df, y_value)
    #print_classifier_diagnostics(model_RF, base_path, 'RF_attr_given_conv', df.columns, y_value_pred, y_value_test)

    # TODO: add LASSO or Least Angle Regression other method for feature selection
    for column in df.columns:
        if df[column].mean() == 0:
            del df[column]
    model_LASSO_LARS_CV = sklearn.linear_model.LassoLarsCV()
    model_LASSO_LARS_CV.fit(df, y_value)
    model_LASSO_LARS = sklearn.linear_model.LassoLars(alpha=model_LASSO_LARS_CV.cv_alphas_[40])
    model_LASSO_LARS.fit(df, y_value)
    feature_coefs = zip(df.columns, model_LASSO_LARS.coef_)
    features_to_keep = [tup[0] for tup in feature_coefs if tup[1] != 0]
    df_features_to_keep = df[features_to_keep]

    # TODO: add statsmodels logistic regression fit, std err, diagnostics
    model_LOG = sm.Logit(y_value, df_features_to_keep) # narrow this down to only use the specific variables that we want
    result = model_LOG.fit()
    print result.summary()

def fit_dim_reductions(base_path):
    io = datasci.io.DataIO(os.path.join(base_path, 'part-all'), nrows=50000)
    df = io.get_data()
    y_value = df.pop('y_value')
    pca = sklearn.decomposition.PCA(n_components=0.95)
    pca.fit(df)

    """
    # MDS (throws malloc error)
    df_small = df[:5000]
    scaler = sklearn.preprocessing.StandardScaler()
    df_small_scaled = pd.DataFrame(scaler.fit_transform(df_small.astype(np.float64)))
    import pdb; pdb.set_trace()
    mds = sklearn.manifold.MDS(n_components=2, max_iter=3000, eps=1e-9, n_jobs=-3)
    pos = mds.fit(df_small_scaled.values).embedding_
    """

def get_users_with_attributes(base_path, click_given_imp, conv_given_click):
    """
    """
    df = datasci.io.DataIO(os.path.join(base_path, 'part-all'), nrows=50000).get_data()
    for column in IMPRESSION_COLUMNS:
        if column in df:
            del df[column]
    for column in NEW_COLUMNS:
        if column in df:
            del df[column]
    del df['y_value']
    click_probabilities = click_given_imp.predict_proba(df)[:, 1]
    conversion_expected_values = conv_given_click.predict(df)
    df['click_probabilities'] = click_probabilities
    df['conversion_values'] = conversion_expected_values

    return df

def get_betas_with_intercepts():
    """
    returns a data frame with a set of random betas and intercepts for clearing_price_given_cpm and imp_given_cpm
    we give some beta parameters at the top
    """
    columns = ['beta_clearing_price', 'intercept_clearing_price', 'beta_impression_prob', 'intercept_impression_prob']
    betas_intercepts = pd.DataFrame(columns=columns)
    number_of_rows = int(1e2)
    average_cpm_guess = 3.
    average_beta_clearing_price_guess = 0.33
    average_beta_impression_prob_guess = 0.1
    for _ in range(number_of_rows):
        weibull_scale_cp = 0.38
        weibull_shape_cp = 3
        beta_clearing_price = random.weibullvariate(weibull_scale_cp, weibull_shape_cp) # weibull with scale 0.33, shape 3
        intercept_adjustment_clearing_price = -average_cpm_guess * average_beta_clearing_price_guess
        weibull_scale_pr = 1.1
        weibull_shape_pr = 1.5
        beta_impression_prob = random.weibullvariate(weibull_scale_pr, weibull_shape_pr)
        intercept_adjustment_impression_prob = -average_cpm_guess * average_beta_impression_prob_guess
        new_row = pd.DataFrame([beta_clearing_price, intercept_adjustment_clearing_price, beta_impression_prob,
                                             intercept_adjustment_impression_prob]).T
        new_row.columns = columns
        betas_intercepts = pd.concat([betas_intercepts, new_row])
    return betas_intercepts

class RandomTestStrategy(object):
    def __init__(self, name, distribution_fn, *args):
        self.name = name
        self._distribution_fn = distribution_fn
        self._args = args

    def get_random_cpm(self, expected_value=None):
        if expected_value is None:
            return self._distribution_fn(*self._args)
        else:
            raise 'Error: expected_value version of get_random_cpm not implemented!'

def get_random_test_strategies():
    strategy1 = RandomTestStrategy('uniform, 0.1 to 1.6', np.random.uniform, 0.1, 1.6)
    strategy2 = RandomTestStrategy('weibull, 1.5, 1.1', random.weibullvariate, 1.5, 1.1)
    return [strategy1, strategy2]

def predict_with_adjustment(imp_given_cpm, row, cpm, beta_intercept):
    row_filtered = row[1][:-2]
    dot_product = np.dot(imp_given_cpm.coef_[0], row_filtered)
    dot_product += cpm * beta_intercept[1]['beta_impression_prob']
    dot_product += beta_intercept[1]['intercept_impression_prob']
    return 1./(1 + np.exp(-dot_product))

def show_impression_probability(cpm, row, imp_given_cpm, beta_intercept):
    prob_impression = predict_with_adjustment(imp_given_cpm, row, cpm, beta_intercept)
    return prob_impression

def expected_clearing_price(cpm, row, clearing_price_given_cpm, beta_intercept):
    row_filtered = row[1][:-2]
    dot_product = np.dot(clearing_price_given_cpm.coef_, row_filtered)
    dot_product += cpm * beta_intercept[1]['beta_clearing_price']
    dot_product += beta_intercept[1]['intercept_clearing_price']
    clearing_price = max(0.01, dot_product)
    clearing_price = round(clearing_price, 2)

    return clearing_price

def prob_imp_is_fit(x_matrix, trial_rows):
    del x_matrix['click_probabilities']
    del x_matrix['conversion_values']
    x_matrix['cpm'] = trial_rows['cpm']
    x_matrix['impression_shown'] = trial_rows['impression_shown']
    x_matrix = x_matrix[x_matrix['impression_shown'].notnull()]

    impression_shown = x_matrix.pop('impression_shown')
    impression_shown = impression_shown.astype('float64')
    # remove dependent rows
    transformer = datasci.sklearn_transformers.DependentFeaturesRemover(eigenvalue_ratio_threshold=1e-3)
    transformer.fit(x_matrix)
    x_matrix = transformer.transform(x_matrix)
    x_matrix = x_matrix.astype('float64')
    logit = sm.Logit(impression_shown, x_matrix)
    result = logit.fit()
    cpm_z_score = result.summary().tables[1][-1][3]  # -1 because cpm is last row of table; 3 because z score is in column 3
    return cpm_z_score > 3

def exp_clearing_price_is_fit(x_matrix, trial_rows):
    del x_matrix['click_probabilities']
    del x_matrix['conversion_values']
    x_matrix['cpm'] = trial_rows['cpm']
    x_matrix['clearing_price'] = trial_rows['clearing_price']
    x_matrix = x_matrix[x_matrix['clearing_price'].notnull()]

    clearing_price = x_matrix.pop('clearing_price')
    clearing_price = clearing_price.astype('float64')
    # remove dependent rows
    transformer = datasci.sklearn_transformers.DependentFeaturesRemover(eigenvalue_ratio_threshold=1e-3)
    transformer.fit(x_matrix)
    x_matrix = transformer.transform(x_matrix)
    x_matrix = x_matrix.astype('float64')
    ols = sm.OLS(clearing_price, x_matrix)
    result = ols.fit()
    cpm_z_score = float(result.summary().tables[1][-1][3].data.strip())  # -1 because cpm is last row of table; 3 because z score is in column 3
    return cpm_z_score > 3

def model_is_fit(ua_p_imp_e_cp, trial_rows):
    # update not_yet_fit (True if the 95% confidence interval for the beta value is still outside of +/- 25% of the actual beta value for both betas)
    prob_imp_is_fit_bool = prob_imp_is_fit(ua_p_imp_e_cp.copy(), trial_rows)
    exp_clearing_price_is_fit_bool = exp_clearing_price_is_fit(ua_p_imp_e_cp.copy(), trial_rows)

    return prob_imp_is_fit_bool and exp_clearing_price_is_fit_bool

def expected_revenue(row):
    return row[1]['click_probabilities'] * row[1]['conversion_values']

def get_test_stats(random_test, ua_p_imp_e_cp, beta_intercept, clearing_price_given_cpm, imp_given_cpm):
    time = 0
    spend = 0
    revenue = 0
    checking_interval = 100

    trial_rows = pd.DataFrame(index=ua_p_imp_e_cp.index, columns=['cpm', 'impression_shown', 'clearing_price'])
    for index, row in enumerate(ua_p_imp_e_cp.iterrows()):
        cpm = random_test.get_random_cpm()
        trial_rows['cpm'][index] = cpm
        probability = show_impression_probability(cpm, row, imp_given_cpm, beta_intercept)
        if np.random.binomial(1, probability):
            trial_rows['impression_shown'][index] = 1
            exp_clearing_price = expected_clearing_price(cpm, row, clearing_price_given_cpm, beta_intercept)
            trial_rows['clearing_price'][index] = exp_clearing_price
            time += 1
            spend += exp_clearing_price
            revenue += expected_revenue(row)
            if time % checking_interval == 0:
                if True: #if model_is_fit(ua_p_imp_e_cp, trial_rows):
                    #print 'model done fitting! time = %f' % time
                    break
                else:
                    print 'model not fit yet, time = %f' % time
        else:
            trial_rows['impression_shown'][index] = 0
    return time, spend, revenue

def get_cpm_by_user(base_path, clearing_price_given_cpm, imp_given_cpm, click_given_imp, conv_given_click):
    ua_p_imp_e_cp = get_users_with_attributes(base_path, click_given_imp, conv_given_click)
    betas_intercepts = get_betas_with_intercepts()
    random_test_strategies = get_random_test_strategies()
    for random_test in random_test_strategies:
        times_spends = pd.DataFrame(columns=['time', 'spend', 'revenue'])
        for beta_intercept in betas_intercepts.iterrows():
            time, spend, revenue = get_test_stats(random_test, ua_p_imp_e_cp, beta_intercept, clearing_price_given_cpm, imp_given_cpm)
            times_spends = times_spends.append(pd.DataFrame({'time': time, 'spend': spend, 'revenue': revenue}, index=[len(times_spends)]))
        print 'test:', random_test.name, 'time: %.2f' % times_spends['time'].mean(), 'spend: %.2f' % times_spends['spend'].mean(),\
                'revenue: %.2f' % times_spends['revenue'].mean()

def compare_model_results(base_path, click_given_imp, conv_given_click):
    df = datasci.io.DataIO(os.path.join(base_path, 'part-all'), nrows=100000).get_data()
    del df['request_id']
    del df['requested_at_iso']
    df.pop('y_value')

    y_cgi = click_given_imp.predict_proba(df)[:, 1]
    y_cgc = conv_given_click.predict(df)
    corr = sp.stats.pearsonr(y_cgi, y_cgc)[0]
    print 'correlation:', corr
    pl.scatter(y_cgi, y_cgc)
    pl.title('Model predictions; correlation: ' + '%.3f' % corr)
    pl.xlabel('predicted P(click | impression)')
    pl.ylabel('predicted E(conversion | click)')
    pl.savefig(base_path + '/model_correlations' + '.png', format='png')
    pl.close()

if __name__ == '__main__':
    BASE_PATH = os.path.join(os.getenv("HOME"), 'Datasets')
    BASE_PATH_IMP_BID_GIVEN_BEACON = os.path.join(BASE_PATH, '20131206-hw-imp-bid-given-beacon-b/training/normalized_extra.gz')
    clearing_price_given_cpm = fit_imp_bid_given_beacon(BASE_PATH_IMP_BID_GIVEN_BEACON, False)
    BASE_PATH_PROB_IMP_GIVEN_BEACON = os.path.join(BASE_PATH, '20131206-hw-imp-prob-given-beacon-c/training/normalized_extra.gz')
    imp_given_beacon = fit_prob_imp_given_beacon(BASE_PATH_PROB_IMP_GIVEN_BEACON)
    BASE_PATH_CLICK_GIVEN_IMP = os.path.join(BASE_PATH, '20131206-hw-click-given-imp-b/training/normalized_extra.gz')
    click_given_imp = fit_click_given_imp(BASE_PATH_CLICK_GIVEN_IMP, False)
    BASE_PATH_CONV_GIVEN_CLICK = os.path.join(BASE_PATH, 'GenerateSignalsTask-20131217234553/HOTWIRE/CARS/training/normalized.gz')
    conv_given_click = fit_conv_given_click(BASE_PATH_CONV_GIVEN_CLICK)
    #compare_model_results(BASE_PATH_CONV_GIVEN_CLICK, click_given_imp, conv_given_click)
    #BASE_PATH_ATTR_GIVEN_CONV = os.path.join(BASE_PATH, '20131209-hw-attr-given-conv-b/training/normalized.gz')
    #fit_attr_given_conv(BASE_PATH_ATTR_GIVEN_CONV)
    #BASE_PATH_GENERATE_SIGNALS = os.path.join(BASE_PATH, 'GenerateSignalsTask-20131215060501/EXPEDIA/FLIGHTS/training/normalized_binary.gz')
    #fit_dim_reductions(BASE_PATH_GENERATE_SIGNALS)
    BASE_PATH_USER_DATA = os.path.join(BASE_PATH, 'GenerateSignalsTask-20131215060501/EXPEDIA/FLIGHTS/training/normalized_binary.gz')
    get_cpm_by_user(BASE_PATH_USER_DATA, clearing_price_given_cpm, imp_given_beacon, click_given_imp, conv_given_click)
