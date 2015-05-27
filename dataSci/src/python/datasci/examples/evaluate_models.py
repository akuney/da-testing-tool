import os
import copy
import datetime
import pandas as pd
import numpy as np
import scipy
import datasci
import datasci.io
import datasci.transformers
import datasci.models
import datasci.aggregators
import datasci.cross_validation
import datasci.plotters
import sklearn
import sklearn.linear_model
import sklearn.tree
import sklearn.ensemble
import sklearn.preprocessing
import sklearn.grid_search
import warnings
warnings.filterwarnings("ignore")

"""
    Example file with a pipeline for building a number of models and plotting
    the results.

    Data flows as follows through the pipeline:

    io object
        TO
    transformer object
        TO
    gridSearchCV object (parameterized by cross_validation object)
        TO
    aggregator object (parameterized by plotter objects, gridSearchCV objects)

"""

LAMBDAS_TO_RUN = [1e5]
Y_COLUMN_TO_FIT = 'null_SESSIONS_1'
Y_COLUMNS_EVALUATION_LIST =\
    ['null_AD_CALLS_1', 'null_SESSIONS_1', 'null_DAYS_14']
INDEX_COLUMN_NAME = 'publisher_user_id'
MAX_Y_VALUE = 100
COL_TO_SPLIT = 'sessionizer_SESSION_COUNT'
NUM_SPLITS = 4
TRAIN_MAX_ROWS_PER_FILE = 1e5
TEST_MAX_ROWS_PER_FILE = 1e5
NUM_CONCURRENT_JOBS = 1
FINAL_DATE_TO_INCLUDE = datetime.datetime(2013, 2, 14)
NUMBER_CV_FOLDS = 3
COLUMN_NAMES_TO_REMOVE = ['null_AD_CALLS_CLICKS_1', 'null_SESSIONS_CLICKS_1',
    'null_AD_CALLS_1', 'null_AD_CALLS_2', 'null_AD_CALLS_3', 'null_AD_CALLS_5',
    'null_AD_CALLS_10', 'null_AD_CALLS_20', 'null_AD_CALLS_50',
    'null_SESSIONS_1', 'null_SESSIONS_2', 'null_SESSIONS_3', 'null_SESSIONS_5',
    'null_SESSIONS_10', 'null_MINUTES_1', 'null_MINUTES_5', 'null_MINUTES_15',
    'null_MINUTES_60', 'null_HOURS_6', 'null_HOURS_12', 'null_DAYS_1',
    'null_DAYS_2', 'null_DAYS_7', 'null_DAYS_14']
OUTPUT_DIRECTORY = os.getenv("HOME") + '/Datasets/' +\
                   datetime.datetime.now().strftime('%Y%m%d%H%M%S') + '_' + \
                   Y_COLUMN_TO_FIT + '_train_' + str(TRAIN_MAX_ROWS_PER_FILE) +\
                   '_test_' + str(TEST_MAX_ROWS_PER_FILE) + '/'
os.makedirs(OUTPUT_DIRECTORY)

base_path = os.getenv('HOME') +\
            '/Datasets/hclc_data_many_y_20130422_with_p_u_id/'
training_path = base_path + 'training.gz'
testing_path = base_path + 'testing.gz'

"""
    DEFINE IO OBJECTS
"""
io_train = datasci.io.DataIO(training_path, index_col_name='requested_at_iso',
    parse_dates=True, date_parser=datasci.io.parse_dates_from_iso,
    nrows_per_file=TRAIN_MAX_ROWS_PER_FILE)
io_test = datasci.io.DataIO(testing_path, index_col_name='requested_at_iso',
    parse_dates=True, date_parser=datasci.io.parse_dates_from_iso,
    nrows_per_file=TEST_MAX_ROWS_PER_FILE)

"""
    DEFINE TRANSFORMER OBJECTS

    X_TRANSFORMER_LIST : list of objects that transform X matrices only
    ALL_TRANSFORMER_LIST : list of objects that transform X, y, and labels
"""

X_TRANSFORMER_LIST = [
    datasci.transformers.ColumnNameFeaturesRemover(COLUMN_NAMES_TO_REMOVE), \
    datasci.transformers.DTypeObjectFeaturesRemover(), \
    datasci.transformers.DependentFeaturesRemover(
        cols_to_keep=['sessionizer_SESSION_COUNT',
        'previous_conversions_ANY_PREVIOUS_CONVERSION_FLIGHTS', \
        'previous_conversions_ANY_PREVIOUS_CONVERSION_CARS_OVER_12_WEEKS', \
        'previous_conversions_ANY_PREVIOUS_CONVERSION_CARS_6_12_WEEKS', \
        'previous_conversions_ANY_PREVIOUS_CONVERSION_CARS_3_6_WEEKS', \
        'previous_conversions_ANY_PREVIOUS_CONVERSION_CARS_0_3_WEEKS', \
        'previous_conversions_ANY_PREVIOUS_CONVERSION_' + \
        'HOTELS_OVER_12_WEEKS',
        'previous_conversions_ANY_PREVIOUS_CONVERSION_HOTELS_6_12_WEEKS', \
        'previous_conversions_ANY_PREVIOUS_CONVERSION_HOTELS_3_6_WEEKS', \
        'previous_conversions_ANY_PREVIOUS_CONVERSION_HOTELS_0_3_WEEKS', \
        'previous_conversions_ANY_PREVIOUS_CONVERSION_' + \
        'PACKAGES_OVER_12_WEEKS',
        'previous_conversions_ANY_PREVIOUS_CONVERSION_' + \
        'PACKAGES_6_12_WEEKS', \
        'previous_conversions_ANY_PREVIOUS_CONVERSION_PACKAGES_3_6_WEEKS',
        'previous_conversions_ANY_PREVIOUS_CONVERSION_PACKAGES_0_3_WEEKS', \
        'expedia_user_has_minfo_EXPEDIA_USER_HAS_MINFO_YES',
        'sessionizer_SESSION_COUNT']),
    datasci.transformers.SklearnScalerWrapper(
        sklearn.preprocessing.StandardScaler())]

ALL_TRANSFORMER_LIST = [datasci.transformers.DateBoundaryRecordRemover(
    end_date_inclusive=FINAL_DATE_TO_INCLUDE)]

test_train_object = datasci.io.DataIOTestTrain(io_train, io_test,
    X_TRANSFORMER_LIST, ALL_TRANSFORMER_LIST, Y_COLUMN_TO_FIT,
    INDEX_COLUMN_NAME, Y_COLUMNS_EVALUATION_LIST)
test_train = test_train_object.get_train_test_data()

"""
    Define CROSS_VALIDATOR object, perform grid searches on training
    data to find best set of model hyperparameters
"""
CROSS_VALIDATOR = datasci.cross_validation.KFoldLabel(
    labels=test_train.index_train.values, n_folds=NUMBER_CV_FOLDS)

# define functions to test different hyperparameters passed in GridSearchCV
# objects, test to find the best
# make plotting functions plot results (lift curves, % protected in HV vs LV,
# etc) for a set of models

GRID_SEARCHES_TO_PERFORM = []
# Model: ridge regression with varying alphas
GRID_SEARCHES_TO_PERFORM.append(
    sklearn.grid_search.GridSearchCV(sklearn.linear_model.Ridge(),
    {'alpha': LAMBDAS_TO_RUN}, cv=CROSS_VALIDATOR, n_jobs=NUM_CONCURRENT_JOBS))
# Model: SplitModel: different fits for different groups of users
GRID_SEARCHES_TO_PERFORM.append(sklearn.grid_search.GridSearchCV(
    datasci.models.SplitModel(sklearn.linear_model.Ridge(), test_train,
    COL_TO_SPLIT, number_of_splits=NUM_SPLITS), {'sklearn_model':
    [sklearn.linear_model.Ridge(alpha=1e2),
     sklearn.linear_model.Ridge(alpha=5e3),
     sklearn.linear_model.Ridge(alpha=1e5)]},
    cv=CROSS_VALIDATOR, n_jobs=NUM_CONCURRENT_JOBS))
# Model: NewUserSplitModel: different fits for new vs old users
GRID_SEARCHES_TO_PERFORM.append(sklearn.grid_search.GridSearchCV(
    datasci.models.NewUserSplitModel(sklearn.linear_model.Ridge(), test_train),
    {'sklearn_model': [sklearn.linear_model.Ridge(alpha=1e2),
     sklearn.linear_model.Ridge(alpha=5e3),
     sklearn.linear_model.Ridge(alpha=1e5)]},
    cv=CROSS_VALIDATOR, n_jobs=NUM_CONCURRENT_JOBS))
# Model: RandomForestRegressor with varying numbers of trees
# GRID_SEARCHES_TO_PERFORM.append(sklearn.grid_search.GridSearchCV(
# sklearn.ensemble.RandomForestRegressor(max_features=int(np.sqrt(
# test_train.get_num_features())), max_depth=5), {'n_estimators':
# [10, 100, 1000]}, cv=CROSS_VALIDATOR, n_jobs=NUM_CONCURRENT_JOBS))

"""
    Define PLOTTER objects, which will be passed into the model aggregator
    objects
"""
PLOTTERS = []
PLOTTERS.append(datasci.plotters.LiftCurvePlotter())
PLOTTERS.append(datasci.plotters.HVBookingsProtectedPlotter(
    ad_call_pct_in_hc=0.25))

MODELS = []

model_aggregator = datasci.aggregators.ModelAggregator(
    test_train, PLOTTERS, OUTPUT_DIRECTORY, MODELS, GRID_SEARCHES_TO_PERFORM)
model_aggregator.fit_models()
model_aggregator.graph_results()

df_X = test_train.X_test
test_set_split_labels = \
    ((df_X['previous_conversions_ANY_PREVIOUS_' +\
        'CONVERSION_FLIGHTS'] > 0) |\
    (df_X['previous_conversions_ANY_PREVIOUS_' +\
          'CONVERSION_CARS_OVER_12_WEEKS'] > 0) |\
    (df_X['previous_conversions_ANY_PREVIOUS_' +\
          'CONVERSION_CARS_6_12_WEEKS'] > 0) |\
    (df_X['previous_conversions_ANY_PREVIOUS_' +\
          'CONVERSION_CARS_3_6_WEEKS'] > 0) | \
    (df_X['previous_conversions_ANY_PREVIOUS_' +\
          'CONVERSION_CARS_0_3_WEEKS'] > 0) | \
    (df_X['previous_conversions_ANY_PREVIOUS_' +\
          'CONVERSION_HOTELS_OVER_12_WEEKS'] > 0) | \
    (df_X['previous_conversions_ANY_PREVIOUS_' +\
          'CONVERSION_HOTELS_6_12_WEEKS'] > 0) | \
    (df_X['previous_conversions_ANY_PREVIOUS_' +\
          'CONVERSION_HOTELS_3_6_WEEKS'] > 0) | \
    (df_X['previous_conversions_ANY_PREVIOUS_' +\
          'CONVERSION_HOTELS_0_3_WEEKS'] > 0) | \
    (df_X['previous_conversions_ANY_PREVIOUS_' +\
          'CONVERSION_PACKAGES_OVER_12_WEEKS'] > 0) | \
    (df_X['previous_conversions_ANY_PREVIOUS_' +\
          'CONVERSION_PACKAGES_6_12_WEEKS'] > 0) | \
    (df_X['previous_conversions_ANY_PREVIOUS_' +\
          'CONVERSION_PACKAGES_3_6_WEEKS'] > 0) | \
    (df_X['previous_conversions_ANY_PREVIOUS_' +\
          'CONVERSION_PACKAGES_0_3_WEEKS'] > 0) | \
    (df_X['expedia_user_has_minfo_EXPEDIA_USER_HAS_MINFO_YES'] > 0) | \
    (df_X['sessionizer_SESSION_COUNT'] > 1))
test_set_split_labels = test_set_split_labels.astype('int32')
model_aggregator.graph_results(test_set_split_labels)
