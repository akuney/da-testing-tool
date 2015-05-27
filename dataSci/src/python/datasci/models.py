import copy
import numpy as np
import pandas as pd
import sklearn

"""
Models
------
Extensions of sklearn models that implement fit(X, y) and predict(X)
"""
class CompoundModel(sklearn.base.RegressorMixin, sklearn.base.BaseEstimator):
    """
        Fits a compound model, of the form:
            E(y) = E(y | y > 0, X) * P(y > 0 | X)

        Parameters
        ----------
        sklearn_model_base : base sklearn model, used to fit P(y > 0 | X)
        sklearn_model_contingent : contingent sklearn model, used to fit
            E(y | y > 0, X)
    """
    def __init__(self, sklearn_model_base, sklearn_model_contingent):
        self._sklearn_model_base = copy.copy(sklearn_model_base)
        self._sklearn_model_contingent = copy.copy(sklearn_model_contingent)

    def fit(self, X, y):
        self._sklearn_model_base.fit(X, y)
        X_contingent = X[y > 0]
        y_contingent = y[y > 0]
        self._sklearn_model_contingent.fit(X_contingent, y_contingent)

    def predict(self, X):
        predicted_probabilities = self._sklearn_model_base.predict_proba(X)
        predicted_contingent_values = self._sklearn_model_contingent.predict(X)
        return predicted_probabilities * predicted_contingent_values


class SplitModel(sklearn.base.RegressorMixin, sklearn.base.BaseEstimator):
    """
        Fits multiple models on a dataset, split by column quantiles
        So e.g. will fit one model each for users with 1 session, 2 sessions,
        3-5 sessions, 6+ sessions, ...

        Parameters
        ----------
        sklearn_model : sklearn model object that will be copied for each split
            and used in fitting and predicting
        col_to_split : column name string of the column used to split the data
        number_of_splits : number of equal splits to fit models on
        split_levels : a list of specific levels of the column that the user
            wants to split on
        test_train : the set of testing/training data used in this model.
            Passed in so that the model can be used with sklearn's
            GridSearchCV, which strips DataFrames of their index/column
            information
     """
    def __init__(self, sklearn_model, test_train, col_to_split,
                 number_of_splits=None, split_levels=None):
        self.sklearn_model = sklearn_model
        self.test_train = test_train
        self.col_to_split = col_to_split
        if(number_of_splits is None and split_levels is None):
            raise Exception("SplitModel: must provide either number_of_splits"
                            "or split_levels")
        self.number_of_splits = number_of_splits
        self._split_levels = split_levels
        self.models = None

    def __str__(self):
        return 'SplitModel. ' +\
               'sklearn_model=' + str(self.sklearn_model) + ', ' +\
               'col_to_split=' + str(self.col_to_split) + ', ' +\
               'number_of_splits=' + str(self.number_of_splits)

    def fit(self, X, y):
        if 'DataFrame' not in str(type(X)):
            X = self._get_data_frame_with_dummy_index(X)
        if self._split_levels is None:
            self._split_levels = self._get_split_levels(X, self.col_to_split,
                                                        self.number_of_splits)
        self.models = []
        for idx in range(len(self._split_levels) - 1):
            split_model = copy.copy(self.sklearn_model)
            X_relevant = X[(X[self.col_to_split] >= self._split_levels[idx]) &
                        (X[self.col_to_split] < self._split_levels[idx + 1])]
            y_relevant = y[(X[self.col_to_split] >= self._split_levels[idx]) &
                        (X[self.col_to_split] < self._split_levels[idx + 1])]
            split_model.fit(X_relevant, y_relevant)
            self.models.append(split_model)

    def predict(self, X):
        if 'DataFrame' not in str(type(X)):
            X = self._get_data_frame_with_dummy_index(X)
        result = pd.Series(np.zeros(len(X)))
        for idx in range(len(self.models)):
            result_to_add = pd.Series(self.models[idx].predict(X))
            result_to_add[(X[self.col_to_split]
                           < self._split_levels[idx]).values |
                          (X[self.col_to_split] >=
                           self._split_levels[idx + 1]).values] = 0
            result += result_to_add.values
        result = result.values
        return result

    def _get_split_levels(self, X, col_to_split, number_of_splits):
        col_values = X[col_to_split].copy()
        col_values.sort()
        col_values = col_values.values
        num_values = len(col_values)
        split_levels = []
        split_increment = num_values / number_of_splits
        for split_num in range(number_of_splits):
            split_levels.append(col_values[split_num * split_increment])
        split_levels.append(col_values.max() + 1)
        return split_levels

    def _get_data_frame_with_dummy_index(self, X):
        cols = self.test_train.X_train.columns
        return pd.DataFrame(X, columns=cols, index=range(X.shape[0]))


class NewUserSplitModel(sklearn.base.RegressorMixin,
                        sklearn.base.BaseEstimator):
    """
        Fits multiple models on a dataset, split by "new users" vs "old users"

        Parameters
        ----------
        sklearn_model : sklearn model object that will be copied for each
            split and used in fitting and predicting.
        test_train : the set of testing/training data used in this model.
            Passed in so that the model can be used with sklearn's
            GridSearchCV, which strips DataFrames of their index/column
            information.
        test_train : the set of testing/training data used in this model.
            Passed in so that the model can be used with sklearn's
            GridSearchCV, which strips DataFrames of their index/column
            information.
    """

    def __init__(self, sklearn_model, test_train):
        self.sklearn_model = sklearn_model
        self.test_train = test_train

    def __str__(self):
        return 'NewUserSplitModel' +\
               'sklearn_model=' + str(self.sklearn_model)

    def is_old_user(self, X):
        return \
        (X['previous_conversions_ANY_PREVIOUS_CONVERSION_FLIGHTS'] > 0) | \
        (X['previous_conversions_ANY_PREVIOUS_CONVERSION_CARS' +\
            '_OVER_12_WEEKS'] > 0) | \
        (X['previous_conversions_ANY_PREVIOUS_CONVERSION_CARS' +\
            '_6_12_WEEKS'] > 0) | \
        (X['previous_conversions_ANY_PREVIOUS_CONVERSION_CARS' +\
            '_3_6_WEEKS'] > 0) | \
        (X['previous_conversions_ANY_PREVIOUS_CONVERSION_CARS' +\
            '_0_3_WEEKS'] > 0) | \
        (X['previous_conversions_ANY_PREVIOUS_CONVERSION_HOTELS' +\
            '_OVER_12_WEEKS'] > 0) | \
        (X['previous_conversions_ANY_PREVIOUS_CONVERSION_HOTELS' +\
            '_6_12_WEEKS'] > 0) | \
        (X['previous_conversions_ANY_PREVIOUS_CONVERSION_HOTELS' +\
            '_3_6_WEEKS'] > 0) | \
        (X['previous_conversions_ANY_PREVIOUS_CONVERSION_HOTELS' +\
            '_0_3_WEEKS'] > 0) | \
        (X['previous_conversions_ANY_PREVIOUS_CONVERSION_PACKAGES' +\
            '_OVER_12_WEEKS'] > 0) | \
        (X['previous_conversions_ANY_PREVIOUS_CONVERSION_PACKAGES' +\
            '_6_12_WEEKS'] > 0) | \
        (X['previous_conversions_ANY_PREVIOUS_CONVERSION_PACKAGES' +\
            '_3_6_WEEKS'] > 0) | \
        (X['previous_conversions_ANY_PREVIOUS_CONVERSION_PACKAGES' +\
            '_0_3_WEEKS'] > 0) | \
        (X['expedia_user_has_minfo_EXPEDIA_USER_HAS_MINFO_YES'] > 0) | \
        (X['sessionizer_SESSION_COUNT'] > 0)

    def fit(self, X, y):
        if 'DataFrame' not in str(type(X)):
            X = self._get_data_frame_with_dummy_index(X)
        self._models = []
        split_model_0 = copy.copy(self.sklearn_model)
        X_relevant = X[self.is_old_user(X)]
        y_relevant = y[self.is_old_user(X)]
        split_model_0.fit(X_relevant, y_relevant)
        split_model_1 = copy.copy(self.sklearn_model)
        X_relevant = X[~self.is_old_user(X)]
        y_relevant = y[~self.is_old_user(X)]
        split_model_1.fit(X_relevant, y_relevant)
        self._models.append(split_model_0)
        self._models.append(split_model_1)

    def predict(self, X):
        if 'DataFrame' not in str(type(X)):
            X = self._get_data_frame_with_dummy_index(X)
        result = pd.Series(np.zeros(len(X)))
        result_to_add = pd.Series(self._models[0].predict(X))
        result_to_add[~self.is_old_user(X).values] = 0
        result += result_to_add.values
        result_to_add = pd.Series(self._models[1].predict(X))
        result_to_add[self.is_old_user(X).values] = 0
        result += result_to_add.values
        return result.values

    def _get_data_frame_with_dummy_index(self, X):
        cols = self.test_train.X_train.columns
        return pd.DataFrame(X, columns=cols, index=range(X.shape[0]))

