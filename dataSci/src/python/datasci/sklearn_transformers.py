import pandas as pd
import numpy as np
import scipy
import sklearn
import sklearn.base
import sklearn.preprocessing
import copy
import datetime

class SklearnScalerWrapper(sklearn.base.TransformerMixin):
    """
        Wrapper for the sklearn preprocessors that leaves the type of a
            pandas DataFrame as is, i.e. does not convert it to a numpy array.

    """
    def __init__(self, sklearn_scaler):
        self.sklearn_scaler = copy.copy(sklearn_scaler)

    def fit(self, X, y=None):
        self.sklearn_scaler.fit(X, y)

    def transform(self, X, y=None, copy=None):
        columns = X.columns
        index = X.index
        as_array = self.sklearn_scaler.transform(X, y, copy)
        return pd.DataFrame(as_array, index=index, columns=columns)

class CapYValues():
    def __init__(self, y_value_cap):
        self.y_value_cap = y_value_cap

    def fit(self, X, y=None, index=None):
        pass

    def transform(self, X, y, index):
        y[y > self.y_value_cap] = self.y_value_cap
        return X, y, index

class DateBoundaryRecordRemover():
    def __init__(self, start_date_inclusive=None, end_date_inclusive=None,
                 copy=True):
        if start_date_inclusive is None:
            self.start_date_inclusive = datetime.datetime.combine(
                datetime.date.min, datetime.time())
        else:
            self.start_date_inclusive = start_date_inclusive
        if end_date_inclusive is None:
            self.end_date_inclusive = datetime.datetime.combine(
                datetime.datetime.max, datetime.time())
        else:
            self.end_date_inclusive = end_date_inclusive

    def fit(self, X, y=None, index=None, y_evaluation=None):
        pass

    def transform(self, X, y, index, y_evaluation=None):
        idx = X.index
        vals_to_keep = [True if i[0] <= self.end_date_inclusive
                        else False for i in idx]
        X = X[vals_to_keep]
        y = y[vals_to_keep]
        if y_evaluation is not None:
            y_evaluation = y_evaluation[vals_to_keep]
        index = index[vals_to_keep]

        idx = X.index
        vals_to_keep = [True if i[0] >= self.start_date_inclusive
                        else False for i in idx]
        X = X[vals_to_keep]
        y = y[vals_to_keep]
        index = index[vals_to_keep]
        if y_evaluation is not None:
            y_evaluation = y_evaluation[vals_to_keep]

        return X, y, index, y_evaluation


class ColumnNameFeaturesRemover(sklearn.base.TransformerMixin):
    """
        Removes columns with specific names from a DataFrame
        inherits from sklearn.base.TransformerMixin so can be used in an
            sklearn.Pipeline

        Parameters
        ----------
        cols_to_remove : set, names of columns to be removed
        copy : boolean, optional, default is True
        True if it returns a copy of the input data on transform
        False if transform happens in-place
    """
    def __init__(self, cols_to_remove, copy=True):
        self.cols_to_remove = cols_to_remove
        self.copy = copy

    def fit(self, X, y=None):
        if type(X) != pd.core.frame.DataFrame:
            raise ValueError("ColumnNameFeaturesRemover: pandas DataFrame"
                             "expected.")

    def transform(self, X, y=None, copy=None):
        if self.copy:
            X = X.copy()
        for col in self.cols_to_remove:
            if col in X:
                del X[col]
        return X


class DTypeObjectFeaturesRemover(sklearn.base.TransformerMixin):
    """
        removes columns of dtype Object from a DataFrame
        inherits from sklearn.base.TransformerMixin so can be used in an
        sklearn.Pipeline

        Parameters
        ----------
        copy : boolean, optional, default is True
        True if it returns a copy of the input data on transform
        False if transform happens in-place
    """
    def __init__(self, copy=True):
        self.copy = copy
        self._cols_to_remove = None

    def fit(self, X, y=None):
        if type(X) != pd.core.frame.DataFrame:
            raise ValueError("ObjectFeaturesRemover: pandas DataFrame"
                             "expected.")
        self._cols_to_remove = set()
        for col in X.columns:
            if X[col].dtype == 'object' or X[col].dtype == 'O':
                self._cols_to_remove.add(col)

    def transform(self, X, y=None, copy=None):
        if self.copy:
            X = X.copy()
        for col in self._cols_to_remove:
            if col in X:
                del X[col]
        return X

class DependentFeaturesRemover(sklearn.base.TransformerMixin):
    """
        goes through num_rows number of data records
        performs a QR decomposition
        removes any signals that correspond to eigenvalues less than
        eigenvalue_threshold

        inherits from sklearn.base.TransformerMixin so can be used in an
        sklearn.Pipeline

        Parameters
        ----------
        copy : boolean, optional, default is True
            True if it returns a copy of the input data on transform
            False if transform happens in-place
        eigenvalue_ratio_threshold : float, divide a column's eigenvalue by the
            maximum eigenvalue.  if the result is less than this threshold, the
            column will be removed
        num_rows : int, exists so that the user may save time by only scanning
            a limited number of rows in the dataset when determining
            independence of columns
        cols_to_keep : list, names of columns to keep even if they are linearly
            dependent on other columns

    """
    def __init__(self, copy=True, eigenvalue_ratio_threshold=1e-9,
                 num_rows=1e6, cols_to_keep=None):
        self.copy = copy
        self.eigenvalue_ratio_threshold = eigenvalue_ratio_threshold
        self.num_rows = num_rows
        if cols_to_keep is None:
            self._cols_to_keep = []
        else:
            self._cols_to_keep = cols_to_keep
        self._cols_to_remove = None

    def fit(self, X, y=None):
        if type(X) != pd.core.frame.DataFrame:
            raise ValueError("DependentFeaturesRemover: pandas DataFrame" +\
                             "expected.")
        self._cols_to_remove = set()
        XtX = np.dot(X[:self.num_rows].T, X[:self.num_rows])
        q,r,p = scipy.linalg.qr(XtX, pivoting=True)

        eigs = scipy.linalg.eig(XtX)[0]
        max_eig = np.max(np.abs(eigs)) # change to find eig with maximum value
        close_to_zero_eigs = 0
        for eig in eigs:
            if (np.abs(eig) / max_eig) < self.eigenvalue_ratio_threshold:
                close_to_zero_eigs += 1

        # select the columns to keep from p based on the pivoted qr
        # decomposition results
        cols_and_names = [(p_val, X.columns[p_val]) for p_val in p]
        # cols_to_keep = cols_and_names[:-close_to_zero_eigs]
        if close_to_zero_eigs > 0:
            cols_to_remove = cols_and_names[-close_to_zero_eigs:]
        self._cols_to_remove = set([col[1] for col in cols_to_remove if col[1]\
            not in self._cols_to_keep])

    def transform(self, X, y=None, copy=None):
        if self.copy:
            X = X.copy()
        for col in self._cols_to_remove:
            if col in X:
                del X[col]
        return X

