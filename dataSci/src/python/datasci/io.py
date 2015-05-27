import sys
import pandas as pd
import datetime
import os
import numpy as np
import csv
import itertools
import re

def parse_dates_from_iso(dt):
    import datetime
    return datetime.datetime.strptime(dt, '%Y-%m-%dT%H:%M:%S.%fZ')


def parse_dates_from_unix_div_by_1000(dt):
    """
        divide by 1000 to match millisecond format used in Intent Media hadoop
            output
    """
    import datetime
    return datetime.datetime.fromtimestamp(float(dt)/1000)

class TrainTestData(object):
    def __init__(self, X_train, X_test, y_train, y_test, index_train=None,
                 index_test=None, y_evaluation=None):
        self.X_train = X_train
        self.X_test = X_test
        self.y_train = y_train
        self.y_test = y_test
        self.index_train = index_train
        self.index_test = index_test
        self.y_evaluation = y_evaluation

    def get_num_features(self):
        return self.X_train.shape[1]

    def get_feature_column_names(self):
        return self.X_train.columns

    def get_num_train_records(self):
        return self.X_train.shape[0]

    def get_train_index_names(self):
        return self.X_train.index

    def get_num_test_records(self):
        return self.X_test.shape[0]

    def get_test_index_names(self):
        return self.X_test.index

    def get_y_evaluation(self):
        return self.y_evaluation

class DataIOTestTrain(object):
    """
        Class for loading train/test datasets and transforming them.
        Produces a TrainTestData data structure

        INPUTS
        * data_io_train: io object, used to source the training data
        * data_io_test: io object, used to source the test data
        * X_transformer_list: list, transformers that return modified versions
            of the X values
        * all_transformer_list: list, transformers that return modified
            versions of the X values, y values, and index values
            Note: X_transformer_list is kept as a separate list to allow
            compatibility with existing sklearn transformers that
            only return X values, and for clarity as to what is being
            transformed.
        * y_column_name: string, name of y-value column
        * index_column_name: string, name of the index column
        * y_columns_evaluation_list : list, names of y columns to evaluate
            the model against.  If None, the y_column_name will be used
            for evaluation.
    """
    def __init__(self, data_io_train, data_io_test, X_transformer_list=None,
                 all_transformer_list=None,
                 y_column_name=None, index_column_name=None,
                 y_columns_evaluation_list=None):
        self.data_io_train = data_io_train
        self.data_io_test = data_io_test
        if X_transformer_list is None:
            self.X_transformer_list = []
        else:
            self.X_transformer_list = X_transformer_list
        if all_transformer_list is None:
            self.all_transformer_list = []
        else:
            self.all_transformer_list = all_transformer_list
        self.y_column_name = y_column_name
        self.index_column_name = index_column_name
        if y_columns_evaluation_list is None:
            self.y_columns_evaluation_list = []
        else:
            self.y_columns_evaluation_list = y_columns_evaluation_list

    def get_train_test_data(self):
        X_train = self.data_io_train.load_data_frame()
        X_test = self.data_io_test.load_data_frame()

        if self.y_column_name is None:
            y_train = None
            y_test = None
        else:
            y_train = X_train[self.y_column_name]
            y_test = X_test[self.y_column_name]

        if len(self.y_columns_evaluation_list) == 0:
            y_evaluation = pd.DataFrame(X_test[self.y_column_name],
                                index=X_test[self.y_column_name].index)
        else:
            y_evaluation = pd.DataFrame(X_test[self.y_columns_evaluation_list],
                                index=X_test[self.y_column_name].index)

        if self.index_column_name is None:
            index_column_train = None
            index_column_test = None
        else:
            index_column_train = X_train.pop(self.index_column_name)
            index_column_test = X_test.pop(self.index_column_name)

        for all_transformer in self.all_transformer_list:
            all_transformer.fit(X_train, y_train, index_column_train)
            X_train, y_train, index_column_train, _ =\
                all_transformer.transform(
                X_train, y_train, index_column_train, None)
        for all_transformer in self.all_transformer_list:
            X_test, y_test, index_column_test, y_evaluation =\
                all_transformer.transform(
                X_test, y_test, index_column_test, y_evaluation)

        for X_transformer in self.X_transformer_list:
            X_transformer.fit(X_train)
            X_train = X_transformer.transform(X_train)
        for X_transformer in self.X_transformer_list:
            X_test = X_transformer.transform(X_test)

        return TrainTestData(X_train, X_test, y_train, y_test,
                             index_column_train, index_column_test,
                             y_evaluation)

class DataIO(object):
    """
        Class for loading a dataset, providing
        functionality for loading column names
        from .column_names file
    """
    def __init__(self, path_to_data, nrows=None,
                 index_col_name=None, parse_dates=True,
                 date_parser=parse_dates_from_iso, sep='\t',
                 add_binary_columns=False,
                 add_multi_index=False):
        self.path_to_data = path_to_data
        self.nrows = nrows
        self.index_col_name = index_col_name
        self.parse_dates = parse_dates
        self.date_parser = date_parser
        self.sep = sep
        self.add_binary_columns = add_binary_columns
        self.add_multi_index = add_multi_index

    def get_data(self):
        """
            reads a file and populates column names based on .pig_schema file
            (if .pig_schema exists) this function will read all files in the
            path_to_data directory (ignoring dotfiles, hadoop-style)

            A MultiIndex is used so that index values are unique
            even if multiple ad calls have the same Timestamp
        """
        if os.path.isdir(self.path_to_data):
            folder_path = self.path_to_data
        else:
            folder_path = os.path.dirname(self.path_to_data)
        column_names = self._get_column_names(folder_path)
        if self.add_binary_columns:
            binary_columns = ['request_id', 'requested_at_iso']
            column_names = binary_columns + column_names

        if self.index_col_name in column_names:
            index_col = column_names.index(self.index_col_name)
        else:
            index_col = None
            if self.index_col_name is not None:
                for idx, idx_col_name in enumerate(column_names):
                    if self.index_col_name in idx_col_name:
                        index_col = idx

        if os.path.isdir(self.path_to_data):
            df = pd.DataFrame(columns = column_names)
            file_list = os.listdir(self.path_to_data)
            for file in file_list:
                if file != '.column_names':
                    if self.nrows is not None and len(df) >= self.nrows:
                        df = df[:self.nrows]
                        break
                    else:
                        df_new = pd.read_csv(os.path.join(self.path_to_data, file),
                                             sep='\t', header=None, nrows=self.nrows,
                                             index_col=index_col)
                        df_new.columns = column_names
                        df = df.append(df_new)
            df.index = range(len(df))
        else:
            df = pd.read_csv(self.path_to_data, sep='\t', header=None,
                             nrows=self.nrows, index_col=index_col)
            df.columns = column_names

        if self.add_multi_index:
            df.index = pd.MultiIndex.from_tuples(
                zip(df.index, range(len(df))),
                names=['dates', 'nums'])
        return df

    def _get_column_names(self, path_to_data):
        f = open(os.path.join(path_to_data, '.column_names'))
        columns_unsplit = f.readline()
        columns = columns_unsplit.split('\t')
        f.close()
        return columns
