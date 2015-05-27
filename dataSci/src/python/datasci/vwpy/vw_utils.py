def safe_remove(f):
    import os
    try:
        os.remove(f)
    except OSError:
        pass

"""
Adapted from https://github.com/josephreisinger/vowpal_porpoise/
Basic logger functionality; replace this with a real logger of your choice
"""
import imp
import sys
import gzip
import datasci.vwpy
import datasci.plotting
import pandas as pd


class VPLogger:
    def debug(self, s):
        print '[DEBUG] %s' % s

    def info(self, s):
        print '[INFO] %s' % s

    def warning(self, s):
        print '[WARNING] %s' % s

    def error(self, s):
        print '[ERROR] %s' % s


def import_non_local(name, custom_name=None):
    """Import when you have conflicting names"""
    custom_name = custom_name or name

    f, pathname, desc = imp.find_module(name, sys.path[1:])
    module = imp.load_module(custom_name, f, pathname, desc)
    if f:
      f.close()

    return module


def get_base_dir(file_path):
    last_slash_index = len(file_path) - file_path[::-1].index('/')
    return file_path[:last_slash_index]


def get_file_name(file_path):
    last_slash_index = len(file_path) - file_path[::-1].index('/')
    return file_path[last_slash_index:]


def get_preds_and_actuals(train_file, test_file, loss, regularization, moniker, passes):
    train_working_dir = get_base_dir(train_file)
    file_name = get_file_name(train_file)
    vw = datasci.vwpy.vw.VW(moniker=moniker,
                            name=file_name,
                            working_dir=train_working_dir,
                            loss=loss,
                            l2=regularization,
                            passes=passes)
    with vw.training(), gzip.open(train_file) as fin:
        for line in fin:
            vw.push_instance(line.strip())  # strip() to avoid single line being counted as two examples

    actual_values = []
    with vw.predicting(), gzip.open(test_file) as fin:
        for line in fin:
            actual_values.append(line[:line.index(' ')])
            vw.push_instance(line.strip())  # strip() to avoid single line being counted as two examples

    predictions = list(vw.read_predictions_())
    return pd.DataFrame({'act': actual_values, 'pred': predictions})


class ClassifierModelEvaluator(object):
    def __init__(self, working_dir, moniker, passes=1, num_processes=1):
        self._name = 'classifer'
        self._loss = 'logistic'
        self._passes = passes
        self._working_dir = working_dir
        self._moniker = moniker
        self._num_processes = num_processes

    def _evaluate_preds(self, df, regularization_factor, file_name):
        datasci.plotting.utils.plot_roc_curve(df, self._working_dir, '_'.join([self._name, self._moniker, file_name,
                                              regularization_factor]))

    def evaluate_model(self, train_files, test_files, regularization_factors):
        for train_file, test_file in zip(train_files, test_files):
            for regularization_factor in regularization_factors:
                df = get_preds_and_actuals(train_file, test_file, self._loss, regularization_factor, self._moniker,
                                           self._passes)
                df['act'] = df['act'].astype('int')
                df['act'][df['act'] < 0] = 0
                self._evaluate_preds(df, str(regularization_factor), get_file_name(train_file))


class ContinuousModelEvaluator(object):
    def __init__(self, working_dir, moniker, passes=1, num_processes=1):
        self._name = 'continuous'
        self._loss = 'squared'
        self._passes = passes
        self._working_dir = working_dir
        self._moniker = moniker
        self._num_processes = num_processes

    def _evaluate_preds(self, df, regularization_factor, file_name):
        datasci.plotting.utils.plot_scatter(df, self._working_dir, '_'.join([self._name, self._moniker, file_name,
                                            regularization_factor]))

    def evaluate_model(self, train_files, test_files, regularization_factors):
        for train_file, test_file in zip(train_files, test_files):
            for regularization_factor in regularization_factors:
                df = get_preds_and_actuals(train_file, test_file, self._loss, regularization_factor, self._moniker,
                                           self._passes)
                df['act'] = df['act'].astype('float')
                self._evaluate_preds(df, str(regularization_factor), get_file_name(train_file))
