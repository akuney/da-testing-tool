import pandas as pd

class ModelAggregator(object):
    """
        Class that takes a list of models to train (in the form of
        GridSearchCV objects or model objects), trains them, and outputs the
        results using a list of Plotter objects.

        Parameters
        ----------
        test_train : datasci.io.TestTrainData object, the data that will
            be used in fitting models, creating plots, creating charts
        plotters : list of datasci.plotters.GenericPlotter objects that
            create output data and plots given a model fit.
        output_directory : string, directory name for output results.
        models : list of sklearn model objects or datasci.models objects.
        grid_searches : list of sklearn.grid_search.GridSearchCV objects.
            Parameterized with lists of hyperparameters to evaluate.
            The best_estimator will be used from each of these grid searches
            when making plots and outputting charts.
    """
    def __init__(self, test_train, plotters, output_directory,
                 models=None, grid_searches=None):
        self.test_train = test_train
        if grid_searches is None:
            self.grid_searches = []
        else:
            self.grid_searches = grid_searches
        if models is None:
            self.models = []
        else:
            self.models = models
        self.plotters = plotters
        self.output_directory = output_directory

    def fit_models(self):
        X_train = self.test_train.X_train
        y_train = self.test_train.y_train

        for model in self.models:
            model.fit(X_train.values, y_train.values)
        for grid_search in self.grid_searches:
            grid_search.fit(X_train.values, y_train.values)

    """
        function: graph_results

        Parameters
        ----------
        test_set_split_labels : list, labels for each of the rows in the
            test data.  If None, all rows are assumed to have the same
            label.
    """
    def graph_results(self, test_set_split_labels=None):
        y_evaluation_column_list =\
            self.test_train.y_evaluation.columns
        for col in y_evaluation_column_list:
            result_df = self.get_test_result_df_for_evaluation_column(col)
            user_index = self.get_user_index()

            for plotter in self.plotters:
                plotter.do_plot(self.output_directory, result_df, 'y_values',
                                user_index, test_set_split_labels,
                                col)

    def get_test_result_df_for_evaluation_column(self, y_evaluation_column):
        X_test = self.test_train.X_test
        y_test = self.test_train.y_evaluation[y_evaluation_column]
        result_df = pd.DataFrame({'y_values': y_test.values},
                                 index=y_test.index)
        for model in self.models:
            result_df[str(model)] = model.predict(X_test.values)
        best_estimators = [grid_search.best_estimator_ for grid_search in
                           self.grid_searches]
        for estimator in best_estimators:
            result_df[str(estimator)] = estimator.predict(X_test.values)
        return result_df

    def get_user_index(self):
        return self.test_train.index_test.copy()
