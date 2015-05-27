import datetime
import matplotlib
import matplotlib.pyplot as plt
import sklearn
import numpy as np
import pandas as pd
import pickle
import math
import os

class GenericPlotter(object):
    def __init__(self):
        pass

    """
        function: do_plot

        inputs: path_name : string, location for storing the .png and
            .csv files
        model_predictions : pandas DataFrame, with one column for each set
            of model predictions.  Also, one column with the actual set of
            y values that the models are predicting.
        y_value_col_name : string, name of the column with the actual set
            of y values that the models are predicting.
        user_index : list, unique identifiers for users in the
            model_predictions DataFrame.  Could be publisher_user_ids,
            webuser_ids, etc.  Not used by all Plotters.
        test_set_split_labels : list, a set of labels for the test set of
            data.  Each unique set of labels will be output in a different
            plot in the output.  For example, to plot model results for
            'new users' against 'old users', a label of 'new user' or
            'old user' could be passed in for each ad call here.
    """
    def do_plot(self, path_name, model_predictions, y_value_col_name,
                user_index, test_set_split_labels=None,
                column_name_to_print=None):
        self._do_plot_impl(path_name, model_predictions,
                y_value_col_name, user_index,
                test_set_split_labels,
                column_name_to_print)

class LiftCurvePlotter(GenericPlotter):
    """
        Plots lift curves for a DataFrame in a .png file
        Also creates a .csv file with the lift values at a list of points

        Parameters
        ----------
        resolution : int, number of points on the x-axis in the lift
            curve chart
        lift_chart_points : list, the x-axis points for which we want to
            display lift values
    """
    def __init__(self, resolution=None, lift_chart_points=None):
        super(LiftCurvePlotter, self).__init__()
        if resolution is None:
            self._resolution = 10000
        else:
            self._resolution = resolution
        if lift_chart_points is None:
            self._lift_chart_points = [.05, .10, .15, .20, .25, .30]
        else:
            self._lift_chart_points = lift_chart_points

    def _do_plot_impl(self, path_name, model_predictions, y_value_col_name,
                user_index, test_set_split_labels=None,
                column_name_to_print=None):
        test_set_split_labels = self._get_test_split_labels(
            test_set_split_labels, model_predictions)
        model_predictions_normalized_y = self.\
            _get_model_predictions_normalized(
            model_predictions, y_value_col_name)
        lift_df = self._get_lift_df(model_predictions_normalized_y,
                                    y_value_col_name, test_set_split_labels)
        label_string = self._get_label_string(test_set_split_labels,
                                              column_name_to_print)
        self._draw_and_save_plots(lift_df, path_name, label_string)

    def _get_test_split_labels(self, test_set_split_labels, model_predictions):
        if test_set_split_labels is None:
            return np.zeros(model_predictions.shape[0], dtype=np.int)
        else:
            return test_set_split_labels

    def _get_model_predictions_normalized(self, model_predictions,
                                          y_value_col_name):
        model_predictions_normalized_y = pd.DataFrame(model_predictions,
                         index=model_predictions[y_value_col_name].index)
        model_predictions_normalized_y[y_value_col_name] =\
            model_predictions_normalized_y[y_value_col_name] /\
            model_predictions_normalized_y[y_value_col_name].sum()
            # normalize y_value_col_name column
        return model_predictions_normalized_y

    def _get_lift_df(self, model_predictions_normalized_y, y_value_col_name,
                     test_set_split_labels):
        lift_df = pd.DataFrame(index=np.array(range(self._resolution))/
                                     float(self._resolution))

        for label in np.unique(test_set_split_labels):
            model_predictions_this_label = model_predictions_normalized_y[
                test_set_split_labels == label]
            model_predictions_this_label[y_value_col_name] = \
                model_predictions_this_label[y_value_col_name] /\
                model_predictions_this_label[y_value_col_name].sum()
            lift_label_index = np.array(range(
                model_predictions_this_label.shape[0])) /\
                               float(len(model_predictions_this_label))
            for col in model_predictions_this_label.columns:
                if col == y_value_col_name:
                    col_name = 'perfect_sort_split_label_' + str(label)
                else:
                    col_name = col + '_split_label_' + str(label)
                col_lift_series = pd.Series(np.array(
                    model_predictions_this_label.copy().sort_index(by=col,
                    ascending=False).cumsum()[y_value_col_name]),
                                            index=lift_label_index)
                lift_df[col_name] = self._resample_to_resolution_size(
                    col_lift_series, self._resolution)
        return lift_df

    def _get_label_string(self, test_set_split_labels,
                          column_name_to_print):
        if len(np.unique(test_set_split_labels)) > 1:
            label_string = '_'.join([str(val) for val in np.unique(
                test_set_split_labels)]) + '_' + column_name_to_print
        else:
            label_string = '_' + column_name_to_print
        return label_string

    def _resample_to_resolution_size(self, col_lift_series, resolution):
        resolution_index = np.array(range(resolution)) / float(resolution)
        return col_lift_series.reindex(resolution_index, method='ffill')

    def _draw_and_save_plots(self, lift_df, path_name, label_string):
        plt.close()
        ax = lift_df.plot(title='Lift Curves', ylim=[0,1])
         # modify this to print to a pdf
        ax.add_line(matplotlib.lines.Line2D([0,1],[0,1], linestyle='dashed',
                                            color='g'))
        plt.legend(loc='lower right', prop={'size':6})
        plt.savefig(path_name + 'Lift_Curves' + label_string + '.png',
                    format='png')

        idx = pd.Index([int(math.floor(lcp*self._resolution)) for lcp in
                        self._lift_chart_points])
        lift_chart_df = pd.DataFrame(lift_df.ix[idx])
        lift_chart_df.index = self._lift_chart_points
        lift_chart_df.to_csv(path_name + 'Lift_Chart' + label_string + '.csv',
                             float_format='%.2f')


class HVBookingsProtectedPlotter(GenericPlotter):
    """
        Creates two plots:
        1. The number of users who are HV on all ad calls they see for that
          day, plotted by day.
        2. Same as #1, but averaged over all days for that model.  A user's
          HV status is determined the same way as in plot 1: they are HV
          only if they were HV for each ad call on a given day.  If a user
          is seen across many days, one observation is considered for each
          day.

        Parameters
        ----------
        ad_call_pct_in_hc : float, percentage of ad calls that are 'HC'
    """
    def __init__(self, ad_call_pct_in_hc=0.25):
        super(HVBookingsProtectedPlotter, self).__init__()
        self._ad_call_pct_in_hc = ad_call_pct_in_hc

    def _do_plot_impl(self, path_name, model_predictions, y_value_col_name,
                user_index, test_set_split_labels=None,
                column_name_to_print=None):
        test_set_split_labels = self._get_test_split_labels(
            test_set_split_labels, model_predictions)
        hv_total, hv_by_day_df = self._get_hv_dfs(model_predictions,
            y_value_col_name, user_index, self._ad_call_pct_in_hc,
            test_set_split_labels)
        label_string = self._get_label_string(test_set_split_labels,
                                              column_name_to_print)
        self._draw_and_save_plots(hv_by_day_df, hv_total, path_name,
                                  label_string)

    def _get_test_split_labels(self, test_set_split_labels, model_predictions):
        if test_set_split_labels is None:
            return np.zeros(model_predictions.shape[0], dtype=np.int)
        else:
            return test_set_split_labels

    def _get_label_string(self, test_set_split_labels,
                          column_name_to_print):
        if len(np.unique(test_set_split_labels)) > 1:
            label_string = '_'.join([str(val) for val in np.unique(
                test_set_split_labels)]) + '_' + column_name_to_print
        else:
            label_string = ''
        return label_string

    def _draw_and_save_plots(self, hv_by_day_df, hv_total, path_name,
                             label_string):
        plt.close()
        hv_by_day_df.plot(title='Bookers protected in HV by day', ylim=[0,1])
        plt.legend(loc='best', prop={'size':6})
        plt.savefig(path_name + 'Bookers_in_HV_by_day_' + label_string +
                    '.png', format='png')

        plt.close()
        fig_1 = plt.figure(1)
        fig_1.suptitle('Bookers proteced in HV, overall average')
        fig_1.subplots_adjust(bottom=0.4)
        fig_1.subplots_adjust(left=0.4)

        ax = fig_1.add_subplot(111)
        ax.bar(np.arange(len(hv_total)), hv_total.values)
        ax.tick_params(labelsize=6)
        plt.xticks(np.arange(len(hv_total)), hv_total.index, rotation=25)
        for label in ax.get_xticklabels():
            label.set_horizontalalignment('right')
        plt.savefig(path_name + 'Bookers_in_HV_overall_' + label_string +
                    '.png', format='png')

        plt.close()

    def _get_hv_dfs(self, preds_and_y_values, y_value_col_name, user_index,
                    ad_call_pct_in_hc, test_set_split_labels=None):
        ad_call_pct_in_lc_plus_mix = 1 - ad_call_pct_in_hc

        days = pd.Series(preds_and_y_values.index.get_level_values('dates')
            .to_period('D')).unique()
        hv_by_day_df = pd.DataFrame(index=days)
        hv_total = {}

        for label in np.unique(test_set_split_labels):
            label_preds_and_y_values = preds_and_y_values[
                test_set_split_labels== label]
            user_bookings_by_day_this_label = \
                self._get_user_bookings_by_day_this_label(
                    label_preds_and_y_values, y_value_col_name,
                    ad_call_pct_in_lc_plus_mix, user_index)

            for col in label_preds_and_y_values.columns:
                if col != 'y_values':
                    col_name = col + '_label_' + str(label)
                    hvs_by_day = user_bookings_by_day_this_label['y_values'].\
                        groupby([user_bookings_by_day_this_label.index.
                        get_level_values(0), user_bookings_by_day_this_label
                        [col]]).agg(['sum', 'size']).unstack()
                    hv_by_day_df[col_name] = hvs_by_day['sum'][0] /\
                                (hvs_by_day['sum'][0] + hvs_by_day['sum'][1])
                    total_df = hvs_by_day.sum()
                    hv_total[col_name] = total_df['sum'][0] /\
                                (total_df['sum'][0] + total_df['sum'][1])
        hv_total = pd.Series(hv_total)
        hv_by_day_df.index = pd.DatetimeIndex([datetime.date(1970, 1, 1) +
                       datetime.timedelta(days=int(day)) for day in days])

        return hv_total, hv_by_day_df

    def _get_user_bookings_by_day_this_label(self, label_preds_and_y_values,
                 y_value_col_name, ad_call_pct_in_lc_plus_mix, user_index):
        days_this_label = label_preds_and_y_values.index\
            .get_level_values('dates').to_period('D')
        is_ever_lc_and_y_values = pd.DataFrame(
            index=label_preds_and_y_values.index)

        for col in label_preds_and_y_values.columns:
            if col == y_value_col_name:
                is_ever_lc_and_y_values[col] = label_preds_and_y_values[col]
            else:
                is_ever_lc_and_y_values[col] = np.zeros(
                    len(is_ever_lc_and_y_values))
                hc_threshold = label_preds_and_y_values[col].quantile(
                    ad_call_pct_in_lc_plus_mix)
                is_ever_lc_and_y_values[col][label_preds_and_y_values[col]
                                             < hc_threshold] = 1

        user_bookings_by_day_this_label = is_ever_lc_and_y_values.groupby(
            [days_this_label, user_index]).sum()
        user_bookings_by_day_this_label[user_bookings_by_day_this_label > 0]\
            = 1
        return user_bookings_by_day_this_label
