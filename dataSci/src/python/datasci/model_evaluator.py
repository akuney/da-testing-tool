import pandas as pd
import numpy as np
import sklearn
import sklearn.linear_model
import sklearn.metrics
import subprocess
import datetime
import ast
import os
import re
import tempfile
import pylab as pl

class SimpleLogisticModel(sklearn.base.RegressorMixin, sklearn.base.BaseEstimator):
    def __init__(self, betas, intercept):
        self._betas = betas
        self._intercept = intercept

    def coef_(self):
        return self._betas

    def fit(self, X, y):
        raise Exception("method not implemented!")

    def predict(self, X):
        exponent_values = np.dot(X, self._betas) + self._intercept * np.ones(X.shape[0])
        return 1 / (1 + np.exp(-exponent_values))

class GeneralClassifierModel(sklearn.base.RegressorMixin, sklearn.base.BaseEstimator):
    def __init__(self, model_name):
        self._model_name = model_name

    # depending on the model, will load from a training set or
    # load from a model definition file
    def load(self, file):
        pass

    def _get_betas(self):
        return self._model.coef_()

    def evaluate(self, test_data_location, vw_test_data_location=None, sep='\t', cols_to_remove=None):
        betas = self._get_betas()
        f = open('/Users/jon.sondag/Datasets/' + self._model_name + '_betas', 'w')
        #f.write(str(betas.tolist()))
        f.write(str(betas))
        f.close()

        if test_data_location is not None:
            test_df_X, test_df_y = self._get_test_df_for_evaluation(test_data_location, sep, cols_to_remove)
        elif vw_test_data_location is not None:
            test_df_X, test_df_y = self._get_test_df_for_evaluation(vw_test_data_location, sep, cols_to_remove)
        else:
            raise Exception("either test_data_location or vw_test_data_location must be not None")
        predicted_values = self._get_predictions(test_df_X, vw_test_data_location)
        auc_score = sklearn.metrics.auc_score(test_df_y, predicted_values)
        print 'model name:', self._model_name + ',', 'auc score:', auc_score

    def _get_df_all_files_in_dir(self, test_data_location, sep):
        file_list = os.listdir(test_data_location)
        file_path_list = [os.path.join(test_data_location, file) for file in
                          file_list if file[0] != '.']
        df_list = [pd.io.parsers.read_csv(
            file_path, header=None, sep=sep, quoting=3)
                   for file_path in file_path_list] # skip quotes in training data
        df = pd.concat(df_list)
        return df

    def _get_test_df_for_evaluation(self, test_data_location, sep='\t', cols_to_remove=None):
        test_df = self._get_df_all_files_in_dir(test_data_location, sep)
        y = test_df.pop(len(test_df.columns) - 1)
        y = y.apply(lambda x: x[:-1])
        y = y.astype(float)
        y[y > 0] = 1
        y[y < 0] = 0
        if cols_to_remove is not None:
            for col in cols_to_remove:
                del test_df[col]
        return test_df, y

class MahoutClassifierModel(GeneralClassifierModel):
    # load the model from a mahout output file
    # TODO: un-hardcode num_variables
    def load(self, file):
        f = open(file)
        line = f.readline()
        model_dict = ast.literal_eval(line)
        intercept = model_dict[0]
        num_variables = 258 # max(model_dict.keys()) # no need to add 1, b/c of intercept
        betas = np.zeros(num_variables)
        for k, v in model_dict.items():
            if k != 0:
                betas[k - 1] = model_dict[k]
        f.close()
        self._model = SimpleLogisticModel(betas, intercept)

    def _get_predictions(self, X, vw_test_data_location=None):
        return self._model.predict(X)

class AdmmClassifierModel(GeneralClassifierModel):
    def _get_z_initial(self, file_path_betas):
        f = open(file_path_betas)
        line = f.readline()

        match = re.search(r'zInitial[^\[]*\[([^\]]+)', line)
        match_str = match.group(1)
        split_match = match_str.split(',')

        zInitial = np.array(split_match, dtype=float)
        return zInitial

    def _get_high_indices(self, file_path_std_errs, zInitial, default_std_err):
        f = open(file_path_std_errs)
        line = f.readline()
        line = line[3:-2]
        split_line = line.split(',')
        split_line = [val if val != "\"NaN\"" else default_std_err for val in split_line]
        split_line = [val if val != 0.0 else default_std_err for val in split_line]

        std_errs = np.array(split_line, dtype=float)
        std_errs_ser = pd.Series(std_errs)
        betas_ser = pd.Series(zInitial)
        ratios_ser = np.abs(betas_ser) / std_errs_ser
        # ratios_ser.plot()
        high_indices = ratios_ser.index.values

        return high_indices, betas_ser, std_errs_ser

    def _get_split_line_new(self, file_path_pig_header):
        f = open(file_path_pig_header)
        line = f.readline()
        split_line = line.split('\t')
        split_line_new = ['intercept']
        for split in split_line:
            if split != 'request_id' and split != 'requested_at_iso' and split!= 'publisher_user_id':
                split_line_new.append(split)

        return split_line_new

    def get_admm_result_df(self, default_std_err, file_path_betas, file_path_std_errs, file_path_pig_header):
        zInitial = self._get_z_initial(file_path_betas)
        high_indices, betas_ser, std_errs_ser = self._get_high_indices(file_path_std_errs, zInitial, default_std_err)
        split_line_new = self._get_split_line_new(file_path_pig_header)

        high_indices_names = [(val[1], betas_ser.values[val[0]], std_errs_ser.values[val[0]])\
                              for val in enumerate(split_line_new) if val[0] in high_indices]
        my_df = pd.DataFrame(high_indices_names)
        my_df.columns = ['name', 'beta', 'std_err']
        my_df['z_score'] = np.abs(my_df['beta']) / my_df['std_err']

        return my_df

    # file in this case is the base directory with the results
    def load(self, file):
        default_std_err = 1e-3
        base_folder = file
        # part-00000 from iteration_final folder
        file_path_betas = base_folder + 'betas_file'
        # part-00000 from standard-error folder, renamed
        file_path_std_errs = base_folder + 'standard-error'
        # .pig_header from generate_signals/training/signals.gz folder
        file_path_pig_header = base_folder + '.pig_header'
        file_path_signal_vars = base_folder + 'signal-vars'

        admm_result_df = self._get_admm_result_df(default_std_err, file_path_betas,\
                                                  file_path_std_errs, file_path_pig_header)

        # get intercept from result_df. The intercept is the first beta - see getZUpdated
        # in AdmmIterationReducer.java
        intercept = admm_result_df['beta'][0].copy()
        # get betas from result_df
        betas_unscaled = admm_result_df['beta'][1:].copy()
        # no need to scale the betas
        betas = betas_unscaled.values

        self._model = SimpleLogisticModel(betas, intercept)

    def _get_predictions(self, X, vw_test_data_location=None):
        return self._model.predict(X)

class SklearnClassifierModel(GeneralClassifierModel):

    def _get_betas(self):
        return self._model.coef_[0]

    # file in this case is the directory of training data
    def load(self, file):
        lambda_val = 0.1 # TODO: allow this to be passed in

        train_df = self._get_df_all_files_in_dir(file, '\t')
        y = train_df.pop(260)
        y = y.apply(lambda x: x[:-1])
        y = y.astype(float)
        y[y > 0] = 1
        del train_df[0]
        del train_df[1]
        X = train_df

        C = 1.0 / lambda_val

        model = sklearn.linear_model.LogisticRegression(C = C, tol=1e-15, intercept_scaling=1e6)
        model.fit(X.as_matrix(), y.values)

        self._model = model

    def _get_predictions(self, X, vw_test_data_location=None):
        return np.array([val[1] for val in self._model.predict_proba(X)])

class PredictClassifierModel(GeneralClassifierModel):

    def _get_betas(self):
        return []

    # file in this case is TBD: can be either
    #  a) training set location; shell script that will create predictions and then we load them, or
    #  b) a file with predictions that have been pre-generated
    def load(self, file):
        self._predictions = pd.read_csv(file, header=None).values.flatten()

    def _get_predictions(self, X, vw_test_data_location=None):
        return self._predictions

    def evaluate(self, test_data_location, vw_test_data_location=None, sep='\t', cols_to_remove=None):
        predicted_values = self._predictions
        test_df = pd.read_csv(test_data_location, header=None, sep='\t')
        y_actual = test_df[1].values

        auc_score = sklearn.metrics.auc_score(y_actual, predicted_values)
        # fpr, tpr, thresholds = sklearn.metrics.roc_curve(y_actual, predicted_values)
        # roc_auc = sklearn.metrics.auc(fpr, tpr)
        # pl.clf()
        # pl.plot(fpr, tpr, label='ROC curve (area = %0.2f)' % roc_auc)
        # pl.plot([0, 1], [0, 1], 'k--')
        # pl.xlim([0.0, 1.0])
        # pl.ylim([0.0, 1.0])
        # pl.xlabel('False Positive Rate')
        # pl.ylabel('True Positive Rate')
        # pl.title('Receiver operating characteristic example')
        # pl.legend(loc="lower right")
        # pl.show()

        print 'model name:', self._model_name + ',', 'auc score:', auc_score

class VWClassifierModel(GeneralClassifierModel):

    def _get_betas(self):
        return []

    # file in this case is a vw .model file, pre-generated
    # TODO: add option to allow this to be generated on our machine directly from training dataset
    def load(self, file):
        self._model_file = file

    def _apply_sigmoid_transform(self, input_path, output_path):
        f_in = open(input_path)
        f_out = open(output_path, 'w')

        for line in f_in.readlines():
            float_val = float(line)
            f_out.write(str(1.0 / (1.0 + np.exp(-float_val))) + '\n')

        f_in.close()
        f_out.close()

    def _get_predictions(self, X, vw_test_data_location=None):
        temp_prediction_file = tempfile.NamedTemporaryFile().name
        output = subprocess.check_output(['vw', '-t', '-d', vw_test_data_location, '-i', self._model_file,\
                                          '-p', temp_prediction_file])
        # print output
        temp_prediction_file_transformed = tempfile.NamedTemporaryFile().name
        self._apply_sigmoid_transform(temp_prediction_file, temp_prediction_file_transformed)
        predictions = pd.read_csv(temp_prediction_file_transformed, header=None)
        return predictions.values

    # test_df_X, test_df_y = self._get_test_df_for_evaluation(vw_test_data_location, sep, cols_to_remove)
    def _get_test_df_for_evaluation(self, vw_test_data_location, sep='\t', cols_to_remove=None):
        test_df = pd.read_csv(vw_test_data_location, sep='|', header=None)
        return None, test_df[0].values

def create_all_betas_df(lambdas):
    df = None

    for file_name in ['betas_mahout', 'betas_admm']:
        betas_path = '/Users/jon.sondag/Datasets/' + file_name
        f = open(betas_path)
        betas_list = ast.literal_eval(f.readline())
        if df is None:
            df = pd.DataFrame(index = range(len(betas_list)))
        df[file_name] = betas_list
        f.close()
    for lambda_val in lambdas:
        file_name = 'betas_sklearn_' + str(lambda_val)
        betas_path = '/Users/jon.sondag/Datasets/' + file_name
        f = open(betas_path)
        betas_list = ast.literal_eval(f.readline())
        if df is None:
            df = pd.DataFrame(index = range(len(betas_list)))
        df[file_name] = betas_list
        f.close()

    df.to_csv('/Users/jon.sondag/Datasets/betas_all.csv')

def convert_floats_to_ints(input_file_path, output_file_path, rearrange_columns=False):
    """
    given a Predict output file, converts floats to ints
    """
    old_ordering = ['IMPRESSION_COUNT', 'CLICK_COUNT', 'ADVERTISER_CATEGORY_TYPE_ORDINAL', 'ADVERTISER_ID', 'AD_COPY_ID', 'AD_GROUP_ID', 'AD_UNIT_ID', 'AD_UNIT_LOCATION_ID', 'AD_UNIT_PAGE_TYPE_ID', 'CAMPAIGN_ID', 'CELL_ID', 'CREATIVE_ID', 'DESTINATION_SEARCHED_ORDINAL', 'HAS_COMPARE_BUTTON', 'INCLUDES_SATURDAY_NIGHT', 'IS_WEEKDAY', 'IS_WITHIN_THIRTEEN_DAYS', 'PUBLISHER_ID', 'SITE_ID', 'USER_TIME_BLOCK']
    new_ordering = ['IMPRESSION_COUNT', 'CLICK_COUNT', 'CELL_ID', 'AD_UNIT_ID', 'AD_UNIT_PAGE_TYPE_ID', 'AD_UNIT_LOCATION_ID', 'SITE_ID', 'PUBLISHER_ID', 'CREATIVE_ID', 'AD_GROUP_ID', 'CAMPAIGN_ID', 'ADVERTISER_ID', 'ADVERTISER_CATEGORY_TYPE_ORDINAL', 'IS_WITHIN_THIRTEEN_DAYS', 'INCLUDES_SATURDAY_NIGHT', 'DESTINATION_SEARCHED_ORDINAL', 'USER_TIME_BLOCK', 'IS_WEEKDAY', 'HAS_COMPARE_BUTTON', 'AD_COPY_ID']

    input_file = pd.read_csv(input_file_path, sep='\t', header=None, names=old_ordering)
    input_file = input_file.astype(int64)
    if rearrange_columns:
        input_file = input_file.reindex(columns=new_ordering)
    input_file.to_csv(output_file_path, sep='\t', header=False, index=False)



if __name__ == '__main__':
    TRAIN_TEST_BASE_PATH = '/Users/jon.sondag/Datasets/'
    VW_MODEL_FOLDER = '/Users/jon.sondag/Datasets/20140129-generate-signals-predict-b/model/'
    VW_TEST_DATA_LOCATION = TRAIN_TEST_BASE_PATH + '20140129-generate-signals-predict-b/testing/normalized_vw.gz/part-all'
    TEST_DATA_LOCATION = TRAIN_TEST_BASE_PATH + 'testing/20130829-20130904-5pct/'
    TRAIN_DATA_LOCATION = TRAIN_TEST_BASE_PATH + 'training/20130801-20130814-10pct/'
    PREDICT_FILE_DATA_LOCATION = TRAIN_TEST_BASE_PATH + '20140129-generate-signals-predict-b/testing/normalized_predict.gz/predict-out'
    PREDICT_TEST_DATA_LOCATION = TRAIN_TEST_BASE_PATH + '20140129-generate-signals-predict-b/testing/normalized_predict.gz/part-all-formatted-rearranged'


    models = [VWClassifierModel('vw_classifier_1e-3'),
              VWClassifierModel('vw_classifier_1e-6'),
              VWClassifierModel('vw_classifier_1e-9')]
            #[PredictClassifierModel('predict_model')]
              #VWClassifierModel('vw_classifier')]
              #SklearnClassifierModel('sklearn1e-1')]
    model_files = [VW_MODEL_FOLDER + 'model_logistic_1e-3',
                   VW_MODEL_FOLDER + 'model_logistic_1e-6',
                   VW_MODEL_FOLDER + 'model_logistic_1e-9']
                #[PREDICT_FILE_DATA_LOCATION]
                   #VW_MODEL_FOLDER + 'model']
                   #TRAIN_DATA_LOCATION]

    #test_data_locations = [None, TEST_DATA_LOCATION]
    #vw_test_data_locations = [VW_TEST_DATA_LOCATION, None]
    #test_data_locations = [None]
    #vw_test_data_locations = [VW_TEST_DATA_LOCATION]
    #test_data_locations = [PREDICT_TEST_DATA_LOCATION]
    #vw_test_data_locations = [None]
    test_data_locations = [None, None, None]
    vw_test_data_locations = [VW_TEST_DATA_LOCATION, VW_TEST_DATA_LOCATION, VW_TEST_DATA_LOCATION]

    for idx, model in enumerate(models):
       model.load(model_files[idx])
       model.evaluate(test_data_locations[idx], vw_test_data_locations[idx])
