import sklearn
import sklearn.datasets
import Queue
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning) # to get rid of strange numpy warning

UPLIFT_MODEL_EVALUATOR_PATH = '/Users/jon.sondag/code/adServer/model-builder/test/python/files/uplift_model_evaluator/part-00000'

# TODO: split out splitting criterion classes into treatment/control and classical cases, rather than using if 'isTreatment'...

class Gini(object):
    """
    Gini for the n-class binary-split case, as in a classical CART decision tree.  Splits chosen to minimize this.
    """
    def __init__(self):
        pass

    def get_split_value(self, target_data_left, target_data_right):
        """
        :param target_data_left: pandas Series representing target class values for the left split
        :param target_data_right: pandas Series representing target class values for the right split
        :return: gini index for this split
        """
        gini_left = self.get_value(target_data_left)

        percent_in_left_branch = 1.0
        gini = 0.0
        gini_right = 0.0

        if len(target_data_right) > 0:
            gini_right = self.get_value(target_data_right)

            percent_in_right_branch = float(len(target_data_right)) / (len(target_data_left) + len(target_data_right))
            percent_in_left_branch = 1 - percent_in_right_branch

            gini += percent_in_right_branch * gini_right

        gini += percent_in_left_branch * gini_left

        return gini_left, gini_right, gini

    def get_value(self, target_data):

        gini = 0.0
        num_examples = len(target_data)

        for val in target_data.unique():
            num_val_examples = len(target_data[target_data == val])
            if num_val_examples < num_examples:
                prob_val = float(num_val_examples) / num_examples
                gini += prob_val * (1 - prob_val)

        return gini


class Entropy(object):
    """
    Entropy for the n-class binary split case, as in a classical decision tree.  Splits chosen to minimize this.
    """
    def __init__(self):
        pass

    def get_split_value(self, target_data_left, target_data_right):
        """
        :param target_data_left: pandas Series representing target class values for the left split
        :param target_data_right: pandas Series representing target class values for the right split
        :return: entropy for this split
        """
        entropy_left = self.get_value(target_data_left)

        percent_in_left_branch = 1.0
        entropy = 0.0
        entropy_right = 0.0

        if len(target_data_right) > 0:
            entropy_right = self.get_value(target_data_right)

            percent_in_right_branch = float(target_data_right.size) / (target_data_left.size + target_data_right.size)
            percent_in_left_branch = 1 - percent_in_right_branch

            entropy += percent_in_right_branch * entropy_right

        entropy += percent_in_left_branch * entropy_left

        return entropy

    def get_value(self, target_data):

        entropy = 0.0
        num_examples = target_data.size

        for val in target_data.unique():
            num_val_examples = target_data[target_data == val].size
            if num_val_examples < num_examples:
                prob_val = float(num_val_examples) / num_examples
                entropy -= prob_val * np.log2(prob_val)

        return entropy


class EuclideanDistance(object):
    """
    Splitting criterion for the Treatment vs Control n-class binary split case, using Gini Index.
    Reference: Rzepakowski, Jaroszewicz, "Decision trees for uplift modeling with single and multiple treatments"
    """
    def __init__(self):
        self._total_num_treatment = None
        self._total_num_control = None

    def set_treatment_control_nums(self, total_num_treatment, total_num_control):
        self._total_num_treatment = total_num_treatment
        self._total_num_control = total_num_control
        return self

    def get_split_value(self, target_data_left, target_data_right):
        """
        :param target_data_left: pandas DataFrame with 'target' and 'isTreatment' columns for the left split
        :param target_data_right: pandas DataFrame with 'target' and 'isTreatment' columns for the right split
        :return: splitting criterion value for this split (note: we return the negative split value so that this
        will be minimized rather than maximized, same as the classical decision tree criterion.
        """
        euclidean_gain = self._calculate_euclidean_gain(target_data_left, target_data_right)

        # calculate J(A)
        normalization_factor = self._normalization_factor(target_data_left, target_data_right)

        return euclidean_gain / normalization_factor

    def _calculate_euclidean_gain(self, target_data_left, target_data_right):
        euclid_left = self.get_value(target_data_left)
        euclid_right = self.get_value(target_data_right)
        euclid_all = self.get_value(target_data_left.append(target_data_right, ignore_index=True))

        proportion_left = float(len(target_data_left)) / (len(target_data_left) + len(target_data_right))
        proportion_right = 1 - proportion_left

        return (proportion_left * euclid_left + proportion_right * euclid_right) - euclid_all

    def get_value(self, target_data):
        distance = 0.0
        num_examples = len(target_data)
        num_examples_treatment = len(target_data[target_data['isTreatment'] == 1])
        num_examples_control = num_examples - num_examples_treatment

        for val in target_data['target'].unique():
            target_data_val = target_data[target_data['target'] == val]
            num_val_examples_treatment = len(target_data_val[target_data['isTreatment'] == 1])
            if num_examples_treatment == 0:
                prob_treatment = 0.0
            else:
                prob_treatment = float(num_val_examples_treatment) / num_examples_treatment
            num_val_examples_control = len(target_data_val[target_data['isTreatment'] == 0])
            if num_examples_control == 0:
                prob_control = 0.0
            else:
                prob_control = float(num_val_examples_control) / num_examples_control
            distance += (prob_treatment - prob_control) ** 2

        return -distance # negative so that this quantity is minimized

    def _normalization_factor(self, target_data_left, target_data_right):
        # Gini(N^T / N, N^C / N)
        overall_tc_gini_coef = self._get_overall_tc_gini_coef()

        # D(P^T(A) : P^C(A))
        target_data_all = target_data_left.append(target_data_right, ignore_index=True)

        num_treatment_overall = float(len(target_data_all[target_data_all['isTreatment'] == 1]))
        if num_treatment_overall == 0:
            p_treatment_left = 1.0 # TODO: does this default make sense?
        else:
            p_treatment_left = len(target_data_left[target_data_left['isTreatment'] == 1]) / num_treatment_overall
        num_control_overall = float(len(target_data_all[target_data_all['isTreatment'] == 0]))
        if num_control_overall == 0:
            p_control_left = 1.0 # TODO: does this default make sense?
        else:
            p_control_left = len(target_data_left[target_data_left['isTreatment'] == 0]) / num_control_overall
        p_treatment_right = 1 - p_treatment_left
        p_control_right = 1 - p_control_left
        treatment_control_test_distance = (p_treatment_left - p_control_left) ** 2 + (p_treatment_right - p_control_right) ** 2

        # (N^T / N) * Gini(P^T(A)) + (N^C / N) * Gini(P^C(A))
        total_num = self._total_num_treatment + self._total_num_control
        term_one = (self._total_num_treatment / total_num) * p_treatment_left * (1 - p_treatment_left) * p_treatment_right * (1 - p_treatment_right)
        term_two = (self._total_num_control / total_num) * p_control_left * (1 - p_control_left) * p_control_right * (1 - p_control_right)
        gini_treatment_control = term_one + term_two

        # total
        return overall_tc_gini_coef * treatment_control_test_distance + gini_treatment_control + 0.5

    def _get_overall_tc_gini_coef(self):
        total_num = self._total_num_treatment + self._total_num_control
        treatment_gini = (self._total_num_treatment / float(total_num)) * (1 - self._total_num_treatment / float(total_num))
        control_gini = (self._total_num_control / float(total_num)) * (1 - self._total_num_control / float(total_num))

        return treatment_gini + control_gini


class KLDivergence(object):
    """
    Splitting criterion for the Treatment vs Control n-class binary split case, using Kullback-Liebler divergence.
    Reference: Rzepakowski, Jaroszewicz, "Decision trees for uplift modeling with single and multiple treatments"
    """
    def __init__(self):
        self._total_num_treatment = None
        self._total_num_control = None

    def set_treatment_control_nums(self, total_num_treatment, total_num_control):
        self._total_num_treatment = total_num_treatment
        self._total_num_control = total_num_control
        return self

    def get_split_value(self, target_data_left, target_data_right):
        """
        :param target_data_left: pandas DataFrame with 'target' and 'isTreatment' columns for the left split
        :param target_data_right: pandas DataFrame with 'target' and 'isTreatment' columns for the right split
        :return: splitting criterion value for this split
        """
        gini_left = self.get_value(target_data_left)
        gini_right = self.get_value(target_data_right)
        gini_all = self.get_value(target_data_left.append(target_data_right, ignore_index=True))

        # TODO: finish this
        return 0.0

    def get_value(self, target_data):
        # TODO: finish this
        return 0.0



class ImpurityTracker(object):
    def __init__(self, impurity_metric):
        self._impurity_metric = impurity_metric
        self._best_row = 0
        self._best_row_val = 0
        self._best_col = 0
        self._best_impurity = float(inf)
        self._last_row_flag = -1

    def _calc_impurity(self, sorted_data, col, row):
        if 'isTreatment' in sorted_data.columns:
            target_data_left = sorted_data[:row][['target', 'isTreatment']]
            target_data_right = sorted_data[row:][['target', 'isTreatment']]
        else:
            target_data_left = sorted_data[:row]['target']
            target_data_right = sorted_data[row:]['target']
        impurity = self._impurity_metric.get_split_value(target_data_left, target_data_right)
        if impurity < self._best_impurity:
            self._best_row = row
            self._best_row_val = sorted_data[col][row]
            self._best_col = col
            self._best_impurity = impurity

    def _get_next_row(self, sorted_data, col, row):
        original_row_val = sorted_data[col][row]
        next_row = row + 1

        while next_row < len(sorted_data):
            new_row_val = sorted_data[col][next_row]
            if new_row_val != original_row_val:
                return next_row
            next_row += 1
        return self._last_row_flag

    def get_best_filter_func(self, data):
        for col in data.columns:
            if col != 'target' and col != 'isTreatment':
                sorted_data = data.sort(col)
                sorted_data.index = range(len(sorted_data))
                row = 0
                while row != self._last_row_flag:
                    self._calc_impurity(sorted_data, col, row)
                    row = self._get_next_row(sorted_data, col, row)
        return self._best_impurity, self._best_col, self._best_row_val, lambda data_to_filter: data_to_filter[data_to_filter[self._best_col] < self._best_row_val]

    def get_best_inverse_filter_func(self):
        return self._best_col, self._best_row_val, lambda data_to_filter: data_to_filter[data_to_filter[self._best_col] >= self._best_row_val]

    def get_best_row(self):
        return self._best_row

    def get_best_col(self):
        return self._best_col

    def get_best_impurity(self):
        return self._best_impurity



class Node(object):
    def __init__(self, level, parents, max_levels, impurity_metric):
        self._level = level
        self._parents_and_branches = parents
        self._max_levels = max_levels
        self._filter_func = None
        self._inverse_filter_func = None
        self._left_child = None
        self._right_child = None
        self._impurity_metric = impurity_metric
        self._best_col = -1
        self._best_row_val = -1
        self._best_col_inv = -1
        self._best_row_val_inv = -1
        self._error = 0
        self._samples = 0
        self._num_per_class = []

    def __repr__(self):
        result = ''
        error_string = '%.6f' % self._error
        result += str(self._level) + ',' + error_string + ',' + str(self._samples) + ',' + str(self._num_per_class)
        if self._left_child is not None:
            result += ',' + str(self._best_col) + ',' + str(self._best_row_val) + '\n'
        else:
            result += '\n'
        if self._left_child is not None:
            result += str(self._left_child)
        if self._right_child is not None:
            result += str(self._right_child)
        return result

    def get_parents_and_branches(self):
        return self._parents_and_branches

    def filter_data(self, data, branch):
        if branch == 'left':
            return self._filter_func(data)
        elif branch == 'right':
            return self._inverse_filter_func(data)
        else:
            raise Exception('Node must be a left or right descendant of parent')

    def predict_data(self, data, predict_probability=False, is_treatment=False):
        if self.is_leaf_node():
            if is_treatment:
                if predict_probability:
                    treatment_false_target_false = self._num_per_class.get((0, 0), 0)
                    treatment_false_target_true = self._num_per_class.get((0, 1), 0)
                    treatment_true_target_false = self._num_per_class.get((1, 0), 0)
                    treatment_true_target_true = self._num_per_class.get((1, 1), 0)
                    total_treatment_false = treatment_false_target_false + treatment_false_target_true
                    total_treatment_true = treatment_true_target_false + treatment_true_target_true
                    if total_treatment_false > 0 and total_treatment_true > 0:
                        return treatment_true_target_true / total_treatment_true - treatment_false_target_true / total_treatment_false
                    else:
                        return 0.0
                else:
                    raise Exception('predict_probability=False not implemented for treatment/control case!')
            else:
                if predict_probability:
                    count_sum = np.sum(self._num_per_class.values())
                    return_dict = {}
                    for key in self._num_per_class.keys():
                        return_dict[key] = self._num_per_class[key] / float(count_sum)
                    return return_dict
                else:
                    max_count = np.max(self._num_per_class.values())
                    for key in self._num_per_class.keys():
                        if self._num_per_class[key] == max_count:
                            return key
        else:
            if len(self.filter_data(data, 'left')) == 0:
                return self._right_child.predict_data(data, predict_probability, is_treatment)
            else:
                return self._left_child.predict_data(data, predict_probability, is_treatment)

    def calculate_children(self, data, total_num_treatment=None, total_num_control=None):
        self._samples = len(data)
        if 'isTreatment' in data.columns:
            self._error = self._impurity_metric.get_value(data[['isTreatment', 'target']])
            num_per_class_df = data.groupby(['isTreatment', 'target']).count()['target']
            self._num_per_class = dict(zip(num_per_class_df.index, num_per_class_df.values))
        else:
            self._error = self._impurity_metric.get_value(data['target'])
            num_per_class_df = data['target'].value_counts()
            self._num_per_class = dict(zip(num_per_class_df.index, num_per_class_df.values))


        if self._level < self._max_levels and len(data) > 0 and len(data['target'].unique()) > 1:
            if total_num_treatment is None:
                entropy_tracker = ImpurityTracker(self._impurity_metric)
            else:
                entropy_tracker = ImpurityTracker(self._impurity_metric.set_treatment_control_nums(total_num_treatment, total_num_control))
            best_impurity, self._best_col, self._best_row_val, filter_func = entropy_tracker.get_best_filter_func(data)
            if best_impurity < self._error:
                self._filter_func = filter_func
                child_one = Node(self._level + 1, self._parents_and_branches + [[self, 'left']], self._max_levels, self._impurity_metric)
                self._left_child = child_one
                self._best_col_inv, self._best_row_val_inv, inverse_filter_func = entropy_tracker.get_best_inverse_filter_func()
                self._inverse_filter_func = inverse_filter_func
                child_two = Node(self._level + 1, self._parents_and_branches + [[self, 'right']], self._max_levels, self._impurity_metric)
                self._right_child = child_two
        return self._left_child, self._right_child

    def is_leaf_node(self):
        return self._left_child is None and self._right_child is None


class CART(object):
    def __init__(self, max_levels, impurity_metric=None):
        if impurity_metric is None:
            impurity_metric = Gini()
        self._root_node = Node(0, [], max_levels, impurity_metric)
        # make self a queue with nodes to be split
        self._to_be_processed = Queue.Queue()
        self._to_be_processed.put(self._root_node)
        self._has_been_fit = False
        self._impurity_metric = impurity_metric

    def __repr__(self):
        return str(self._root_node)

    def _add_to_be_processed(self, node):
        self._to_be_processed.put(node)

    def fit(self, data):
        while not self._to_be_processed.empty():
            # get splits, pass child nodes to the queue
            node = self._to_be_processed.get()
            new_data = data
            for parent_and_branch in node.get_parents_and_branches():
                parent = parent_and_branch[0]
                branch = parent_and_branch[1]
                new_data = parent.filter_data(new_data, branch)
            new_data.index = range(len(new_data))
            if 'isTreatment' in data.columns:
                total_num_treatment = len(data[data['isTreatment'] == 1])
                total_num_control = len(data[data['isTreatment'] == 0])
                left_child, right_child = node.calculate_children(new_data, total_num_treatment, total_num_control)
            else:
                left_child, right_child = node.calculate_children(new_data)
            if left_child is not None and right_child is not None:
                self._add_to_be_processed(left_child)
                self._add_to_be_processed(right_child)
            elif left_child is not None or right_child is not None:
                raise Exception('error: a Node must have 2 children or 0 children!')

        self._has_been_fit = True
        return self

    def predict(self, data, predict_probabilities=False, is_treatment=False):
        if self._has_been_fit is False:
            return None
        else:
            if is_treatment:
                if not predict_probabilities:
                    raise Exception('predict_probability=False not implemented for treatment/control case!')
                else:
                    result = pd.DataFrame(index=data.index)
                    for idx in result.index:
                        proba_prediction = self._root_node.predict_data(data.iloc[[idx]], predict_probabilities, is_treatment)
                        if 'pred' not in result.columns:
                            result['pred'] = pd.Series(index=result.index, dtype=float64)
                        result['pred'][idx] = proba_prediction
            else:
                if not predict_probabilities:
                    result = pd.Series(index=data.index)
                    for idx in result.index:
                        result[idx] = self._root_node.predict_data(data.iloc[[idx]], predict_probabilities, is_treatment)
                else:
                    result = pd.DataFrame(index=data.index)
                    for idx in result.index:
                        proba_predictions = self._root_node.predict_data(data.iloc[[idx]], predict_probabilities, is_treatment)
                        for col in proba_predictions.keys():
                            if col not in result.columns:
                                result[col] = pd.Series(index=result.index, dtype=float64)
                            result[col][idx] = proba_predictions[col]
            result = result.fillna(0.0)
            return result


def fit_basic_tree():
    print 'fitting basic tree'
    iris_data = load_iris_data()
    tree = CART(max_levels=5).fit(iris_data)

    prediction_data = iris_data.copy()
    del prediction_data['target']
    predictions = tree.predict(prediction_data)

    print tree # should match tree here: http://scikit-learn.org/stable/modules/tree.html#classification
    print predictions

def load_iris_data():
    iris = sklearn.datasets.load_iris()
    data = pd.DataFrame(iris.data)
    data['target'] = iris.target
    return data

def fit_uplift_tree():
    print 'fitting uplift tree'
    uplift_data = load_uplift_data()
    tree = CART(max_levels=2, impurity_metric=EuclideanDistance()).fit(uplift_data)

    prediction_data = uplift_data.copy()
    del prediction_data['isTreatment']
    del prediction_data['target']
    predictions = tree.predict(prediction_data, is_treatment=True, predict_probabilities=True)

    print tree
    print predictions

    import pdb; pdb.set_trace()

def load_uplift_data():
    # generate data by running gen_data_file in uplift_model_evaluator.py
    uplift_data = pd.read_csv(UPLIFT_MODEL_EVALUATOR_PATH, header=None, sep='\t')
    uplift_data = uplift_data[:1000]
    uplift_data.columns = [0, 'isTreatment', 'target']
    # add some random columns, see if we still split on column 0
    uplift_data[1] = np.random.randn(len(uplift_data))
    uplift_data[2] = np.random.randn(len(uplift_data))
    uplift_data[3] = np.random.randn(len(uplift_data))
    return uplift_data

if __name__ == '__main__':
    # fit_basic_tree()
    fit_uplift_tree()
