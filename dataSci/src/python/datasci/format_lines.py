import random
import pandas as pd
import gzip
import hashlib
import re


def get_column_names(file):
    f = open(file, 'r')
    names = f.readline()
    names = names.split('\t')
    return names[2:-1]  # leave out 'request_id', 'requested_at_iso', 'y_value'


def get_indices_to_keep_list(column_names, target_variable):
    to_not_keep = set(['RANK_IN_AD_GROUP', 'IS_RANK_TIED', 'DIFFERENCE_FROM_MEAN_PRICE', 'DIFFERENCE_FROM_MEAN_PRICE_PERCENT',\
            'DIFFERENCE_FROM_PUB_PRICE', 'DIFFERENCE_FROM_PUB_PRICE_PERCENT', 'HOTEL_AVERAGE_NIGHTLY_RATE',\
            'AUCTION_POSITION'])
    to_not_keep = to_not_keep.union([target_variable])
    indices_to_keep = []
    for idx, name in enumerate(column_names):
        if name not in to_not_keep:
            indices_to_keep.append(idx)
    return indices_to_keep


class QueryIdHashGenerator(object):
    def __init__(self):
        self._md5 = hashlib.md5()

    def get_next_query_id(self, request_id):
        self._md5.update(request_id)
        return int(self._md5.digest().encode('hex'), 16)


class QueryIdSequentialGenerator(object):
    def __init__(self):
        self._request_id_idx = 0
        self._last_request_id = ''

    def get_next_query_id(self, request_id):
        if request_id != self._last_request_id:
            self._last_request_id = request_id
            self._request_id_idx += 1
        return self._request_id_idx


class LineWriterLinearRegression(object):
    """
    input format: vw-style, e.g.:  2 |a 0:1.0 1-5 2:0.5 3:1.5 ...
    output format: vw-style, but with columns left out as specified, so e.g.: 2 |a 0:1.0 1-5 3:1.5 ...
    """
    def __init__(self, column_names, base_path, batch_write_size):
        self._column_names = column_names
        self._base_path = base_path
        self._batch_write_size = batch_write_size
        self._batch_number = 0
        self._job_dict = self._get_job_dict()

    def _get_job_dict(self):
        lin_reg_dict = {'a': {'target': 'DIFFERENCE_FROM_MEAN_PRICE',
                        'indices_to_keep': get_indices_to_keep_list(self._column_names, 'DIFFERENCE_FROM_MEAN_PRICE')},
                        'b': {'target': 'DIFFERENCE_FROM_MEAN_PRICE_PERCENT',
                        'indices_to_keep': get_indices_to_keep_list(self._column_names, 'DIFFERENCE_FROM_MEAN_PRICE_PERCENT')},
                        'c': {'target': 'DIFFERENCE_FROM_PUB_PRICE',
                        'indices_to_keep': get_indices_to_keep_list(self._column_names, 'DIFFERENCE_FROM_PUB_PRICE')},
                        'd': {'target': 'DIFFERENCE_FROM_PUB_PRICE_PERCENT',
                        'indices_to_keep': get_indices_to_keep_list(self._column_names, 'DIFFERENCE_FROM_PUB_PRICE_PERCENT')},
                        'e': {'target': 'HOTEL_AVERAGE_NIGHTLY_RATE',
                        'indices_to_keep': get_indices_to_keep_list(self._column_names, 'HOTEL_AVERAGE_NIGHTLY_RATE')}}
        for k in lin_reg_dict.keys():
            lin_reg_dict[k]['out_file'] = gzip.open(self._base_path + 'lin_reg_' + k + '_file.gz', 'w')
            lin_reg_dict[k]['batch_cache'] = []
        return lin_reg_dict

    def write_to_file(self, vw_line):
        self._batch_number += 1
        for k, v in self._job_dict.iteritems():
            self._job_dict[k]['batch_cache'].append(self._get_formatted_line(vw_line,
                                            self._job_dict[k]['target'], self._job_dict[k]['indices_to_keep']))
            if self._batch_number == self._batch_write_size:
                self._job_dict[k]['out_file'].write(''.join(self._job_dict[k]['batch_cache']))
                self._job_dict[k]['batch_cache'] = []
        if self._batch_number == self._batch_write_size:
            self._batch_number = 0

    def _get_formatted_line(self, vw_line, target_name, indices_to_keep):
        vw_line_split = vw_line.split(' ')
        vw_variables = vw_line_split[2:] # leave out vw target, namespace name
        new_line_variables = [vw_variables[i] for i in indices_to_keep]
        split_regex = re.compile('^[0-9]+[:-]')
        this_var = str(vw_variables[self._column_names.index(target_name)])
        target_value = split_regex.split(this_var)[1]

        return target_value + ' ' + '|a ' + ' '.join(new_line_variables) + '\n'


class LineWriterLogisticRegression(object):
    """
    input format: vw-style, e.g.:  -1 |a 0:1.0 1-5 2:0.5 3:1.5 ...
    output format: vw-style, but with columns left out as specified, features sorted, and 0-indexed feature
     mapped to 10000 (since features need to start at 1 for ranksvm), so e.g.: -1 |a 1-5 3:1.5 ... 10000:1.0
    """
    def __init__(self, column_names, base_path, batch_write_size):
        self._column_names = column_names
        self._base_path = base_path
        self._batch_write_size = batch_write_size
        self._batch_number = 0
        self._job_dict = self._get_job_dict()

    def _get_job_dict(self):
        log_reg_dict = {'a': {'target': 'RANK_IN_AD_GROUP',
                        'indices_to_keep': get_indices_to_keep_list(self._column_names, 'RANK_IN_AD_GROUP')},
                        'b': {'target': 'DIFFERENCE_FROM_PUB_PRICE',
                        'indices_to_keep': get_indices_to_keep_list(self._column_names, 'DIFFERENCE_FROM_PUB_PRICE')}}
        for k in log_reg_dict.keys():
            log_reg_dict[k]['out_file'] = gzip.open(self._base_path + 'log_reg_' + k + '_file.gz', 'w')
            log_reg_dict[k]['batch_cache'] = []
        return log_reg_dict

    def write_to_file(self, vw_line):
        self._batch_number += 1
        for k, v in self._job_dict.iteritems():
            self._job_dict[k]['batch_cache'].append(self._get_formatted_line(vw_line,
                                                                             self._job_dict[k]['target'],
                                                                             self._job_dict[k]['indices_to_keep']))
            if self._batch_number == self._batch_write_size:
                self._job_dict[k]['out_file'].write(''.join(self._job_dict[k]['batch_cache']))
                self._job_dict[k]['batch_cache'] = []
        if self._batch_number == self._batch_write_size:
            self._batch_number = 0

    def _get_formatted_line(self, vw_line, target_name, indices_to_keep):
        vw_line_split = vw_line.split(' ')
        vw_variables = vw_line_split[2:] # leave out vw target, namespace name
        new_line_variables = [vw_variables[i] for i in indices_to_keep]
        split_regex = re.compile('^[0-9]+[:-]')
        this_var = str(vw_variables[self._column_names.index(target_name)])
        target_value = split_regex.split(this_var)[1]
        if float(target_value) > 0:
            target_value = '1'
        else:
            target_value = '-1'

        return target_value + ' ' + '|a ' + ' '.join(new_line_variables) + '\n'


class LineWriterRankRegression(object):
    """
    input format: vw-style, e.g.:  -1 |a 0:1.0 1-5 2:0.5 3:1.5 ..., with a request_id for each line
        so that query ids can be generated (for PPA)
    output format: svmrank-style, with columns left out as specified and query ids filled in.  also, svmrank does not
        handle categorical features so this creates new variables for each observed hashed feature.
        so e.g.: 3 qid:1 0:1.0 100:5 3:1.5 ...
    """
    def __init__(self, column_names, base_path, batch_write_size, query_id_generator):
        self._column_names = column_names
        self._base_path = base_path
        self._batch_write_size = batch_write_size
        self._batch_number = 0
        self._job_dict = self._get_job_dict()
        self._query_id_generator = query_id_generator
        self._hash_dict = dict()
        self._highest_hash_val_so_far = 1000  # set this higher than the value of any variable in our set

    def _get_job_dict(self):
        rank_reg_dict = {'a': {'target': 'RANK_IN_AD_GROUP',
                        'indices_to_keep': get_indices_to_keep_list(self._column_names, 'RANK_IN_AD_GROUP')}}
        for k in rank_reg_dict.keys():
            rank_reg_dict[k]['out_file'] = gzip.open(self._base_path + 'rank_reg_new2_' + k + '_file.gz', 'w')
            rank_reg_dict[k]['batch_cache'] = []
        return rank_reg_dict

    def write_to_file(self, vw_line, request_id):
        query_id = self._query_id_generator.get_next_query_id(request_id)
        self._batch_number += 1
        for k, v in self._job_dict.iteritems():
            self._job_dict[k]['batch_cache'].append(self._get_formatted_line(vw_line,
                                                                             self._job_dict[k]['target'],
                                                                             self._job_dict[k]['indices_to_keep'],
                                                                             query_id))
            if self._batch_number == self._batch_write_size:
                self._job_dict[k]['out_file'].write(''.join(self._job_dict[k]['batch_cache']))
                self._job_dict[k]['batch_cache'] = []
        if self._batch_number == self._batch_write_size:
            self._batch_number = 0

    def _get_formatted_line(self, vw_line, target_name, indices_to_keep, query_id):
        vw_line_split = vw_line.split()
        vw_variables = vw_line_split[2:]
        new_line_variables = [self._get_hashed_value(vw_variables[i]) for i in indices_to_keep]
        line_split_regex = re.compile('[:-]')
        new_line_variables_sorted = sorted(new_line_variables, key=lambda x: int(line_split_regex.split(x)[0]))
        this_var = str(vw_variables[self._column_names.index(target_name)])
        split_regex = re.compile('^[0-9]+[:-]')
        target_value = split_regex.split(this_var)[1]
        target_value = str(int(float(target_value)) + 1)
        result_string = target_value + ' ' + 'qid:' + str(query_id) + ' ' + ' '.join(new_line_variables_sorted) + '\n'
        return result_string

    def _get_hashed_value(self, variable):
        if ':' in variable:
            if variable.split(':')[0] == '0':
                return '1000000:' + variable.split(':')[1]
            else:
                return variable
        else:
            if variable not in self._hash_dict:
                self._highest_hash_val_so_far += 1
                self._hash_dict[variable] = str(self._highest_hash_val_so_far) + ':' + str(1.0)
            return self._hash_dict[variable]


class LineWriterQualityScore(object):
    """
    input format: vw-style, e.g.:  0 |a 0:1.0 1-5 2:0.5 3:1.5 ... (same as vw, but with 0's instead of -1's)
    output format: vw-style, but with columns left out as specified, so e.g.: -1 |a 0:1.0 1-5 3:1.5 ...
    """
    def __init__(self, column_names, base_path, batch_write_size):
        self._column_names = column_names
        self._base_path = base_path
        self._batch_write_size = batch_write_size
        self._batch_number = 0
        self._job_dict = self._get_job_dict()

    def _get_job_dict(self):
        qs_reg_dict = {'all_columns': {'indices_to_keep': range(len(self._column_names))},
                       'no_price_columns': {'indices_to_keep': get_indices_to_keep_list(self._column_names, 'RANK_IN_AD_GROUP')}}
        for k in qs_reg_dict.keys():
            qs_reg_dict[k]['out_file'] = gzip.open(self._base_path + 'qs_model_sampled_' + k + '_file.gz', 'w')
            qs_reg_dict[k]['batch_cache'] = []
        return qs_reg_dict

    def write_to_file(self, vw_line):
        self._batch_number += 1
        for k, v in self._job_dict.iteritems():
            self._job_dict[k]['batch_cache'].append(self._get_formatted_line(vw_line, self._job_dict[k]['indices_to_keep']))
            if self._batch_number == self._batch_write_size:
                self._job_dict[k]['out_file'].write(''.join(self._job_dict[k]['batch_cache']))
                self._job_dict[k]['batch_cache'] = []
        if self._batch_number == self._batch_write_size:
            self._batch_number = 0

    def _get_formatted_line(self, vw_line, indices_to_keep):
        vw_line_split = vw_line.split(' ')
        vw_variables = vw_line_split[2:]  # leave out vw target, namespace name
        new_line_variables = [vw_variables[i] for i in indices_to_keep]
        if vw_line_split[0] == '0':
            target_value = '-1'
        else:
            target_value = '1'

        return target_value + ' ' + '|a ' + ' '.join(new_line_variables) + '\n'


def sample_clicks_from_file(input_file, output_file):
    line_number = 0
    lines_to_write = []
    with gzip.open(input_file, 'r') as fin, gzip.open(output_file, 'w') as fout:
        for line in fin:
            if line.split('\t')[2][0] == '0' or line.split('\t')[0][0] == '0':  # TODO: check why we need both of these
                if random.random() < 0.0001:
                    lines_to_write.append(line)
            else:
                lines_to_write.append(line)
            line_number += 1
            if line_number % 100000 == 0:
                fout.write(''.join(lines_to_write))
                lines_to_write = []
                print 'line_number:', line_number
            if line_number > 100000000:
                break


def format_lines():
    base_path = '/Users/jon.sondag/Datasets/20140923-generate-signals-ppa-price-20140902/normalized_vw.gz/'
    data_file = base_path + 'part-all-sampled-100mm.gz'
    # sample_clicks_from_file(data_file, base_path + 'part-all-sampled-100mm.gz')
    # import pdb; pdb.set_trace()
    column_names = get_column_names(base_path + '.column_names')
    batch_write_size = 1000

    # line_writer_linear_regression = LineWriterLinearRegression(column_names, base_path, batch_write_size)
    # line_writer_logistic_regression = LineWriterLogisticRegression(column_names, base_path, batch_write_size)
    # line_writer_rank_regression = LineWriterRankRegression(column_names, base_path, batch_write_size,
    #                                                        QueryIdSequentialGenerator())
    line_writer_quality_score = LineWriterQualityScore(column_names, base_path, batch_write_size)

    with gzip.open(data_file, 'r') as fin:
        line_number = 0
        for line in fin:
            line_split = line.strip().split('\t')
            request_id = line_split[1]
            vw_line = line_split[2]
            # line_writer_linear_regression.write_to_file(vw_line)
            # line_writer_logistic_regression.write_to_file(vw_line)
            # line_writer_rank_regression.write_to_file(vw_line, request_id)
            line_writer_quality_score.write_to_file(vw_line)

            line_number += 1
            # running 1mm lines per file for predicting prices
            if line_number > 1000000:
                break
            if line_number % 10000 == 0:
                print 'line number:', line_number


if __name__ == '__main__':
    # pass
    format_lines()


# Fitting models:
# vw -d log_reg_a_file.gz -c --passes 10 -f models/log_reg_a_file_predictor.vw --loss_function logistic
# vw -d lin_reg_a_file.gz -c --passes 10 -f models/lin_reg_a_file_predictor.vw --loss_function squared
# vw -d lin_reg_b_file.gz -c --passes 10 -f models/lin_reg_b_file_predictor.vw --loss_function squared
# vw -d lin_reg_c_file.gz -c --passes 10 -f models/lin_reg_c_file_predictor.vw --loss_function squared
# vw -d lin_reg_d_file.gz -c --passes 10 -f models/lin_reg_d_file_predictor.vw --loss_function squared
# vw -d lin_reg_e_file.gz -c --passes 10 -f models/lin_reg_e_file_predictor.vw --loss_function squared
# vw -d qs_model_sampled_all_columns_file.gz -c --passes 10 -f models/qs_model_sampled_all_columns_predictor_0.vw --loss_function logistic --l2 0
# vw -d qs_model_sampled_all_columns_file.gz -c --passes 10 -f models/qs_model_sampled_all_columns_predictor_1e-6.vw --loss_function logistic --l2 1e-6
# vw -d qs_model_sampled_all_columns_file.gz -c --passes 10 -f models/qs_model_sampled_all_columns_predictor_1e-9.vw --loss_function logistic --l2 1e-9
# vw -d qs_model_sampled_no_price_columns_file.gz -c --passes 10 -f models/qs_model_sampled_no_price_columns_predictor_0.vw --loss_function logistic --l2 0
# vw -d qs_model_sampled_no_price_columns_file.gz -c --passes 10 -f models/qs_model_sampled_no_price_columns_predictor_1e-6.vw --loss_function logistic --l2 1e-6
# vw -d qs_model_sampled_no_price_columns_file.gz -c --passes 10 -f models/qs_model_sampled_no_price_columns_predictor_1e-9.vw --loss_function logistic --l2 1e-9
