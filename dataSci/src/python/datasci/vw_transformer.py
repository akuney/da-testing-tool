import pandas as pd
import re
import os
import gzip

BASE_PATH = '/Users/jon.sondag/Datasets/train_test_datasets/EXPEDIA/FLIGHTS'
INPUT_PATH = BASE_PATH + '/training_vw/20130801-20130814-100pct'
OUTPUT_PATH = BASE_PATH + '/training_vw/20130801-20130814-100pct-19file'
NAMESPACE_NAME = 'a'
DO_CREATE_COMBINED_FILE_DIRECTORY = True
INPUT_NUMBER_OF_FILES = 266
OUTPUT_NUMBER_OF_FILES = 19

def transform_line_no_quotes(line):
    return line[1:-2]

def transform_line_identity(line):
    return line

# MAKE SURE TO SET THIS PROPERLY - MAY VARY BY FILE!
LINE_TRANSFORMER = transform_line_no_quotes


class VWTransformer(object):
    def __init__(self):
        pass

    def _ignore_file(self, file_name):
        return file_name != '.DS_Store' and file_name != '.pig_header' and file_name != '.pig_schema'

    def _get_input_files(self, input_path):
        return [f for f in os.listdir(input_path) if not self._ignore_file(f)]

    def _get_output_file(self, output_path, file):
        return output_path + '/' + file

    def _get_concatenated_string(self, line_array):
        output = ''
        for idx, val in enumerate(line_array):
            output = output + ' ' + str(idx) + ':' + val
        output = output[1:]
        output = output + '\n'
        return output

    def _get_y_value(self, last_array_value):
        return '1' if float(last_array_value) > 0 else '-1'

    def _reformat_line(self, line, namespace_name, line_transformer):
        line_no_quotes = line_transformer(line)
        line_array = line_no_quotes.split('\t')[2:] # 2: to remove request_id, requested_at
        features = self._get_concatenated_string(line_array[:-1]) # :-1 to remove y value
        y_value = self._get_y_value(line_array[-1])
        line_out = y_value + ' |' + namespace_name + ' ' + features
        return line_out

    def _get_file_handle(self, files_compressed, file_name, open_mode=None):
        if files_compressed and open_mode is not None:
            return gzip.open(file_name, open_mode)
        elif files_compressed and open_mode is None:
            return gzip.open(file_name)
        elif not files_compressed and open_mode is not None:
            return open(file_name, open_mode)
        elif not files_compressed and open_mode is None:
            return open(file_name)

    def _create_combined_file_directory(self, output_path, files_compressed):
        file_list = os.listdir(output_path)
        combined_file_path = output_path + '-1file'
        if not os.path.exists(combined_file_path):
            os.mkdir(combined_file_path)
        if files_compressed:
            f_out = self._get_file_handle(files_compressed, combined_file_path + '/part-00000.gz', 'w')
        else:
            f_out = self._get_file_handle(files_compressed, combined_file_path + '/part-00000', 'w')
        for file in file_list:
            if not self._ignore_file(file):
                f_in = open(output_path + '/' + file)
                for line in f_in.readlines():
                    f_out.write(line)
        f_out.close()

    """
    function: reformat_files_for_vw

    inputs:
        input_path: directory with files to be reformatted
        output_path: output directory
        namespace_name: a single namespace is created in the output files, with this name
        line_transformer: lines are read in and transformed by this function
        do_create_combined_file_directory: create an additional directory with all outputs
            combined into a single file (useful for running vw against on a single machine)
    """
    def reformat_files_for_vw(self, input_path, output_path, namespace_name, line_transformer, \
                              do_create_combined_file_directory=True):
        input_files = self._get_input_files(input_path)
        if not os.path.exists(output_path):
            os.mkdir(output_path)
        files_compressed = False

        for input_file in input_files:
            if input_file[-3:] == '.gz':
                files_compressed = True

            output_file = self._get_output_file(output_path, input_file)
            f_out = self._get_file_handle(files_compressed, output_file, 'w')
            f_in = self._get_file_handle(files_compressed, input_path + '/' + input_file)

            for line in f_in.readlines():
                line_reformatted = self._reformat_line(line, namespace_name, line_transformer)
                f_out.write(line_reformatted)
            f_in.close()
        f_out.close()
        if do_create_combined_file_directory:
            self._create_combined_file_directory(output_path, files_compressed)

    """
    function: combine_gz_files

    inputs:
        input_path: directory with .gz files to be combined
        output_path: output directory
        input_number_of_files: number of .gz files that we're combining (TODO: automate this)
        output_number_of_files: number of desired .gz files in output
    """
    def combine_gz_files(self, input_path, output_path, input_number_of_files, output_number_of_files):
        if output_number_of_files > input_number_of_files:
            raise Exception("output number of files must be <= input number of files")
        files_at_a_time = input_number_of_files / output_number_of_files
        number_remainder_files = input_number_of_files % output_number_of_files
        file_index_overall = 0
        for output_file_number in range(output_number_of_files):
            file_index_this_output = 0
            if output_file_number < number_remainder_files:
                files_in_this_output = files_at_a_time + 1
            else:
                files_in_this_output = files_at_a_time
            print 'fito:', files_in_this_output, 'ofn:', output_file_number, 'fio:', file_index_overall
            output_file = gzip.open(output_path + '/part-' + str("%05d" % (output_file_number,)) + '.gz', 'w')
            for file_index_this_output in range(files_in_this_output):
                input_file = gzip.open(input_path + '/part-' + str("%05d" % (file_index_overall,)) + '.gz')
                for line in input_file.readlines():
                    output_file.write(line)
                input_file.close()
                file_index_overall += 1
            output_file.close()

if __name__ == "__main__":
    transformer = VWTransformer()
    #transformer.reformat_files_for_vw(INPUT_PATH, OUTPUT_PATH, NAMESPACE_NAME, LINE_TRANSFORMER, DO_CREATE_COMBINED_FILE_DIRECTORY)
    transformer.combine_gz_files(INPUT_PATH, OUTPUT_PATH, INPUT_NUMBER_OF_FILES, OUTPUT_NUMBER_OF_FILES)
