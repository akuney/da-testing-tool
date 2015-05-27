import datasci.nearest_correlation
import datetime
import gzip
import joblib
import logging
import math
import numpy as np
import os
import pandas as pd
import random
import re
import scipy.stats
import sklearn
import sklearn.ensemble
import sklearn.linear_model
import sklearn.tree
import time

"""
To run on actual ad call logs (using the AdCallGetterFromGenerateSignals class):
    1) Run two emr job to generate aggregated data from the sondag/predict-ppa-ad-price branch, similar to:
            emr --create --jar s3://intentmedia-hawk-output/jon_sondag/jars/jobRunner-hadoop.jar --instance-group master --instance-type m3.2xlarge --instance-count 1 --instance-group core --instance-type m3.2xlarge --instance-count 69 --main-class tasks.generate_signals_ppa_price --args "-stepInputBaseUrl,s3n://intentmedia-hadoop-production/input/,-outputUrl,s3n://intentmedia-hawk-output/jon_sondag/cascalog/20141201-generate-signals-ppa-price-a,-trainingDateFrom,20141001,-trainingDateTo,20141014,-productCategoryType,HOTELS,-adType,META,-nonClickSampleRateDecimal,0.001,-placementType,IN_CARD"
            emr --create --jar s3://intentmedia-hawk-output/jon_sondag/jars/jobRunner-hadoop.jar --instance-group master --instance-type m3.2xlarge --instance-count 1 --instance-group core --instance-type m3.2xlarge --instance-count 69 --main-class tasks.generate_signals_ppa_price --args "-stepInputBaseUrl,s3n://intentmedia-hadoop-production/input/,-outputUrl,s3n://intentmedia-hawk-output/jon_sondag/cascalog/20141201-generate-signals-ppa-price-one-per-a,-trainingDateFrom,20141001,-trainingDateTo,20141014,-productCategoryType,HOTELS,-adType,META,-nonClickSampleRateDecimal,1.0,-placementType,IN_CARD,-oneImpressionPerRequestId,true"
    2) Download the results from the first job to e.g.
        BASE_PATH + normalized_vw.gz
        BASE_PATH + normalized_sampled.gz
        BASE_PATH + variance
        ...and cat the results into a single file within each folder.  Download the .column_names file
        from normalized.gz into normalized_sampled.gz
    3) Download the results from the second job (not all files are needed - just need enough rows to have enough data
       to simulate the auction)'s normalized_vw.gz folder.  Name it something different so that it does not conflict
       with the same folder from the first job, e.g.
        BASE_PATH + normalized_vw_one_per.gz
    4) Run this file with the prepare_inputs() function uncommented, to generate necessary flat files and models
    5) Run the following sql queries and save the results to BASE_PATH + [query_name - sql suffix].csv
            (BASE_PATH is defined in the `if __name__ == '__main__'` function):
        avg_effective_bids.sql
        advertiser_price_wlt_corr.sql
        advertiser_price_wlt_marginals.sql
        ctc_overall_avg.sql
        ctc_rate_where_available.sql
        ctr_position_parity.sql
        hotel_property_ids_by_ad_calls.sql
        hotel_property_ids_by_clicks.sql
        hotel_property_ids_with_advertiser_ids.sql
        hotel_property_ids_with_star_brand_market.sql
        net_conversion_values_with_property_attributes.sql (requires access to sp_publisher_hotel_properties table)
        roi_by_wlt.sql
"""


class FlatFileGenerator(object):
    def __init__(self, file_locs, simulation_config, feature_names_to_remove=None, feature_names_to_keep=None,
                 feature_names_set_to_one_in_simulation=None, overwrite_existing_files=False):
        self._base_path = file_locs.base_path
        self._file_locs = file_locs
        self._simulation_config = simulation_config
        self._logger = simulation_config.logger
        self._overwrite_existing_files = overwrite_existing_files
        self._column_names = self._get_column_names()
        if feature_names_to_keep and feature_names_to_remove is not None:
            raise Exception('must either keep or remove features, not both')
        if feature_names_to_remove is None:
            feature_names_to_remove = []
        self._feature_numbers_to_remove = [self._column_names.index(val) for val in feature_names_to_remove]
        if feature_names_to_keep is None:
            feature_names_to_keep = []
        self._feature_numbers_to_keep = [self._column_names.index(val) for val in feature_names_to_keep]
        if feature_names_set_to_one_in_simulation is None:
            feature_names_set_to_one_in_simulation = []
        self._feature_numbers_set_to_one_in_simulation = \
            [self._column_names.index(val) for val in feature_names_set_to_one_in_simulation]
        self._non_click_sample_rate_decimal = simulation_config.non_click_sample_rate_decimal
        self._verbose = simulation_config.verbose

    def _print(self, text):
        if self._verbose:
            print text

    def _get_column_names(self):
        column_names_file = self._file_locs.column_names_file
        with open(column_names_file, 'r') as fin:
            column_names = fin.readline().strip().split('\t')[2:]
            return column_names

    def generate_flat_files(self):
        self._simulation_config.logger.info('Generating flat files...')
        self._generate_grouped_advertiser_property_ids()
        self._generate_hotel_property_ids_to_advertiser_ids_dict()  # depends on generate_grouped step
        self._generate_click_through_rate_model()
        self._generate_net_conversion_value_model()
        self._generate_price_sampler()
        self._generate_position_effects()
        self._simulation_config.logger.info('Finished generating flat files')

    def _generate_grouped_advertiser_property_ids(self):
        output_path = self._file_locs.grouped_advertiser_property_ids_file
        if not os.path.isfile(output_path) or self._overwrite_existing_files:
            self._simulation_config.logger.info(' Generating grouped advertiser property ids...')
            df = self._get_grouped_advertiser_property_ids_df()
            df[['ADVERTISER_ID', 'HOTEL_PROPERTY_ID', 'ADVERTISEMENT_ID', 'AD_GROUP_ID', 'CAMPAIGN_ID']] \
                .groupby(['ADVERTISER_ID']).max().to_csv(output_path)
        else:
            self._simulation_config.logger.info(
                ' Grouped advertiser property ids file already exists (and overwrite mode off)')

    def _get_grouped_advertiser_property_ids_df(self):
        df_raw = pd.read_csv(self._file_locs.input_data_frame_file, header=None, sep='\t')
        df_raw[3] = df_raw[3].apply(lambda x: x[1:-1])
        df = pd.DataFrame(df_raw[3].str.split(' ').tolist())
        df[len(df.columns)] = df_raw[2]
        df = df.astype('float')
        with open(self._file_locs.column_names_file, 'r') as fin:
            line = fin.readline()
            line = line.split('\t')
            df.columns = line[2:]  # skip request_id, requested_at
            df['y_value'][df['y_value'] > 1] = 1
            return df

    def _generate_click_through_rate_model(self):
        self._preprocess_ctr_model_file()
        self._preprocess_simulation_file()
        self._train_ctr_model()

    def _preprocess_ctr_model_file(self):
        self._simulation_config.logger.info(' Preprocessing CTR model training file...')
        ctr_training_file_in = self._file_locs.input_ctr_training_file_path
        ctr_training_file_out = self._file_locs.ctr_training_data_file_name

        if not os.path.isfile(ctr_training_file_out) or self._overwrite_existing_files:
            if os.path.isfile(ctr_training_file_out):
                os.unlink(ctr_training_file_out)
            with gzip.open(ctr_training_file_in, 'r') as fin, gzip.open(ctr_training_file_out, 'w') as fout_ctr:
                for line in fin:
                    fout_train_line = line.split('\t')[2].strip()
                    fout_train_line = re.sub(' +', ' ', fout_train_line)
                    if len(self._feature_numbers_to_keep) > 0:
                        fout_train_line = self._keep_features(fout_train_line)
                    elif len(self._feature_numbers_to_remove) > 0:
                        fout_train_line = self._remove_features(fout_train_line)
                    fout_ctr.write(fout_train_line + '\n')

    def _preprocess_simulation_file(self):
        self._simulation_config.logger.info(' Preprocessing simulation file...')
        simulation_data_file_in = self._file_locs.input_simulation_file_path
        simulation_data_file_out = self._file_locs.simulation_data_file_name

        if not os.path.isfile(simulation_data_file_out) or not os.path.isfile(simulation_data_file_out):
            if os.path.isfile(simulation_data_file_out):
                os.unlink(simulation_data_file_out)
            with gzip.open(simulation_data_file_in, 'r') as fin, gzip.open(simulation_data_file_out, 'w') as fout_sim:
                for line in fin:
                    line = re.sub(' +', ' ', line)
                    fout_simulation_line = line.split('\t')[2].strip()
                    if len(self._feature_numbers_set_to_one_in_simulation) > 0:
                        fout_simulation_line = self._set_features_to_one(fout_simulation_line)
                    fout_sim.write(fout_simulation_line + '\n')

    def _keep_features(self, line):
        line_processed_arr = []
        line_arr = line.split(' ')
        line_processed_arr.append(line_arr[0])
        line_processed_arr.append(line_arr[1])
        for feature_number in self._feature_numbers_to_keep:
            loc = re.search(' ' + str(feature_number) + '[:-][\w.-]+', line)
            if loc is not None:
                line_processed_arr.append(loc.group(0).strip())
        return ' '.join(line_processed_arr)


    def _remove_features(self, line):
        line_processed = line
        for feature_number in self._feature_numbers_to_remove:
            line_processed = re.sub(' ' + str(feature_number) + '[:-][\w.-]+', '', line_processed)
        return line_processed

    def _set_features_to_one(self, line):
        line_processed = line
        for feature_number in self._feature_numbers_set_to_one_in_simulation:
            line_processed = re.sub(' ' + str(feature_number) + '([:-])[\w.-]+',
                                    ' ' + str(feature_number) + r'\g<1>' + '1.0', line_processed)
        return line_processed

    def _train_ctr_model(self):
        self._simulation_config.logger.info(' Training CTR model...')
        model_file_path = self._file_locs.ctr_model_file_name

        if not os.path.isfile(model_file_path) or self._overwrite_existing_files:
            df_train = self._get_grouped_advertiser_property_ids_df()
            ctr_columns_not_categorical = self._simulation_config.ctr_column_names_not_categorical
            ctr_columns_categorical = self._simulation_config.ctr_column_names_categorical
            df_train_wanted_cols = df_train[ctr_columns_categorical + ctr_columns_not_categorical]
            one_hot_cols = ctr_columns_categorical
            df_train_one_hot_cols = StaticUtils.with_one_hot_columns(df_train_wanted_cols, one_hot_cols)
            ctr_model = sklearn.linear_model.LogisticRegression()
            ctr_model.fit(df_train_one_hot_cols, df_train['y_value'])
            click_through_rate_model = ClickThroughRateModel(ctr_model, df_train_wanted_cols.columns,
                                                             one_hot_cols,
                                                             df_train_one_hot_cols.columns)
            joblib.dump(click_through_rate_model, self._file_locs.ctr_model_file_name)
        else:
            self._simulation_config.logger.info(' Trained CTR model already exists (and overwrite mode off)')

    def _get_feature_numbers(self, feature_names):
        return [self._column_names.index(val) for val in feature_names]

    def _generate_net_conversion_value_model(self):
        model_file_path = self._file_locs.ncv_model_file_name
        if not os.path.isfile(model_file_path) or self._overwrite_existing_files:
            self._simulation_config.logger.info(' Generating NCV model...')
            file_path = self._file_locs.ncv_data_file_name
            df = pd.read_csv(file_path, header=None).dropna()
            df.columns = ['net_conversion_value', 'hotel_property_id', 'site_type', 'entity_id', 'weekend_travel',
                          'upcoming', 'market_id', 'brand_id', 'star_rating']
            del df['site_type']
            del df['entity_id']
            # TODO: consider a tree for speed here
            model = self._simulation_config.ncv_model
            y_value = df.pop('net_conversion_value')
            model.fit(df[['hotel_property_id', 'weekend_travel', 'upcoming', 'market_id', 'brand_id', 'star_rating']],
                      y_value)
            # TODO: split into train, test sets, add std dev back in here as necessary
            joblib.dump(model, model_file_path)
        else:
            self._simulation_config.logger.info(' Trained NCV model already exists (and overwrite mode off)')

    def _generate_hotel_property_ids_to_advertiser_ids_dict(self):
        # filters out advertiser_ids if they are not found in grouped_advertiser_property_ids.csv
        unique_advertiser_id_lists_output_path = self._file_locs.unique_advertiser_id_lists_file
        unique_adv_id_identifiers_output_path = self._file_locs.unique_adv_id_identifiers_file
        grouped_advertiser_ids_df = pd.read_csv(self._file_locs.grouped_advertiser_property_ids_file)
        eligible_advertiser_ids = set(grouped_advertiser_ids_df['ADVERTISER_ID'].unique())
        if not os.path.isfile(unique_advertiser_id_lists_output_path) or self._overwrite_existing_files:
            self._simulation_config.logger.info(' Generating hotel property ids to advertiser ids dict...')
            eligible_advertiser_ids = \
                set(pd.read_csv(self._file_locs.grouped_advertiser_property_ids_file)['ADVERTISER_ID'])
            df = pd.read_csv(self._file_locs.hotel_property_ids_with_advertiser_ids_file, header=None)
            df.columns = ['hotel_property_id', 'advertiser_id']
            property_ids_to_adv_ids = {k: tuple(sorted(filter(lambda x: x in eligible_advertiser_ids, list(v)))) for
                                       k, v
                                       in df.groupby('hotel_property_id')['advertiser_id']}
            series = pd.Series(property_ids_to_adv_ids)
            series_unique = series.unique()
            list_ids_to_unique_adv_id_tuples = dict(zip(range(len(series_unique)), series_unique))
            unique_adv_id_tuples_to_list_ids = dict(zip(series_unique, range(len(series_unique))))
            unique_ids_series = pd.Series([unique_adv_id_tuples_to_list_ids[x] for x in series.values],
                                          index=series.index)
            tuples_series = pd.Series(list_ids_to_unique_adv_id_tuples)
            unique_ids_series.to_csv(unique_adv_id_identifiers_output_path)
            tuples_series.to_csv(unique_advertiser_id_lists_output_path)
        else:
            self._simulation_config.logger.info(
                ' Hotel property ids to advertiser ids dict file already exists (and overwrite mode off)')

    def _generate_price_sampler(self):
        output_path = self._file_locs.price_sampler_file
        if not os.path.isfile(output_path) or self._overwrite_existing_files:
            self._simulation_config.logger.info(' Generating price sampler...')
            all_advertiser_ids = sorted(self._simulation_config.us_advertiser_ids + \
                                        self._simulation_config.uk_advertiser_ids)
            price_sampler = PriceSampler(self._simulation_config.discrete_corr_num_samples_per_iteration,
                                         self._simulation_config.discrete_corr_epsilon,
                                         self._simulation_config.assumed_wlt_corr_value_when_missing,
                                         self._file_locs.advertiser_price_wlt_corr_file,
                                         self._file_locs.advertiser_price_marginals_file,
                                         all_advertiser_ids,
                                         self._simulation_config.uk_advertiser_ids,
                                         self._simulation_config.us_advertiser_ids)
            joblib.dump(price_sampler, output_path)
        else:
            self._simulation_config.logger.info(' Price sampler file already exists (and overwrite mode off)')

    def _generate_position_effects(self):
        output_path = self._file_locs.position_effects_file
        if not os.path.isfile(output_path) or self._overwrite_existing_files:
            self._simulation_config.logger.info(' Generating price parities list (for position effects...')
            df = pd.read_csv(self._file_locs.ctr_position_parity_file, header=None)
            df.columns = ['site_id', 'site_name', 'auction_position', 'impressions', 'clicks', 'ctr']
            df_sites = pd.DataFrame(df.groupby('site_id').sum()['impressions'] / df.groupby('site_id').sum()['clicks'])
            df_all = df.join(df_sites, on=['site_id'])
            df_all['ctr_adj'] = df_all['ctr'] * df_all[0]
            df_all = df_all[['site_id', 'auction_position', 'ctr_adj']]
            df_all = df_all.set_index(['site_id', 'auction_position'])
            position_effects = dict()
            for site_id in df_sites.index:
                vals = np.array([val[0] for val in df_all.xs(site_id, level='site_id').values])
                position_effects[site_id] = sorted(vals, reverse=True)  # for now, assume we're sorting positions by CTR
            joblib.dump(position_effects, output_path)
        else:
            self._simulation_config.logger.info(
                ' Price parities list file (for position effects) already exists (and overwrite mode off)')


class FileLocations(object):
    def __init__(self, base_path, column_names_file, input_data_frame_file, input_ctr_training_file_path,
                 input_simulation_file_path, ctr_model_working_dir, ctr_model_file_name, ctr_training_data_file_name,
                 simulation_data_file_name, ncv_model_file_name,
                 ncv_data_file_name, grouped_advertiser_property_ids_file, average_effective_bids_file,
                 normalization_factors_file, hotel_property_ids_with_advertiser_ids_file, unique_advertiser_lists_file,
                 property_ids_to_unique_advertiser_list_ids_file, hotel_property_ids_with_star_brand_market_file,
                 hotel_property_ids_by_ad_calls_file, hotel_property_ids_by_clicks_file,
                 roi_by_wlt_file, advertiser_price_wlt_corr_file, advertiser_price_marginals_file, price_sampler_file,
                 ctr_position_parity_file, position_effects_file, ctc_overall_avg_file, ctc_rate_where_available_file,
                 result_logs_file):
        self.base_path = base_path
        self.column_names_file = base_path + column_names_file
        self.input_data_frame_file = base_path + input_data_frame_file
        self.input_ctr_training_file_path = base_path + input_ctr_training_file_path
        self.input_simulation_file_path = base_path + input_simulation_file_path
        self.ctr_model_working_dir = base_path + ctr_model_working_dir
        self.ctr_model_file_name = base_path + ctr_model_file_name
        self.ctr_training_data_file_name = base_path + ctr_training_data_file_name
        self.simulation_data_file_name = base_path + simulation_data_file_name
        self.ncv_model_file_name = base_path + ncv_model_file_name
        self.ncv_data_file_name = base_path + ncv_data_file_name
        self.grouped_advertiser_property_ids_file = base_path + grouped_advertiser_property_ids_file
        self.average_effective_bids_file = base_path + average_effective_bids_file
        self.normalization_factors_file = base_path + normalization_factors_file
        self.hotel_property_ids_with_advertiser_ids_file = base_path + hotel_property_ids_with_advertiser_ids_file
        self.unique_advertiser_id_lists_file = base_path + unique_advertiser_lists_file
        self.unique_adv_id_identifiers_file = base_path + property_ids_to_unique_advertiser_list_ids_file
        self.hotel_property_ids_with_star_brand_market_file = base_path + hotel_property_ids_with_star_brand_market_file
        self.hotel_property_ids_by_ad_calls_file = base_path + hotel_property_ids_by_ad_calls_file
        self.hotel_property_ids_by_clicks_file = base_path + hotel_property_ids_by_clicks_file
        self.roi_by_wlt_file = base_path + roi_by_wlt_file
        self.advertiser_price_wlt_corr_file = base_path + advertiser_price_wlt_corr_file
        self.advertiser_price_marginals_file = base_path + advertiser_price_marginals_file
        self.price_sampler_file = base_path + price_sampler_file
        self.ctr_position_parity_file = base_path + ctr_position_parity_file
        self.position_effects_file = base_path + position_effects_file
        self.ctc_overall_avg_file = base_path + ctc_overall_avg_file
        self.ctc_rate_where_available_file = base_path + ctc_rate_where_available_file
        self.result_logs_file = base_path + result_logs_file

    @staticmethod
    def get_file_name_only(value):
        last_slash_index = len(value) - value[::-1].index('/')
        return value[last_slash_index:]

    def create_model_directories(self, directories):
        for directory in directories:
            if not os.path.exists(self.base_path + directory):
                os.makedirs(self.base_path + directory)


class SimulationConfig(object):
    def __init__(self, base_path, num_ad_calls_to_simulate,
                 ncv_model, non_click_sample_rate_decimal, discrete_corr_num_samples_per_iteration,
                 discrete_corr_epsilon, discrete_corr_max_iterations, assumed_wlt_corr_value_when_missing,
                 ad_call_required_fields,
                 us_advertiser_names, uk_advertiser_names, us_advertiser_ids, uk_advertiser_ids,
                 us_corresponding_ct_advertiser_names, uk_corresponding_ct_advertiser_names,
                 us_corresponding_ct_advertiser_ids, uk_corresponding_ct_advertiser_ids,
                 us_sources_some_properties_from_affiliate_networks,
                 uk_sources_some_properties_from_affiliate_networks,
                 site_adv_blacklists, ctr_column_names_not_categorical, ctr_column_names_categorical, verbose):
        self.timestamp = '_' + datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        self.logger = self._set_up_logger(base_path)
        self.num_ad_calls_to_simulate = num_ad_calls_to_simulate
        self.ncv_model = ncv_model
        self.non_click_sample_rate_decimal = non_click_sample_rate_decimal
        self.discrete_corr_num_samples_per_iteration = discrete_corr_num_samples_per_iteration
        self.discrete_corr_epsilon = discrete_corr_epsilon
        self.discrete_corr_max_iterations = discrete_corr_max_iterations
        self.assumed_wlt_corr_value_when_missing = assumed_wlt_corr_value_when_missing
        self.ad_call_required_fields = ad_call_required_fields
        self.us_advertiser_names = us_advertiser_names
        self.uk_advertiser_names = uk_advertiser_names
        self.us_advertiser_ids = us_advertiser_ids
        self.uk_advertiser_ids = uk_advertiser_ids
        self.us_corresponding_ct_advertiser_names = us_corresponding_ct_advertiser_names
        self.uk_corresponding_ct_advertiser_names = uk_corresponding_ct_advertiser_names
        self.us_corresponding_ct_advertiser_ids = us_corresponding_ct_advertiser_ids
        self.uk_corresponding_ct_advertiser_ids = uk_corresponding_ct_advertiser_ids
        self.us_sources_some_properties_from_affiliate_networks = us_sources_some_properties_from_affiliate_networks
        self.uk_sources_some_properties_from_affiliate_networks = uk_sources_some_properties_from_affiliate_networks
        self.site_adv_blacklists = site_adv_blacklists
        self.ctr_column_names_not_categorical = ctr_column_names_not_categorical
        self.ctr_column_names_categorical = ctr_column_names_categorical
        self.verbose = verbose

    def _set_up_logger(self, log_path):
        if not os.path.exists(log_path):
            os.makedirs(log_path)
        logging.basicConfig(level=logging.DEBUG)
        logger = logging.getLogger(__name__)
        # TODO: Set up RotatingFileHandler
        handler = logging.FileHandler(log_path + '/logfile' + self.timestamp + '.log', mode='w')
        logger.addHandler(handler)
        logger.info('Initiated logger')
        return logger


class AuctionRevenueData(object):
    def __init__(self, simulation_config, base_path, auction_name, advertiser_ids):
        self._base_path = base_path
        self._auction_name = auction_name
        self._advertiser_prices = pd.DataFrame(index=advertiser_ids)
        self._publisher_data = pd.DataFrame(index=['site_id', 'publisher_price', 'minimum_bid'])
        self._advertiser_effects = pd.DataFrame(index=advertiser_ids)
        self._advertiser_values_per_click = pd.DataFrame(index=advertiser_ids)
        self._advertiser_payments = pd.DataFrame(index=advertiser_ids)
        self._realized_advertiser_utilities = pd.DataFrame(index=advertiser_ids)
        self._max_efficiency_advertiser_utilities = pd.DataFrame(index=advertiser_ids)
        self._position_effects = pd.DataFrame(index=advertiser_ids)
        self._auction_positions = pd.DataFrame(index=advertiser_ids)
        self._simulation_config = simulation_config
        self._logger = simulation_config.logger
        # self._logger.info('Generating AuctionRevenueData (%s)', auction_name)
        self._id_to_name_map = self._get_id_to_name_map()

    def add_result(self, site_id, publisher_hotel_price, minimum_bids, advertiser_ids, advertiser_prices, advertiser_effects,
                   advertiser_values_per_click, advertiser_payments, realized_advertiser_utilities,
                   max_efficiency_advertiser_utilities, position_effects, auction_positions):
        current_cols = len(self._advertiser_prices.columns)
        self._advertiser_prices[current_cols] = advertiser_prices
        self._publisher_data[current_cols] = pd.Series({'site_id': site_id, 'publisher_price': publisher_hotel_price,
                                                        'minimum_bid': minimum_bids[0]})  # min bids equal for all adv
        self._advertiser_effects[current_cols] = pd.Series(dict(zip(advertiser_ids, advertiser_effects)))
        self._advertiser_values_per_click[current_cols] = advertiser_values_per_click
        self._advertiser_payments[current_cols] = pd.Series(dict(zip(advertiser_ids, advertiser_payments)))
        self._realized_advertiser_utilities[current_cols] = pd.Series(dict(zip(advertiser_ids,
                                                                               realized_advertiser_utilities)))
        self._max_efficiency_advertiser_utilities[current_cols] = pd.Series(dict(zip(advertiser_ids,
                                                                      max_efficiency_advertiser_utilities)))
        self._position_effects[current_cols] = pd.Series(dict(zip(advertiser_ids, position_effects)))
        self._auction_positions[current_cols] = pd.Series(dict(zip(advertiser_ids, auction_positions)))

    def _transpose_data_structures(self):
        # data is logged column by column for performance purposes; invert it here before printing stats
        self._advertiser_prices = self._advertiser_prices.T
        self._publisher_data = self._publisher_data.T
        self._advertiser_effects = self._advertiser_effects.T
        self._advertiser_values_per_click = self._advertiser_values_per_click.T
        self._advertiser_payments = self._advertiser_payments.T
        self._realized_advertiser_utilities = self._realized_advertiser_utilities.T
        self._max_efficiency_advertiser_utilities = self._max_efficiency_advertiser_utilities.T
        self._position_effects = self._position_effects.T
        self._auction_positions = self._auction_positions.T

    def print_stats(self):
        self._transpose_data_structures()
        self.log_diagnostics()
        # aggregate stats
        sum_realized_advertiser_utilities = self._realized_advertiser_utilities.sum().sum()
        sum_advertiser_payments = self._advertiser_payments.sum().sum()
        self._logger.debug(
            'max total utilities, all advertisers: %0.4f' % self._max_efficiency_advertiser_utilities.sum().sum())
        self._logger.debug('total utilities, all advertisers: %0.4f' % sum_realized_advertiser_utilities)
        self._logger.debug('total payments, all advertisers (i.e. auctioneer revenue): %0.4f' % sum_advertiser_payments)
        self._logger.debug('total advertiser surplus, all advertisers (i.e. utilities - payments): %0.4f' % \
                           (sum_realized_advertiser_utilities - sum_advertiser_payments) + '\n')
        pd.options.display.float_format = '{:20.6f}'.format
        # per-advertiser stats
        # print '\ntotal utilities by advertiser:\n', self._realized_advertiser_utilities.sum()
        # print '\nmean utilities by advertiser:\n', self._realized_advertiser_utilities.mean()
        # print '\ntotal payments by advertiser:\n', self._advertiser_payments.sum()
        # print '\nmean payments by advertiser:\n', self._advertiser_payments.mean()
        # utils = (self._advertiser_values_per_click * self._advertiser_effects *
        # self._position_effects).iloc[0].sort(inplace=False, ascending=False)
        # Set up diagnostics

    def get_auction_summary_dict(self):
        sum_realized_advertiser_utilities = self._realized_advertiser_utilities.sum().sum()
        sum_realized_advertiser_utilities_lower_price = self._realized_advertiser_utilities[
            self._advertiser_prices < self._publisher_data.loc['publisher_price']].sum().sum()
        sum_realized_advertiser_utilities_eq_or_higher_price = self._realized_advertiser_utilities[
            self._advertiser_prices >= self._publisher_data.loc['publisher_price']].sum().sum()
        sum_advertiser_payments = self._advertiser_payments.sum().sum()
        sum_realized_advertiser_payments_lower_price = self._advertiser_payments[
            self._advertiser_prices < self._publisher_data.loc['publisher_price']].sum().sum()
        sum_realized_advertiser_payments_eq_or_higher_price = self._advertiser_payments[
            self._advertiser_prices >= self._publisher_data.loc['publisher_price']].sum().sum()
        mean_minimum_bid = self._publisher_data.T['minimum_bid'].mean()
        std_minimum_bid = self._publisher_data.T['minimum_bid'].std()
        return {'total_utilities': sum_realized_advertiser_utilities,
                'total_payments': sum_advertiser_payments,
                'advertiser_surplus': sum_realized_advertiser_utilities - sum_advertiser_payments,
                'total_utilities_lower_price': sum_realized_advertiser_utilities_lower_price,
                'total_utilities_eq_or_higher_price': sum_realized_advertiser_utilities_eq_or_higher_price,
                'total_payments_lower_price': sum_realized_advertiser_payments_lower_price,
                'total_payments_eq_or_higher_price': sum_realized_advertiser_payments_eq_or_higher_price,
                'min_bid_mean': mean_minimum_bid,
                'min_bid_std': std_minimum_bid}

    def _get_id_to_name_map(self):
        all_advertiser_ids = self._simulation_config.uk_advertiser_ids + self._simulation_config.us_advertiser_ids
        all_advertiser_names = self._simulation_config.uk_advertiser_names + self._simulation_config.us_advertiser_names
        return dict(zip(all_advertiser_ids, all_advertiser_names))

    def log_diagnostics(self):
        if self._auction_name == 'simple_auction':
            self._logger.info('\nStart diagnostics ...')
            self.log_price_corr()
            self.log_ctr_by_wlt()
            self.log_wlt_by_site_and_advertiser()
            self.log_number_advertisers_per_auction()
            self.log_average_value_per_click_by_advertiser()
            self.log_ctr_vpc_scatter()

    def log_price_corr(self):
        ddf_prices = (self._advertiser_prices.sub(self._publisher_data['publisher_price'], axis='index')).astype(float)
        ddf_prices_uk = ddf_prices[self._simulation_config.uk_advertiser_ids]
        ddf_prices_us = ddf_prices[self._simulation_config.us_advertiser_ids]
        ddf_columns_map = self._get_id_to_name_map()
        ddf_prices.columns = [ddf_columns_map[col] for col in ddf_prices.columns]
        ddf_prices_uk.columns = [ddf_columns_map[col] for col in ddf_prices_uk.columns]
        ddf_prices_us.columns = [ddf_columns_map[col] for col in ddf_prices_us.columns]
        self._logger.info('All advertisers price correlation matrix:')
        self._logger.info(ddf_prices.corr())
        self._logger.info('UK advertisers price correlation matrix:')
        self._logger.info(ddf_prices_uk.corr())
        self._logger.info('US advertisers price correlation matrix:')
        self._logger.info(ddf_prices_us.corr())
        ddf_prices.corr().to_csv(path_or_buf=self._base_path + 'logs/price_corr' +
                                 self._simulation_config.timestamp + '.csv', mode='w')
        ddf_prices_uk.corr().to_csv(path_of_buf=self._base_path + 'log/price_corr_uk' +
                                    self._simulation_config.timestamp + '.csv', mode='w')
        ddf_prices_us.corr().to_csv(path_of_buf=self._base_path + 'log/price_corr_us' +
                                    self._simulation_config.timestamp + '.csv', mode='w')

    def log_ctr_by_wlt(self):
        advertiser_prices = self._get_copy_df_no_duplicate_col_names(self._advertiser_prices)
        advertiser_effects = self._get_copy_df_no_duplicate_col_names(self._advertiser_effects)
        ddf_prices = (advertiser_prices.sub(self._publisher_data['publisher_price'], axis='index')).astype(float)
        df_ctr = advertiser_effects  # * self._position_effects
        win_ctrs = []
        lose_ctrs = []
        tie_ctrs = []
        for col in df_ctr.columns:
            win_ctrs_col = df_ctr[ddf_prices[col] < 0][col]
            win_ctrs_col_gt_0 = win_ctrs_col[win_ctrs_col > 0]
            for win_ctr in win_ctrs_col_gt_0:
                win_ctrs.append(win_ctr)
            lose_ctrs_col = df_ctr[ddf_prices[col] > 0][col]
            lose_ctrs_col_gt_0 = lose_ctrs_col[lose_ctrs_col > 0]
            for lose_ctr in lose_ctrs_col_gt_0:
                lose_ctrs.append(lose_ctr)
            tie_ctrs_col = df_ctr[ddf_prices[col] == 0][col]
            tie_ctrs_col_gt_0 = tie_ctrs_col[tie_ctrs_col > 0]
            for tie_ctr in tie_ctrs_col_gt_0:
                tie_ctrs.append(tie_ctr)
        format_str = '{:.6f}'
        self._logger.info('win_ctr: ' + format_str.format((np.mean(win_ctrs))))
        self._logger.info('lose_ctr: ' + format_str.format((np.mean(lose_ctrs))))
        self._logger.info('tie_ctr: ' + format_str.format((np.mean(tie_ctrs))))

    def _get_copy_df_no_duplicate_col_names(self, df):
        df_copy = df.copy()
        df_copy_t = df_copy.T
        df_copy_t['index'] = df_copy_t.index
        no_dupes = df_copy_t.drop_duplicates(subset='index')
        del no_dupes['index']
        return no_dupes.T

    def log_wlt_by_site_and_advertiser(self):
        advertiser_prices = self._get_copy_df_no_duplicate_col_names(self._advertiser_prices)
        ddf_prices = (advertiser_prices.sub(self._publisher_data['publisher_price'], axis='index')).astype(float)
        publisher_data = self._publisher_data
        for site_id in publisher_data['site_id'].unique():
            this_site_counts = pd.DataFrame(columns=[-1, 0, 1])
            ddf_prices_site = ddf_prices[publisher_data['site_id'] == site_id]
            for col in ddf_prices_site.columns:
                ddf_prices.loc[publisher_data['site_id'] == site_id, col] = pd.Series.round(ddf_prices_site[col])
                ddf_prices_site[col] = pd.Series.round(ddf_prices_site[col])
                this_site_counts = this_site_counts.append(pd.DataFrame(ddf_prices_site[col].value_counts(),
                                                                        columns=[self._id_to_name_map[col]]).T)
            this_site_counts = this_site_counts.dropna(how='all').fillna(0).astype(int)
            remappings = {-1: 'win', 0: 'tie', 1: 'lose'}
            this_site_counts.columns = [remappings[col_name] for col_name in this_site_counts.columns]
            self._logger.info('wlt info by advertiser for site_id:' + str(site_id))
            self._logger.info(this_site_counts)

    def log_number_advertisers_per_auction(self):
        us_sites = {2, 3}
        uk_sites = {4, 33, 37}
        us_ad_call_indices = [(val in us_sites) for val in self._publisher_data['site_id']]
        uk_ad_call_indices = [(val in uk_sites) for val in self._publisher_data['site_id']]
        advertiser_prices_us = self._advertiser_prices[us_ad_call_indices]
        advertiser_prices_uk = self._advertiser_prices[uk_ad_call_indices]
        self._logger.info('Number of auctions by advertiser count, all:')
        self._logger.info(self._advertiser_prices.T.count().value_counts().sort_index(ascending=False))
        self._logger.info('Number of auctions by advertiser count, UK:')
        self._logger.info(advertiser_prices_uk.T.count().value_counts().sort_index(ascending=False))
        self._logger.info('Number of auctions by advertiser count, US:')
        self._logger.info(advertiser_prices_us.T.count().value_counts().sort_index(ascending=False))
        count_by_advertiser = self._advertiser_prices.count()
        count_by_advertiser = count_by_advertiser[count_by_advertiser > 0]
        count_by_advertiser.index = [self._id_to_name_map[val] for val in count_by_advertiser.index]
        self._logger.info('Number of auctions per advertiser, all:')
        self._logger.info(count_by_advertiser)
        count_by_advertiser_uk = advertiser_prices_uk.count()
        count_by_advertiser_uk = count_by_advertiser_uk[count_by_advertiser_uk > 0]
        count_by_advertiser_uk.index = [self._id_to_name_map[val] for val in count_by_advertiser_uk.index]
        self._logger.info('Number of auctions per advertiser, UK:')
        self._logger.info(count_by_advertiser_uk)
        count_by_advertiser_us = advertiser_prices_us.count()
        count_by_advertiser_us = count_by_advertiser_us[count_by_advertiser_us > 0]
        count_by_advertiser_us.index = [self._id_to_name_map[val] for val in count_by_advertiser_us.index]
        self._logger.info('Number of auctions per advertiser, US:')
        self._logger.info(count_by_advertiser_us)

    def log_average_value_per_click_by_advertiser(self):
        mean_vpc = self._advertiser_values_per_click.mean()
        uk_advertisers = self._simulation_config.uk_advertiser_ids
        us_advertisers = self._simulation_config.us_advertiser_ids
        mean_vpc_uk = mean_vpc[mean_vpc.index.isin(uk_advertisers)]
        mean_vpc_us = mean_vpc[mean_vpc.index.isin(us_advertisers)]
        mean_vpc.index = [self._id_to_name_map[val] for val in mean_vpc.index]
        mean_vpc_uk.index = [self._id_to_name_map[val] for val in mean_vpc_uk.index]
        mean_vpc_us.index = [self._id_to_name_map[val] for val in mean_vpc_us.index]
        self._logger.info('Mean value per click, all advertisers:')
        self._logger.info(mean_vpc)
        self._logger.info('Mean value per click, UK advertisers:')
        self._logger.info(mean_vpc_uk)
        self._logger.info('Mean value per click, US advertisers:')
        self._logger.info(mean_vpc_us)

    def log_ctr_vpc_scatter(self):
        df_scatter = pd.DataFrame(self._advertiser_values_per_click.stack(), columns=['vpc'])
        df_scatter['ctr'] = self._advertiser_effects.stack()
        df_scatter.to_csv(self._base_path + 'logs/ctr_vpc' + self._simulation_config.timestamp + '.csv', mode='w')


def run_auction_multiprocess_fn(arg_tuple):
    auction = arg_tuple[0]
    auction_name = arg_tuple[1]
    advertiser_information = arg_tuple[2]
    hotel_property_id = arg_tuple[3]
    publisher_hotel_price = arg_tuple[4]
    site_id = arg_tuple[5]
    publisher_id = arg_tuple[6]
    includes_saturday_night = arg_tuple[7]
    advance_days = arg_tuple[8]
    return auction.get_auction_results(hotel_property_id, publisher_hotel_price, site_id, advertiser_information,
                                       publisher_id, includes_saturday_night, advance_days),\
        auction_name, site_id, publisher_hotel_price


class AllAuctions(object):
    def __init__(self, simulation_config, base_path, advertisers, publisher, auctions):
        self._base_path = base_path
        self._simulation_config = simulation_config
        self._logger = simulation_config.logger
        self._advertisers = advertisers
        self._publisher = publisher
        self._auctions = auctions
        self._auction_revenue_data_dict = self._get_auction_revenue_data_dict()

    def _get_auction_revenue_data_dict(self):
        auction_revenues = dict()
        for auction_name in self._auctions.keys():
            auction_revenues[auction_name] = \
                AuctionRevenueData(self._simulation_config, self._base_path, auction_name,
                                   self._advertisers.get_advertiser_ids())
        return auction_revenues

    def run_auctions(self, ad_call):
        advertiser_prices = self._advertisers.get_advertiser_prices_draw(ad_call)
        advertiser_effects = self._advertisers.get_advertiser_effects_draw(ad_call, advertiser_prices)
        advertiser_values_per_click = self._advertisers.get_value_per_click_draw(ad_call, advertiser_prices)
        advertiser_information = pd.DataFrame({'advertiser_prices': advertiser_prices,
                                               'advertiser_effects': advertiser_effects,
                                               'advertiser_values_per_click': advertiser_values_per_click})
        # pool = mp.Pool(processes=7)
        arg_tuples = []
        for auction_name, auction in self._auctions.iteritems():
            arg_tuples.append([auction,
                               auction_name,
                               advertiser_information,
                               ad_call.get_field_value('HOTEL_PROPERTY_ID'),
                               ad_call.get_unnormalized_field_value('PUBLISHER_HOTEL_PRICE'),
                               ad_call.get_field_value('SITE_ID'),
                               ad_call.get_field_value('PUBLISHER_ID'),
                               ad_call.get_field_value('INCLUDES_SATURDAY_NIGHT'),
                               ad_call.get_unnormalized_field_value('ADVANCE_DAYS')])
        # results = pool.map(run_auction_multiprocess_fn, arg_tuples)
        results = map(run_auction_multiprocess_fn, arg_tuples)
        # pool.terminate()

        for result in results:
            bidders = result[0]
            auction_name = result[1]
            site_id = result[2]
            publisher_hotel_price = result[3]

            self._auction_revenue_data_dict[auction_name] \
                .add_result(site_id,
                            publisher_hotel_price,
                            [bidders[advertiser_id].minimum_bid for advertiser_id in advertiser_information.index],
                            advertiser_information.index,
                            advertiser_information['advertiser_prices'],
                            advertiser_information['advertiser_effects'],
                            advertiser_information['advertiser_values_per_click'],
                            [bidders[advertiser_id].payment for advertiser_id in advertiser_information.index],
                            [bidders[advertiser_id].realized_utility for advertiser_id in advertiser_information.index],
                            [bidders[advertiser_id].max_efficiency_utility for advertiser_id in
                             advertiser_information.index],
                            [bidders[advertiser_id].position_effect for advertiser_id in advertiser_information.index],
                            [bidders[advertiser_id].auction_position for advertiser_id in advertiser_information.index])

    def print_stats(self):
        all_auction_results = pd.DataFrame(columns=['total_utilities', 'total_payments',
                                                    'advertiser_surplus', 'total_utilities_lower_price',
                                                    'total_utilities_eq_or_higher_price',
                                                    'min_bid_mean', 'min_bid_std'])
        for auction_name in sorted(self._auctions.keys()):
            all_auction_results = all_auction_results.append(
                pd.DataFrame(self._auction_revenue_data_dict[auction_name].get_auction_summary_dict(),
                             index=[auction_name]))
        all_auction_results = all_auction_results.sort('total_payments', ascending=False, inplace=False)
        print 'Overall auction results:'
        print all_auction_results
        all_auction_results.to_csv(path_or_buf=self._base_path + 'logs/all_auction_results' +\
                                               self._simulation_config.timestamp + '.csv', mode='w')
        self._auction_revenue_data_dict['simple_auction'].print_stats()


class HpaAuction(object):
    def __init__(self, position_effects, simple_auction_lower, simple_auction_equal_or_higher,
                 num_slots=4, min_ads_shown=2, inequality_threshold=1e-6):
        self._position_effects = position_effects
        self._simple_auction_lower = simple_auction_lower
        self._simple_auction_equal_or_higher = simple_auction_equal_or_higher
        self._min_ads_shown = min_ads_shown
        self._num_slots = num_slots
        self._inequality_threshold = inequality_threshold

    def get_auction_results(self, hotel_property_id, publisher_hotel_price, site_id, advertiser_information,
                            publisher_id, includes_saturday_night, advance_days):
        advertiser_information_lower = self._get_advertiser_information_lower(publisher_hotel_price, advertiser_information)
        auction_results_lower = self._simple_auction_lower.get_auction_results(
            hotel_property_id, publisher_hotel_price, site_id, advertiser_information_lower,
            publisher_id, includes_saturday_night, advance_days)
        num_ads_shown_lower = self._get_num_ads_shown(auction_results_lower)
        self._set_equal_or_higher_position_effects(num_ads_shown_lower)
        self._set_equal_or_higher_num_slots(num_ads_shown_lower)
        advertiser_information_equal_or_higher = self._get_advertiser_information_equal_or_higher(publisher_hotel_price,
                                                                                                  advertiser_information)
        auction_results_equal_or_higher = self._simple_auction_equal_or_higher.get_auction_results(
            hotel_property_id, publisher_hotel_price, site_id, advertiser_information_equal_or_higher,
            publisher_id, includes_saturday_night, advance_days)
        auction_results = dict()
        for k, v in auction_results_lower.iteritems():
            auction_results[k] = v
        for k, v in auction_results_equal_or_higher.iteritems():
            if v.auction_position < np.inf:
                v.auction_position += num_ads_shown_lower
            auction_results[k] = v
        self._set_max_efficiency_advertiser_utilities(site_id, auction_results)
        return auction_results

    def _get_advertiser_information_lower(self, publisher_hotel_price, advertiser_information):
        publisher_hotel_price_threshold = publisher_hotel_price - self._inequality_threshold
        return advertiser_information[advertiser_information['advertiser_prices'] < publisher_hotel_price_threshold]

    def _get_advertiser_information_equal_or_higher(self, publisher_hotel_price, advertiser_information):
        publisher_hotel_price_threshold = publisher_hotel_price - self._inequality_threshold
        return advertiser_information[advertiser_information['advertiser_prices'] >= publisher_hotel_price_threshold]

    def _set_equal_or_higher_position_effects(self, num_ads_shown_lower):
        new_position_effects = PositionEffects.get_copy_with_lower_positions(self._position_effects,
                                                                             num_ads_shown_lower)
        self._simple_auction_equal_or_higher.set_position_effects(new_position_effects)

    def _set_equal_or_higher_num_slots(self, num_ads_shown_lower):
        self._simple_auction_equal_or_higher.set_num_slots(self._num_slots - num_ads_shown_lower)

    def _get_num_ads_shown(self, auction_results_lower):
        ad_shown_list = [bidder.ad_shown for bidder in auction_results_lower.values()]
        return int(np.sum(ad_shown_list))

    def _set_max_efficiency_advertiser_utilities(self, site_id, auction_results):
        # overwrite the max utilities returned by the sub-auctions since they do not see all advertisers
        bidders_by_utility = sorted(auction_results.iteritems(), key=lambda x: x[1].vpc_by_effect, reverse=True)
        for slot_idx in range(len(auction_results)):
            if slot_idx < self._num_slots:
                bidders_by_utility[slot_idx][1].max_efficiency_utility = \
                    bidders_by_utility[slot_idx][1].vpc_by_effect * \
                    self._position_effects.get_position_effects(site_id)[slot_idx]
            else:
                bidders_by_utility[slot_idx][1].max_efficiency_utility = 0.0


class SimpleAuction(object):
    def __init__(self, position_effects, num_slots=4, min_ads_shown=2, reserve_price=0.0, reserve_price_type='qwr',
                 qs_exponent=1.0, competitive_bid_squashing=False, competitive_qs_exponent=0.0,
                 non_competitive_qs_exponent=1.0, minimum_bid_getter=None, reserve_price_getter=None):
        self._position_effects = position_effects
        self._num_slots = num_slots
        self._min_ads_shown = min_ads_shown
        self._reserve_price = reserve_price
        self._reserve_price_type = reserve_price_type
        self._qs_exponent = qs_exponent
        self._competitive_bid_squashing = competitive_bid_squashing
        self._competitive_qs_exponent = competitive_qs_exponent
        self._non_competitive_qs_exponent = non_competitive_qs_exponent
        if minimum_bid_getter is None:
            self._minimum_bid_getter = MinimumBidGetter()
        else:
            self._minimum_bid_getter = minimum_bid_getter
        self._reserve_price_getter = reserve_price_getter

    def set_position_effects(self, position_effects):
        self._position_effects = position_effects

    def set_num_slots(self, num_slots):
        self._num_slots = num_slots

    @staticmethod
    def _is_competitive_auction(publisher_hotel_price, advertiser_prices):
        # TODO: Use display price logic instead of truncating
        # publisher_display_price = int(ad_call.get_unnormalized_field_value('PUBLISHER_HOTEL_PRICE'))
        publisher_display_price = int(publisher_hotel_price)
        for advertiser_price in advertiser_prices:
            advertiser_display_price = int(advertiser_price)
            if advertiser_display_price < publisher_display_price:
                return True
        return False

    def get_auction_results(self, hotel_property_id, publisher_hotel_price, site_id, advertiser_info,
                            publisher_id, includes_saturday_night, advance_days):
        if self._reserve_price_getter is not None:
            self._reserve_price = self._reserve_price_getter.get_minimum_bid(advertiser_info.index.tolist(),
                                                                             hotel_property_id, publisher_id,
                                                                             includes_saturday_night,
                                                                             advance_days)
        advertiser_info_copy = advertiser_info.copy()
        advertiser_info_copy['advertiser_minimum_bids'] = \
            pd.Series(self._minimum_bid_getter.get_minimum_bids(advertiser_info_copy.index, hotel_property_id,
                                                                publisher_id, includes_saturday_night, advance_days))
        is_competitive = self._is_competitive_auction(publisher_hotel_price, advertiser_info_copy['advertiser_prices'].values)
        qs_exponent = self._get_qs_exponent(is_competitive)
        advertiser_info_copy['advertiser_effects_transformed'] = [val ** qs_exponent for val in
                                                             advertiser_info_copy['advertiser_effects']]
        bidders = [Bidder(*vals) for vals in zip(
            np.maximum(advertiser_info_copy['advertiser_values_per_click'], advertiser_info_copy['advertiser_minimum_bids']) *
            advertiser_info_copy['advertiser_effects_transformed'],
            range(len(advertiser_info_copy)),
            advertiser_info_copy.index,
            advertiser_info_copy['advertiser_effects'],
            advertiser_info_copy['advertiser_effects_transformed'],
            advertiser_info_copy['advertiser_values_per_click'],
            np.maximum(advertiser_info_copy['advertiser_values_per_click'], advertiser_info_copy['advertiser_minimum_bids']),
            advertiser_info_copy['advertiser_values_per_click'] * advertiser_info_copy['advertiser_effects'],
            np.maximum(advertiser_info_copy['advertiser_values_per_click'], advertiser_info_copy['advertiser_minimum_bids']) *
            advertiser_info_copy['advertiser_effects'],
            advertiser_info_copy['advertiser_minimum_bids'])]
        self._set_beats_reserve_price(bidders)
        bidders = sorted(bidders, key=lambda x: (x.beats_reserve_price, x.vpc_adj_by_effect_transformed), reverse=True)
        eligible_bidders = self._get_eligible_bidders(bidders)
        if len(eligible_bidders) >= self._min_ads_shown:
            self._set_advertiser_payments_and_positions(site_id, eligible_bidders)
            self._set_realized_advertiser_utilities(eligible_bidders)
            self._set_max_efficiency_advertiser_utilities(site_id, bidders)
        results_dict = dict(zip([bidder.advertiser_id for bidder in bidders], bidders))
        return results_dict

    def _get_qs_exponent(self, is_competitive):
        if not self._competitive_bid_squashing:
            return self._qs_exponent
        elif self._competitive_bid_squashing and is_competitive:
            return self._competitive_qs_exponent
        elif self._competitive_bid_squashing and not is_competitive:
            return self._non_competitive_qs_exponent
        else:
            raise Exception('qs exponent type not supported')

    def _set_beats_reserve_price(self, bidders):
        for bidder in bidders:
            if self._reserve_price_type == 'uwr':
                if bidder.value_per_click_adj >= self._reserve_price:
                    bidder.beats_reserve_price = True
            elif self._reserve_price_type == 'qwr':
                if bidder.vpc_adj_by_effect >= self._reserve_price:
                    bidder.beats_reserve_price = True

    def _get_eligible_bidders(self, bidders):
        eligible_bidders = []
        for bidder in bidders:
            if len(eligible_bidders) <= self._num_slots:
                if bidder.beats_reserve_price:
                    eligible_bidders.append(bidder)
        return eligible_bidders

    def _set_advertiser_payments_and_positions(self, site_id, eligible_bidders):
        # site_id = ad_call.get_field_value('SITE_ID')

        for slot_idx in range(min(len(eligible_bidders), self._num_slots)):
            eligible_bidders[slot_idx].auction_position = slot_idx
            eligible_bidders[slot_idx].position_effect = self._position_effects.get_position_effects(site_id)[slot_idx]
            self._set_slot_payment(slot_idx, eligible_bidders, site_id)

    def _set_slot_payment(self, slot_idx, eligible_bidders, site_id):
        if slot_idx < len(eligible_bidders):
            payment = 0.0
            w_s = eligible_bidders[slot_idx].effect_transformed
            e_s = eligible_bidders[slot_idx].effect
            for addition_slot_idx in range(slot_idx, min(len(eligible_bidders), self._num_slots)):
                if addition_slot_idx < len(eligible_bidders) - 1:
                    payment += self._get_payment_increment(eligible_bidders, site_id, addition_slot_idx, w_s, e_s)
                else:
                    payment += self._get_payment_increment_reserve(site_id, addition_slot_idx, w_s, e_s)
            eligible_bidders[slot_idx].payment = payment
            eligible_bidders[slot_idx].ad_shown = 1

    def _get_payment_increment(self, eligible_bidders, site_id, addition_slot_idx, w_s, e_s):
        w_t_plus_one = eligible_bidders[addition_slot_idx + 1].effect_transformed
        if addition_slot_idx == self._num_slots - 1:
            x_t_plus_one = 0
        else:
            x_t_plus_one = self._position_effects.get_position_effects(site_id)[addition_slot_idx + 1]
        v_t_plus_one = eligible_bidders[addition_slot_idx + 1].value_per_click_adj
        x_t = self._position_effects.get_position_effects(site_id)[addition_slot_idx]
        return (w_t_plus_one / w_s) * e_s * (x_t - x_t_plus_one) * v_t_plus_one

    def _get_payment_increment_reserve(self, site_id, addition_slot_idx, w_s, e_s):
        if self._reserve_price_type == 'uwr':
            x_t = self._position_effects.get_position_effects(site_id)[addition_slot_idx]
            x_t_plus_one = 0.0
            return e_s * (x_t - x_t_plus_one) * self._reserve_price
        elif self._reserve_price_type == 'qwr':
            # as in Lahaie/Pennock, a bidder with value r and weight 1, who is the first excluded bidder
            x_t = self._position_effects.get_position_effects(site_id)[addition_slot_idx]
            x_t_plus_one = 0.0
            return (x_t - x_t_plus_one) * self._reserve_price

    def _set_realized_advertiser_utilities(self, eligible_bidders):
        for idx, eligible_bidder in enumerate(eligible_bidders):
            if idx < self._num_slots:
                eligible_bidder.realized_utility = \
                    eligible_bidders[idx].effect * \
                    eligible_bidders[idx].position_effect * \
                    eligible_bidders[idx].value_per_click * \
                    eligible_bidders[idx].ad_shown

    def _set_max_efficiency_advertiser_utilities(self, site_id, bidders):
        num_advertisers = len(bidders)
        bidders_by_utility = sorted(bidders, key=lambda x: x.vpc_by_effect, reverse=True)
        for slot_idx in range(min(num_advertisers, self._num_slots)):
            bidders_by_utility[slot_idx].max_efficiency_utility = \
                bidders_by_utility[slot_idx].vpc_by_effect * \
                self._position_effects.get_position_effects(site_id)[slot_idx]


class Bidder(object):
    def __init__(self, vpc_adj_by_effect_transformed, index, advertiser_id, effect,
                 effect_transformed, value_per_click, value_per_click_adj, vpc_by_effect, vpc_adj_by_effect,
                 minimum_bid):
        self.vpc_adj_by_effect_transformed = vpc_adj_by_effect_transformed
        self.index = index
        self.advertiser_id = advertiser_id
        self.effect = effect
        self.effect_transformed = effect_transformed
        self.value_per_click = value_per_click
        self.value_per_click_adj = value_per_click_adj
        self.vpc_by_effect = vpc_by_effect
        self.vpc_adj_by_effect = vpc_adj_by_effect
        self.minimum_bid = minimum_bid
        self.payment = 0.0
        self.ad_shown = 0
        self.realized_utility = 0.0
        self.max_efficiency_utility = 0.0
        self.auction_position = np.inf
        self.beats_reserve_price = False
        self.position_effect = 0.0

    def __repr__(self):
        return 'index:' + str(self.index) + ' effect:' + str(self.effect) + \
               ' value_per_click:' + str(self.value_per_click) + ' vpc_adj_by_effect:' + str(self.vpc_adj_by_effect) + \
               ' beats_reserve_price:' + str(self.beats_reserve_price) + \
               ' payment:' + str(self.payment) + ' max_efficiency_utility:' + str(self.max_efficiency_utility)


class PropertiesByClicksTopN(object):
    def __init__(self, file_locs, top_properties_decimal=0.2,
                 num_clicks_to_use_for_thresold_est=10000):
        self._file_locs = file_locs
        self._top_properties_decimal = top_properties_decimal
        self._num_clicks_to_use_for_threshold_est = num_clicks_to_use_for_thresold_est
        self._top_properties_set = self._get_top_properties()

    def get_ranker_type(self):
        return 'top_n_pct'

    def _get_top_properties(self):
        if self._file_locs is None:
            raise Exception('file_locs cannot be None when using a n pct clicks min bid getter')
        else:
            clicks_df = pd.read_csv(self._file_locs.hotel_property_ids_by_clicks_file, header=None)
            clicks_df.columns = ['hotel_property_id', 'num_clicks']
            threshold = self._get_threshold(clicks_df)
            df_top = clicks_df[clicks_df['num_clicks'] >= threshold]
            return set(df_top['hotel_property_id'])

    def _get_threshold(self, clicks_df):
        column_names = AdCallGetterFromGenerateSignals.get_column_names(self._file_locs.column_names_file)
        num_clicks_list = []
        num_clicks_dict = dict(zip(clicks_df['hotel_property_id'], clicks_df['num_clicks']))
        with gzip.open(self._file_locs.simulation_data_file_name) as fin:
            lines_read = 0
            while lines_read < self._num_clicks_to_use_for_threshold_est:
                line = fin.readline()
                hotel_property_id =\
                    AdCallGetterFromGenerateSignals.get_feature_from_vw_line(column_names, line,
                                                                             'HOTEL_PROPERTY_ID', '-')
                if hotel_property_id in num_clicks_dict:
                    num_clicks_list.append(num_clicks_dict[hotel_property_id])
                else:
                    num_clicks_list.append(0)
                lines_read += 1
        ser = pd.Series(num_clicks_list)
        return ser.quantile(1 - self._top_properties_decimal)

    def is_top_property(self, hotel_property_id, publisher_id, includes_saturday_night, advance_days):
        return hotel_property_id in self._top_properties_set


class PropertiesByAdCallsTopN(object):
    def __init__(self, file_locs, top_properties_decimal=0.2,
                 num_ad_calls_to_use_for_threshold_est=1000):
        self._file_locs = file_locs
        self._top_properties_decimal = top_properties_decimal
        self._num_ad_calls_to_use_for_threshold_est = num_ad_calls_to_use_for_threshold_est
        self._top_properties_set = self._get_top_properties()

    def get_ranker_type(self):
        return 'top_n_pct'

    def _get_top_properties(self):
        if self._file_locs is None:
            raise Exception('file_locs cannot be None when using a n pct imps min bid getter')
        else:
            ad_calls_df = pd.read_csv(self._file_locs.hotel_property_ids_by_ad_calls_file, header=None)
            ad_calls_df.columns = ['hotel_property_id', 'num_ad_calls']
            threshold = self._get_threshold(ad_calls_df)
            df_top = ad_calls_df[ad_calls_df['num_ad_calls'] >= threshold]
            return set(df_top['hotel_property_id'])

    def _get_threshold(self, ad_calls_df):
        column_names = AdCallGetterFromGenerateSignals.get_column_names(self._file_locs.column_names_file)
        num_clicks_list = []
        num_clicks_dict = dict(zip(ad_calls_df['hotel_property_id'], ad_calls_df['num_ad_calls']))
        with gzip.open(self._file_locs.simulation_data_file_name) as fin:
            lines_read = 0
            while lines_read < self._num_ad_calls_to_use_for_threshold_est:
                line = fin.readline()
                hotel_property_id = \
                    AdCallGetterFromGenerateSignals.get_feature_from_vw_line(column_names, line,
                                                                             'HOTEL_PROPERTY_ID', '-')
                if hotel_property_id in num_clicks_dict:
                    num_clicks_list.append(num_clicks_dict[hotel_property_id])
                else:
                    num_clicks_list.append(0)
                lines_read += 1
        ser = pd.Series(num_clicks_list)
        return ser.quantile(1 - self._top_properties_decimal)

    def is_top_property(self, hotel_property_id, publisher_id, includes_saturday_night, advance_days):
        return hotel_property_id in self._top_properties_set


class PropertiesByDefault(object):
    def __init__(self):
        pass

    def get_ranker_type(self):
        return 'top_n_pct'

    def is_top_property(self, hotel_property_id, publisher_id, includes_saturday_night, advance_days):
        return False


class PropertiesByPredictedValueTopN(object):
    def __init__(self, file_locs, top_properties_decimal=0.2,
                 num_ad_calls_to_use_for_threshold_est=1000):
        print 'Generating properties by top predicted value...'
        self._file_locs = file_locs
        self._ncv_model = NetConversionValueModel(file_locs)
        self._top_properties_decimal = top_properties_decimal
        self._num_ad_calls_to_use_for_threshold_est = num_ad_calls_to_use_for_threshold_est
        self._threshold = self._get_threshold()
        print '...done generating properties by top predicted value'

    def get_ranker_type(self):
        return 'top_n_pct'

    def _get_threshold(self):
        column_names = AdCallGetterFromGenerateSignals.get_column_names(self._file_locs.column_names_file)
        thresholds_list = []
        with gzip.open(self._file_locs.simulation_data_file_name) as fin:
            lines_read = 0
            while lines_read < self._num_ad_calls_to_use_for_threshold_est:
                line = fin.readline()
                ad_call = AdCall(line, [], column_names, self._file_locs)
                hotel_property_id = ad_call.get_field_value('HOTEL_PROPERTY_ID')
                publisher_id = ad_call.get_field_value('PUBLISHER_ID')
                includes_saturday_night = ad_call.get_field_value('INCLUDES_SATURDAY_NIGHT')
                advance_days = ad_call.get_unnormalized_field_value('ADVANCE_DAYS')
                thresholds_list.append(self._ncv_model.
                                       get_net_conversion_value_from_fields(hotel_property_id, publisher_id,
                                                                            includes_saturday_night, advance_days))
                lines_read += 1
        ser = pd.Series(thresholds_list)
        return ser.quantile(1 - self._top_properties_decimal)

    def is_top_property(self, hotel_property_id, publisher_id, includes_saturday_night, advance_days):
        predicted_ncv = self._ncv_model.get_net_conversion_value_from_fields(hotel_property_id, publisher_id,
                                                                             includes_saturday_night, advance_days)
        if predicted_ncv > self._threshold:
            return True
        else:
            return False


class PropertiesByPredictedValue(object):
    def __init__(self, file_locs):
        self._file_locs = file_locs
        self._ncv_model = NetConversionValueModel(file_locs)

    def get_ranker_type(self):
        return 'predicted_value'

    def get_minimum_bid(self, hotel_property_id, publisher_id, includes_saturday_night, advance_days):
        return self._ncv_model.get_net_conversion_value_from_fields(hotel_property_id, publisher_id,
                                                                             includes_saturday_night, advance_days)


class MinimumBidGetter(object):
    def __init__(self, property_ranker=None, min_bid_override=0.0,
                 min_bid_override_for_top_n_pct_properties=0.0, min_bid_ncv_pct_multiplier=1.0):
        if property_ranker is None:
            self._property_ranker = PropertiesByDefault()
        else:
            self._property_ranker = property_ranker
        self._min_bid_override = min_bid_override
        self._min_bid_override_for_top_n_pct_properties = min_bid_override_for_top_n_pct_properties
        self._min_bid_ncv_pct_multiplier = min_bid_ncv_pct_multiplier
        self._overall_average_ctc = 1.0
        if self._property_ranker.get_ranker_type() == 'predicted_value':
            file_locs = self._property_ranker._file_locs
            self._overall_average_ctc = ClickToConversionModel.read_overall_average_ctc_value_file(
                file_locs.ctc_overall_avg_file)

    def get_minimum_bid(self, advertiser_ids, hotel_property_id, publisher_id, includes_saturday_night, advance_days):
        return self.get_minimum_bids(advertiser_ids, hotel_property_id, publisher_id, includes_saturday_night,
                                     advance_days).values()[0]

    def get_minimum_bids(self, advertiser_ids, hotel_property_id, publisher_id, includes_saturday_night, advance_days):
        min_bid = self._min_bid_override
        if self._property_ranker.get_ranker_type() == 'top_n_pct':
            if self._property_ranker.is_top_property(hotel_property_id, publisher_id, includes_saturday_night,
                                                     advance_days):
                min_bid = self._min_bid_override_for_top_n_pct_properties
            result = dict()
            for advertiser_id in advertiser_ids:
                result[advertiser_id] = min_bid
            return result
        elif self._property_ranker.get_ranker_type() == 'predicted_value':
            result = dict()
            min_bid = self._overall_average_ctc * self._min_bid_ncv_pct_multiplier *\
                self._property_ranker.get_minimum_bid(hotel_property_id, publisher_id, includes_saturday_night,
                                                            advance_days)
            for advertiser_id in advertiser_ids:
                result[advertiser_id] = min_bid
            return result
        else:
            raise Exception('Invalid property_ranker type')

    def is_top_property(self, hotel_property_id, publisher_id, includes_saturday_night, advance_days):
        return self._property_ranker.is_top_property(hotel_property_id, publisher_id, includes_saturday_night,
                                                     advance_days)


class ClickThroughRateModel(object):
    def __init__(self, ctr_model, column_names, one_hot_column_names, column_names_expanded):
        self._ctr_model = ctr_model
        self._column_names = column_names
        self._one_hot_column_names = one_hot_column_names
        self._column_names_expanded = column_names_expanded

    @staticmethod
    def from_file(file_locs):
        return joblib.load(file_locs.ctr_model_file_name)

    def get_click_through_rate(self, X, non_click_sample_rate=1.0, ctr_multiplier=1.0):
        predict_proba = self._ctr_model.predict_proba(X)[0][1]
        linear_value = -np.log(1 / predict_proba - 1)
        linear_value += np.log(non_click_sample_rate)
        result = ctr_multiplier / (1 + np.exp(-linear_value))
        return result

class Advertisers(object):
    def __init__(self, file_locs, simulation_config, ctr_model, net_conversion_value_model, click_to_conversion_model,
                 ctr_multiplier=1):
        self._ctr_model = ctr_model
        self._ncv_model = net_conversion_value_model
        self._ctc_model = click_to_conversion_model
        self._all_us_advertiser_ids = simulation_config.us_advertiser_ids
        self._all_uk_advertiser_ids = simulation_config.uk_advertiser_ids
        self._advertiser_ids_sourcing_from_affiliate_networks =\
            self._get_ids_sourcing_from_affiliate_networks(simulation_config)
        self._all_advertiser_ids = sorted(self._all_us_advertiser_ids + self._all_uk_advertiser_ids)
        self._advertiser_field_mappings_df = pd.read_csv(file_locs.grouped_advertiser_property_ids_file,
                                                         index_col=['ADVERTISER_ID'])
        self._num_advertisers = len(self._all_advertiser_ids)
        self._price_sampler = joblib.load(file_locs.price_sampler_file)
        self._non_click_sample_rate = simulation_config.non_click_sample_rate_decimal
        self._ctr_multiplier = ctr_multiplier

    @staticmethod
    def _get_ids_sourcing_from_affiliate_networks(simulation_config):
        advertiser_ids_sourcing_from_affiliate_networks = set()
        for idx, advertiser_id in enumerate(simulation_config.us_advertiser_ids):
            if simulation_config.us_sources_some_properties_from_affiliate_networks[idx]:
                advertiser_ids_sourcing_from_affiliate_networks.add(advertiser_id)
        for idx, advertiser_id in enumerate(simulation_config.uk_advertiser_ids):
            if simulation_config.uk_sources_some_properties_from_affiliate_networks[idx]:
                advertiser_ids_sourcing_from_affiliate_networks.add(advertiser_id)
        return advertiser_ids_sourcing_from_affiliate_networks

    def get_advertiser_ids(self):
        return self._all_advertiser_ids

    def get_num_advertisers(self):
        return len(self._all_advertiser_ids)

    def get_value_per_click_draw(self, ad_call, advertiser_prices):
        advertiser_ids = ad_call.advertiser_ids
        values = dict()
        for idx, advertiser_id in enumerate(advertiser_ids):
            if advertiser_ids[idx] in advertiser_ids:
                ncv_pred = self._ncv_model.get_net_conversion_value(ad_call)
                ncv_estimate = max(ncv_pred + 0.03 * (ncv_pred * (random.random() - 0.5)), 0.01)
                # ncv_estimate = self._ncv_model.get_net_conversion_value(ad_call)
                ncv_affiliate_network_adjustment = self._get_ncv_affiliate_network_adjustment(
                    ad_call.get_field_value('HOTEL_PROPERTY_ID'), advertiser_id)
                ctc_estimate = self._ctc_model.get_click_to_conversion_rate(ad_call,
                                                                            advertiser_ids[idx], advertiser_prices)
                values[advertiser_id] = ncv_estimate * ncv_affiliate_network_adjustment * ctc_estimate
        return values

    def get_advertiser_effects_draw(self, ad_call, advertiser_prices):
        values = dict()
        for idx, advertiser_id in enumerate(ad_call.advertiser_ids):
            values[advertiser_id] = self._get_advertiser_effect(idx, advertiser_prices, ad_call)
        return values

    def get_advertiser_prices_draw(self, ad_call):
        publisher_hotel_price = ad_call.get_unnormalized_field_value('PUBLISHER_HOTEL_PRICE')
        site_id = ad_call.get_field_value('SITE_ID')
        return self._price_sampler.get_price_draw(publisher_hotel_price, site_id, ad_call.advertiser_ids)

    def _get_advertiser_effect(self, idx, advertiser_prices, ad_call):
        advertiser_ids = ad_call.advertiser_ids
        advertiser_id = advertiser_ids[idx]
        ad_call_modified = ad_call.get_copy_with_modified_values(
            dict(self._advertiser_field_mappings_df.loc[advertiser_id]))
        X = ad_call_modified.get_ctr_X_values(self._ctr_model, advertiser_prices, advertiser_id)
        return self._ctr_model.get_click_through_rate(X, self._non_click_sample_rate, self._ctr_multiplier)

    def _get_ncv_affiliate_network_adjustment(self, hotel_property_id, advertiser_id):
        if advertiser_id in self._advertiser_ids_sourcing_from_affiliate_networks:
            if random.random() < 0.2:  # TODO: consider making this dependent on hotel_property_id
                return 0.5
        return 1.0

class PriceSampler(object):
    def __init__(self, discrete_corr_num_samples_per_iteration, discrete_corr_epsilon,
                 assumed_wlt_corr_value_when_missing,
                 corr_file, marginals_file, all_advertiser_ids, all_uk_advertiser_ids,
                 all_us_advertiser_ids, default_site_to_use=2):
        self._discrete_corr_num_samples_per_iteration = discrete_corr_num_samples_per_iteration
        self._discrete_corr_epsilon = discrete_corr_epsilon
        self._default_site_to_use = default_site_to_use
        self._all_advertiser_ids = all_advertiser_ids
        self._all_uk_advertiser_ids = all_uk_advertiser_ids
        self._all_us_advertiser_ids = all_us_advertiser_ids
        self._num_all_advertisers = len(all_advertiser_ids)
        self._num_uk_advertisers = len(all_uk_advertiser_ids)
        self._num_us_advertisers = len(all_us_advertiser_ids)
        self._default_site_to_use = default_site_to_use
        self._assumed_wlt_corr_value_when_missing = assumed_wlt_corr_value_when_missing
        self._advertiser_price_wlt_corr_dict = self._get_advertiser_price_wlt_corr_dict(corr_file)
        self._advertiser_price_marginals_dict = self._get_advertiser_price_marginals_dict(marginals_file)
        self._advertiser_price_wlt_corr_target_dict = self._get_advertiser_price_wlt_corr_target_dict()

    def _get_unique_advertiser_ids(self):
        unique_advertiser_ids = []
        for val in self._all_advertiser_ids:
            if val not in set(unique_advertiser_ids):
                unique_advertiser_ids.append(val)
        return unique_advertiser_ids

    def _get_advertiser_price_wlt_corr_dict(self, advertiser_price_wlt_corr_file):
        df = pd.read_csv(advertiser_price_wlt_corr_file, header=None)
        df = df.dropna()  # drop rows with correlation = NaN
        df.columns = ['site_id', 'advertiser_id_1', 'advertiser_id_2', 'correlation']
        corr_matrix_dict = dict()
        for site_id in df['site_id'].unique():
            df_site = df[df['site_id'] == site_id]
            average_correlation_value = self._assumed_wlt_corr_value_when_missing
            unique_advertiser_ids = self._get_unique_advertiser_ids()
            num_unique_advertisers = len(unique_advertiser_ids)
            corr_matrix = pd.DataFrame(np.ones([num_unique_advertisers, num_unique_advertisers]) *
                                       average_correlation_value)
            corr_matrix.columns = unique_advertiser_ids
            corr_matrix.index = unique_advertiser_ids
            for val in unique_advertiser_ids:
                corr_matrix[val][val] = 1.0  # set diagonal elements to 1.0
            for row in df_site.iterrows():
                corr_matrix[int(row[1]['advertiser_id_1'])][int(row[1]['advertiser_id_2'])] = row[1]['correlation']
            corr_matrix_pos_semidef = datasci.nearest_correlation.nearcorr(corr_matrix, tol=(1e-6, 1e-6))
            corr_matrix_pos_semidef = pd.DataFrame(corr_matrix_pos_semidef, columns=unique_advertiser_ids,
                                                   index=unique_advertiser_ids)
            corr_matrix_dict[site_id] = corr_matrix_pos_semidef
        return corr_matrix_dict

    def _get_advertiser_price_marginals_dict(self, advertiser_price_marginals_file):
        df = pd.read_csv(advertiser_price_marginals_file, header=None, index_col=[0, 1])
        df.index.names = ['site_id', 'advertiser_id']
        df.columns = ['adv_win_percent', 'adv_tie_percent', 'adv_loss_percent']
        df = (df.T / df.T.sum()).T  # normalize the matrix so that all rows add up to 1
        df = df.fillna(value=df.mean())  # fill in missing values with the column means
        price_marginals_dict = dict()
        print '!!! In PriceSampler, multiplying adv_win_percent by 4/7 to account for num of advertisers in auction !!!'
        df['adv_win_percent'] = df['adv_win_percent'] * 4/7
        df['adv_tie_percent'] = 1 - df['adv_win_percent'] - df['adv_loss_percent']
        for site_id in set([val[0] for val in df.index]):
            marginal_df = df.loc[site_id]
            marginal_df = marginal_df[[val in self._all_advertiser_ids for val in marginal_df.index]]
            marginal_df_mean = marginal_df.mean()
            for advertiser_id in self._all_advertiser_ids:
                if advertiser_id not in marginal_df.index:
                    marginal_df = marginal_df.append(pd.DataFrame(np.atleast_2d(marginal_df_mean),
                                                                  columns=marginal_df_mean.index,
                                                                  index=[advertiser_id]))
            price_marginals_dict[site_id] = marginal_df.sort()
        return price_marginals_dict

    def _get_advertiser_price_wlt_corr_target_dict(self):
        advertiser_price_wlt_corr_target_dict = dict()
        corr_sites = set(self._advertiser_price_wlt_corr_dict.keys())
        marginal_sites = set(self._advertiser_price_marginals_dict.keys())
        for site_id in corr_sites.difference(marginal_sites):
            del self._advertiser_price_wlt_corr_dict[site_id]
        for site_id in marginal_sites.difference(corr_sites):
            del self._advertiser_price_marginals_dict[site_id]
        for site_id in self._advertiser_price_wlt_corr_dict.keys():
            advertiser_price_wlt_corr_target_dict[site_id] = DiscreteTargetCorr.get_target_discrete_corr(
                self._advertiser_price_wlt_corr_dict[site_id],
                self._advertiser_price_marginals_dict[site_id],
                self._discrete_corr_num_samples_per_iteration,
                self._discrete_corr_epsilon,
                self._discrete_corr_epsilon)
        return advertiser_price_wlt_corr_target_dict

    # returns a dict of {advertiser_id: price}
    def get_price_draw(self, publisher_hotel_price, site_id, advertiser_ids):
        prices = dict()
        if site_id not in self._advertiser_price_wlt_corr_target_dict.keys():
            site_id_to_use = self._default_site_to_use
        else:
            site_id_to_use = site_id
        corr = self._advertiser_price_wlt_corr_target_dict[site_id_to_use]
        marginals = self._advertiser_price_marginals_dict[site_id_to_use]
        price_draw = DiscreteTargetCorr.get_sample(corr, marginals, 1)
        for advertiser_id in advertiser_ids:
            prices[advertiser_id] = publisher_hotel_price + price_draw[advertiser_id][0]
        return prices


class DiscreteTargetCorr(object):
    def __init__(self):
        pass

    @staticmethod
    def get_target_discrete_corr(corr, marginals, num_samples_per_iteration, epsilon, max_iterations):
        current_target_continuous_corr = corr.copy()
        current_target_discrete_corr = corr.copy()
        iteration = 0
        curr_epsilon = np.inf
        while curr_epsilon > epsilon and iteration < max_iterations:
            discrete_sample = DiscreteTargetCorr.get_sample(current_target_continuous_corr, marginals,
                                                            num_samples_per_iteration)
            current_target_continuous_corr = \
                DiscreteTargetCorr._adjust_target_corrs(current_target_discrete_corr, current_target_continuous_corr,
                                                        discrete_sample)
            iteration += 1
            curr_epsilon = DiscreteTargetCorr._calculate_epsilon(discrete_sample, current_target_discrete_corr)
        return current_target_continuous_corr

    @staticmethod
    def get_sample(corr, marginals, num_samples):
        means = np.zeros(len(corr))
        continuous_sample = pd.DataFrame(np.random.multivariate_normal(means, corr, num_samples))
        continuous_sample.columns = marginals.index  # both sorted, so this is fine
        discrete_sample = DiscreteTargetCorr._continuous_to_discrete(marginals, continuous_sample)
        return discrete_sample

    @staticmethod
    def _continuous_to_discrete(marginals, continuous_sample):
        sample_copy = continuous_sample.copy()
        for col in continuous_sample.columns:
            loss_tie_cdf = marginals.loc[col]['adv_loss_percent']
            tie_win_cdf = marginals.loc[col]['adv_loss_percent'] + marginals.loc[col]['adv_tie_percent']
            loss_tie_boundary = scipy.stats.norm.ppf(loss_tie_cdf)
            tie_win_boundary = scipy.stats.norm.ppf(tie_win_cdf)
            col_copy = sample_copy[col].copy()
            sample_copy[col][col_copy <= loss_tie_boundary] = 1.0
            sample_copy[col][(col_copy > loss_tie_boundary) &
                             (col_copy <= tie_win_boundary)] = 0.0
            sample_copy[col][col_copy > tie_win_boundary] = -1.0
        return sample_copy

    @staticmethod
    def _adjust_target_corrs(current_target_discrete_corr, current_target_continuous_corr, discrete_sample):
        discrete_sample_corr = discrete_sample.corr()
        # set this arbitrarily to 0.5 since NaN covariance sample values are constant and defined by mean only
        discrete_sample_corr = pd.DataFrame(discrete_sample_corr).fillna(0.5)
        ratio = current_target_discrete_corr / discrete_sample_corr
        ratio[ratio > 4.0] = 4.0
        ratio[ratio < -4.0] = -4.0
        adjusted_matrix = current_target_continuous_corr * ratio
        adjusted_matrix = np.minimum(adjusted_matrix,np.ones((len(adjusted_matrix), len(adjusted_matrix))))
        adjusted_matrix_pos_semidef = datasci.nearest_correlation.nearcorr(adjusted_matrix, tol=(1e-6, 1e-6))
        return adjusted_matrix_pos_semidef

    @staticmethod
    def _calculate_epsilon(discrete_sample, target_corr):
        discrete_corr = discrete_sample.corr()
        return np.max(np.max(np.abs(discrete_corr - target_corr)))


class NetConversionValueModel(object):
    def __init__(self, file_locs, default_value=20.0):
        self._hotel_property_ids_with_star_brand_market = self._get_hotel_property_ids_with_star_brand_market(file_locs)
        self._ncv_model = joblib.load(file_locs.ncv_model_file_name)

    def _get_hotel_property_ids_with_star_brand_market(self, file_locs):
        df = pd.read_csv(file_locs.hotel_property_ids_with_star_brand_market_file, header=None, index_col=[0, 1])
        df.columns = ['star_rating', 'brand_id', 'market_id']
        return df.T

    def get_net_conversion_value(self, ad_call):
        # target: 'net_conversion_value'
        # signals: ['hotel_property_id', 'weekend_travel', 'upcoming', 'market_id', 'brand_id', 'star_rating']
        hotel_property_id = ad_call.get_field_value('HOTEL_PROPERTY_ID')
        publisher_id = ad_call.get_field_value('PUBLISHER_ID')
        includes_saturday_night = ad_call.get_field_value('INCLUDES_SATURDAY_NIGHT')
        advance_days = ad_call.get_unnormalized_field_value('ADVANCE_DAYS')
        return self.get_net_conversion_value_from_fields(hotel_property_id, publisher_id, includes_saturday_night,
                                                         advance_days)

    def get_net_conversion_value_from_fields(self, hotel_property_id, publisher_id, includes_saturday_night,
                                             advance_days):
        data_row = [hotel_property_id, includes_saturday_night]
        if advance_days <= 21:
            data_row.append(1)
        else:
            data_row.append(0)
        try:
            other_fields = self._hotel_property_ids_with_star_brand_market[hotel_property_id, publisher_id]
            data_row.append(other_fields['market_id'])
            data_row.append(other_fields['brand_id'])
            data_row.append(other_fields['star_rating'])
            # TODO: figure out how much noise to add here, check that columns match up with ncv model
            return max(self._ncv_model.predict(data_row)[0] + random.random() - 0.5, 0.01)
        except Exception:
            return 20.0 + random.random()


class ClickToConversionModel(object):
    def __init__(self, file_locs, simulation_config):
        self._us_advertiser_ids = simulation_config.us_advertiser_ids
        self._uk_advertiser_ids = simulation_config.uk_advertiser_ids
        self._advertiser_ids = self._us_advertiser_ids + self._uk_advertiser_ids
        self._us_corresponding_ct_advertiser_ids = simulation_config.us_corresponding_ct_advertiser_ids
        self._uk_corresponding_ct_advertiser_ids = simulation_config.uk_corresponding_ct_advertiser_ids
        self._corresponding_ct_advertiser_ids = self._us_corresponding_ct_advertiser_ids + \
                                                self._uk_corresponding_ct_advertiser_ids
        self._ct_average_effective_bids_by_advertiser = \
            self._get_ct_average_effective_bids(file_locs.average_effective_bids_file)
        self._average_roi_by_wlt = self._get_average_roi_by_wlt(file_locs.roi_by_wlt_file)
        self._overall_average_ctc = self.read_overall_average_ctc_value_file(file_locs.ctc_overall_avg_file)
        self._ctc_rate_where_available_file = file_locs.ctc_rate_where_available_file
        # print 'Click to Conversion model: setting (\'T\', \'L\') and (\'W\', \'L\') case to 1.0'

    def _get_ct_average_effective_bids(self, average_effective_bids_file):
        df = pd.read_csv(average_effective_bids_file, header=None)
        df.columns = ['advertiser_id', 'advertiser_name', 'avg_effective_bid', 'stddev_effective_bid']
        avg_effective_bids_dict = dict(zip(df['advertiser_id'], df['avg_effective_bid']))
        avg_effective_bids = []
        for advertiser_id in self._corresponding_ct_advertiser_ids:
            if advertiser_id in avg_effective_bids_dict.keys():
                avg_effective_bids.append(avg_effective_bids_dict[advertiser_id])
            else:
                raise Exception('advertiser id not found!')
        return avg_effective_bids

    @staticmethod
    def read_overall_average_ctc_value_file(overall_average_ctc_file):
        with open(overall_average_ctc_file, 'r') as fin:
            return float(fin.readline())

    def _get_ctc_rate_where_available(self, ctc_rate_where_available_file):
        df = pd.read_csv(ctc_rate_where_available_file, header=None)
        df.columns = ['advertiser_id', 'impressions', 'clicks', 'clicked_conversions',
                      'clicked_conversion_rate']
        return dict(zip(df['advertiser_id'], df['clicked_conversion_rate']))

    def get_click_to_conversion_rate(self, ad_call, advertiser_id, advertiser_prices):
        advertiser_adjustment = self._get_advertiser_adjustment(advertiser_id)
        wlt_adjustment = self._get_wlt_adjustment(ad_call, advertiser_id, advertiser_prices)
        overall_average_ctc = self._get_overall_average_ctc()
        ctc_rates_dict = self._get_ctc_rate_where_available(self._ctc_rate_where_available_file)
        if advertiser_id in ctc_rates_dict.keys():
            return wlt_adjustment * ctc_rates_dict[advertiser_id]
        else:
            return advertiser_adjustment * wlt_adjustment * overall_average_ctc

    def _get_advertiser_adjustment(self, advertiser_id):
        if advertiser_id in self._advertiser_ids:
            id_idx = self._advertiser_ids.index(advertiser_id)
            return self._ct_average_effective_bids_by_advertiser[id_idx]
        else:
            return 1.0  # TODO: raise exception when advertiser not found

    def _get_wlt_adjustment(self, ad_call, advertiser_id, advertiser_prices):
        wlt_index = self._get_wlt_index(ad_call, advertiser_id, advertiser_prices)
        return self._average_roi_by_wlt[wlt_index]

    def _get_overall_average_ctc(self):
        return self._overall_average_ctc

    def _get_average_roi_by_wlt(self, roi_by_wlt_file):
        df = pd.read_csv(roi_by_wlt_file, header=None)
        df.columns = ['site_type', 'advertiser_id', 'pub_win_loss_tie', 'adv_win_loss_tie', 'impressions', 'clicks',
                      'cpc_revenue', 'clicked_conversion_count', 'clicked_conversion_value']
        grouped_sums = df.groupby(['pub_win_loss_tie', 'adv_win_loss_tie']).sum()
        grouped_sums['roi'] = grouped_sums['clicked_conversion_value'] / grouped_sums['cpc_revenue']
        bids_by_wlt = dict()
        for val in grouped_sums.index:
            bids_by_wlt[val] = grouped_sums.loc[val]['roi']
        return bids_by_wlt

    def _get_wlt_index(self, ad_call, advertiser_id, advertiser_prices):
        advertisers_min_price = np.min(advertiser_prices.values())  # includes this advertiser
        if advertiser_id in advertiser_prices.keys():
            this_advertiser_price = advertiser_prices[advertiser_id]
            publisher_hotel_price = ad_call.get_unnormalized_field_value('PUBLISHER_HOTEL_PRICE')
            if StaticUtils.lt_tol(publisher_hotel_price, advertisers_min_price):
                pub_win_loss_tie = 'W'
            elif StaticUtils.gt_tol(publisher_hotel_price, advertisers_min_price):
                pub_win_loss_tie = 'L'
            else:
                pub_win_loss_tie = 'T'
            if StaticUtils.gt_tol(this_advertiser_price, advertisers_min_price) or \
                    StaticUtils.gt_tol(this_advertiser_price, publisher_hotel_price):
                adv_win_loss_tie = 'L'
            elif StaticUtils.lte_tol(this_advertiser_price, advertisers_min_price) and \
                    StaticUtils.lt_tol(this_advertiser_price, publisher_hotel_price):
                adv_win_loss_tie = 'W-Unique'
            elif StaticUtils.lte_tol(this_advertiser_price, advertisers_min_price) and \
                    StaticUtils.lt_tol(this_advertiser_price, publisher_hotel_price):
                adv_win_loss_tie = 'W-Joint'
            else:
                adv_win_loss_tie = 'T'
            return pub_win_loss_tie, adv_win_loss_tie
        else:
            raise Exception('Advertiser id %d not found' % advertiser_id)


class StaticUtils(object):
    def __init__(self):
        pass

    @staticmethod
    def lt_tol(val1, val2, tol=1e-6):
        return val1 < val2 - tol

    @staticmethod
    def lte_tol(val1, val2, tol=1e-6):
        return val1 <= val2 + tol

    @staticmethod
    def gt_tol(val1, val2, tol=1e-6):
        return val1 > val2 + tol

    @staticmethod
    def gte_tol(val1, val2, tol=1e-6):
        return val1 >= val2 - tol

    @staticmethod
    def eq_tol(val1, val2, tol=1e-6):
        return (val2 - tol) <= val1 <= (val2 + tol)

    @staticmethod
    def with_one_hot_columns(df, one_hot_columns):
        df_copy = df.copy()
        for col in one_hot_columns:
            dummy_df = pd.get_dummies(df_copy[col])
            del df_copy[col]
            for dummy_col in dummy_df.columns:
                df_copy[col + '_' + str(dummy_col)] = dummy_df[dummy_col]
        return df_copy


class Publisher(object):
    def __init__(self):
        pass

    def get_price_draw(self, ad_call, publisher_hotel_price):
        return publisher_hotel_price


class AdCall(object):
    def __init__(self, line, advertiser_ids, column_names, file_locs):
        self.line = line
        self.advertiser_ids = advertiser_ids
        self.column_names = column_names
        self._column_names_dict = dict([(val, idx) for idx, val in enumerate(self.column_names)])
        self._normalization_factors = AdCallStaticUtils.read_normalization_factors_file(
            file_locs.normalization_factors_file)
        self._file_locs = file_locs
        self._sig_num_map = AdCallStaticUtils.read_signal_names_file(file_locs.column_names_file)

    def get_copy_with_modified_values(self, modification_dict):
        modification_dict = dict([(self._column_names_dict[k], v) for k, v in modification_dict.iteritems()])
        new_line = self.line
        for k, v in modification_dict.iteritems():
            start = new_line.index(' ' + str(k))
            end = new_line.index(' ', start + 1)
            delimiter = '-'
            if end == -1:
                end = len(new_line)
            if ':' in new_line[start:end]:
                delimiter = ':'
            new_line = new_line[:start] + ' ' + str(k) + delimiter + '{:.1f}'.format(v) + new_line[end:]
        return AdCall(new_line, self.advertiser_ids, self.column_names, self._file_locs)

    def get_ctr_X_values(self, ctr_model, advertiser_prices, advertiser_id):
        is_rank_tied = self.get_is_rank_tied(advertiser_prices, advertiser_id)
        price_is_less_than_pub_price, price_is_equal_to_pub_price, price_is_greater_than_pub_price =\
            self.get_relative_price_variables(advertiser_id, advertiser_prices)
        result = []
        for col in ctr_model._column_names_expanded:
            if col not in ctr_model._column_names:
                # then it's categorical
                col_name = '_'.join(col.split('_')[:-1])
                field_val = self.get_field_value(col_name)
                if col_name + '_' + '{:.1f}'.format(field_val) == col:
                    result.append(1.0)
                else:
                    result.append(0.0)
            else:
                if col == 'IS_RANK_TIED':
                    result.append(is_rank_tied)
                elif col == 'PRICE_IS_LESS_THAN_PUB_PRICE':
                    result.append(price_is_less_than_pub_price)
                elif col == 'PRICE_IS_EQUAL_TO_PUB_PRICE':
                    result.append(price_is_equal_to_pub_price)
                elif col == 'PRICE_IS_GREATER_THAN_PUB_PRICE':
                    result.append(price_is_greater_than_pub_price)
                else:
                    result.append(self.get_field_value(col))
        return result

    def get_is_rank_tied(self, advertiser_prices, advertiser_id):
        is_rank_tied = 0.0
        advertiser_prices_array = [val for val in advertiser_prices.iteritems()]
        idx = 0
        for idx_candidate, val in enumerate(advertiser_prices_array):
            if val[0] == advertiser_id:
                idx = idx_candidate
        advertiser_prices_array = np.array([val[1] for val in advertiser_prices_array])
        for idx_this, val in enumerate(advertiser_prices_array):
            if val == advertiser_prices_array[idx] and idx_this != idx:
                is_rank_tied = 1.0
        return is_rank_tied

    def get_relative_price_variables(self, advertiser_id, advertiser_prices):
        publisher_hotel_price = self.get_unnormalized_field_value('PUBLISHER_HOTEL_PRICE')
        hotel_average_nightly_rate = advertiser_prices[advertiser_id]
        price_is_less_than_pub_price = 0
        price_is_equal_to_pub_price = 0
        price_is_greater_than_pub_price = 0
        if StaticUtils.eq_tol(hotel_average_nightly_rate, publisher_hotel_price):
            price_is_equal_to_pub_price = 1
        elif StaticUtils.lt_tol(hotel_average_nightly_rate, publisher_hotel_price):
            price_is_less_than_pub_price = 1
        elif StaticUtils.gt_tol(hotel_average_nightly_rate, publisher_hotel_price):
            price_is_greater_than_pub_price = 1
        return price_is_less_than_pub_price, price_is_equal_to_pub_price, price_is_greater_than_pub_price

    def get_field_value(self, field):
        column_number = self._column_names_dict[field]
        match = re.search(str(column_number) + '([:-])' + '([:-]?[\w.]+)', self.line)
        if match is None:
            return 0
        field_value = match.group(2)
        if match.group(1) == ':':
            return float(field_value)
        else:
            return int(math.floor(float(field_value)))

    def contains_field_value(self, field):
        column_number = self._column_names_dict[field]
        return re.search(str(column_number) + '[:-]', self.line) is not None

    def get_unnormalized_field_value(self, field):
        norm_factor = self._get_normalization_scaling_factor(field)
        return self.get_field_value(field) / norm_factor

    def get_normalized_field_value(self, field, value):
        norm_factor = self._get_normalization_scaling_factor(field)
        return value * norm_factor

    def _get_normalization_scaling_factor(self, signal_name):
        variance = self._normalization_factors[self._sig_num_map[signal_name]]
        std_dev = math.sqrt(variance)
        if std_dev > 0:
            return min(0.5 / std_dev, 1.0)
        else:
            return 1.0

    def is_valid(self, min_ads_shown, ad_call_required_fields):
        if len(self.advertiser_ids) < min_ads_shown:
            return False
        for field in ad_call_required_fields:
            if not self.contains_field_value(field):
                return False
        else:
            return True

class AdCallStaticUtils(object):
    def __init__(self):
        pass

    @staticmethod
    def read_normalization_factors_file(normalization_factors_file):
        with open(normalization_factors_file, 'r') as fin:
            line = fin.readline()
            string_array = np.array(line.strip()[1:-1].split(' '))
            string_array = string_array.astype('float')
            return string_array

    @staticmethod
    def read_signal_names_file(signal_names_file):
        with open(signal_names_file, 'r') as fin:
            line = fin.readline().strip()
            return dict([(val, idx) for idx, val in enumerate(line.strip().split('\t')[2:])])


class AdCallGetterFromGenerateSignals(object):
    def __init__(self, advertisers, file_locs, simulation_config, verbose=True):
        self._advertisers = advertisers
        self._hotel_property_ids_with_advertiser_ids = \
            self._load_id_mappings(file_locs.unique_advertiser_id_lists_file,
                                   file_locs.unique_adv_id_identifiers_file)
        self._simulation_file_path = file_locs.simulation_data_file_name
        self._column_names = self.get_column_names(file_locs.column_names_file)
        self._verbose = verbose
        self._file_locs = file_locs
        self._simulation_config = simulation_config
        self._generate_signals_file = gzip.open(self._simulation_file_path, 'r')

    def get_next_ad_call(self):
        while True:
            line = self._generate_signals_file.readline()
            if not line:
                break
            line = line.strip()
            hotel_property_id = self.get_feature_from_vw_line(self._column_names, line, 'HOTEL_PROPERTY_ID', '-')
            if hotel_property_id in self._hotel_property_ids_with_advertiser_ids:
                eligible_advertiser_ids = self._hotel_property_ids_with_advertiser_ids.loc[hotel_property_id]
                ad_call = AdCall(line, eligible_advertiser_ids,
                                 self._column_names, self._file_locs)
                eligible_advertiser_ids = self._filter_by_site_blacklist(ad_call.get_field_value('SITE_ID'), eligible_advertiser_ids)
                ad_call.advertiser_ids = eligible_advertiser_ids

                return ad_call
            else:
                if self._verbose:
                    print 'AdCallGetterFromGenerateSignals: hotel_property_id not found:', str(hotel_property_id)

    def _filter_by_site_blacklist(self, site_id, eligible_advertiser_ids):
        site_adv_blacklists = self._simulation_config.site_adv_blacklists
        if site_id in site_adv_blacklists.keys():
            blacklisted_ids = site_adv_blacklists[site_id]
            result = np.array([adv_id for adv_id in eligible_advertiser_ids if adv_id not in blacklisted_ids])
            return result
        else:
            return eligible_advertiser_ids


    @staticmethod
    def get_column_names(column_names_path):
        with open(column_names_path, 'r') as fin:
            line = fin.readline()
            return line.split('\t')[2:]

    @staticmethod
    def get_feature_from_vw_line(column_names, line, feature_name, feature_delimiter_vw):
        feature_idx = column_names.index(feature_name)
        start_idx = line.index(str(feature_idx) + feature_delimiter_vw)
        end_idx = line.index(' ', start_idx)
        feature_value = line[start_idx:end_idx].split(feature_delimiter_vw)[1]
        if feature_delimiter_vw:
            return int(math.floor(float(feature_value)))
        else:
            return float(feature_value)

    def _load_id_mappings(self, unique_advertiser_id_lists_file, unique_adv_id_identifiers_file):
        unique_advertiser_id_lists = pd.read_csv(unique_advertiser_id_lists_file, header=None, index_col=0)
        unique_advertiser_id_lists = \
            [np.array(re.sub(',', r'', x)[1:-1].split(' ')).astype('int') if len(x) > 2 else np.array(())
             for x in unique_advertiser_id_lists[1]]
        unique_adv_id_identifiers = pd.read_csv(unique_adv_id_identifiers_file, header=None, index_col=0)
        result = pd.Series([unique_advertiser_id_lists[val] for val in unique_adv_id_identifiers[1].values],
                         index=unique_adv_id_identifiers.index)
        return result


class PositionEffects(object):
    def __init__(self, position_effects_dict, default_site=2):
        self._position_effects_dict = position_effects_dict
        self._default_site = default_site

    @classmethod
    def from_file_locs(cls, file_locs, default_site=2):
        return cls(joblib.load(file_locs.position_effects_file), default_site)

    def get_position_effects(self, site_id):
        if site_id in self._position_effects_dict.keys():
            site_id_to_use = site_id
        else:
            site_id_to_use = self._default_site
        return self._position_effects_dict[site_id_to_use]

    def get_default_site(self):
        return self._default_site

    def get_position_effects_dict(self):
        return self._position_effects_dict

    @classmethod
    def get_copy_with_lower_positions(cls, other_position_effects, from_position):
        position_effects_dict = other_position_effects.get_position_effects_dict().copy()
        default_site = other_position_effects.get_default_site()
        for site_id, position_effects in position_effects_dict.iteritems():
            position_effects_dict[site_id] = position_effects_dict[site_id][from_position:]
        return cls(position_effects_dict, default_site)


class SimulationRunner(object):
    def __init__(self, file_locations, simulation_config):
        self._file_locations = file_locations
        self._simulation_config = simulation_config

    def _get_auction_name_string(self, hpa, reserve_type, reserve, reserve_low, reserve_high, qs_exp,
                                 qs_exp_competitive, qs_exp_non_competitive, min_bid_all, min_bid_low_properties,
                                 min_bid_high_properties, min_bid_type_criterion, position_effect_type,
                                 ncv_min_bid_pct_multiplier, ncv_reserve_pct_multiplier):
        return '_'.join([hpa, 'restype', reserve_type, 'res', reserve, 'reslow', reserve_low,
                         'reshigh', reserve_high, 'qsexp', qs_exp, 'qsexpcomp', qs_exp_competitive,
                         'qsexpnoncomp', qs_exp_non_competitive, 'minbidall', min_bid_all,
                         'minbidlow', min_bid_low_properties, 'minbidhigh', min_bid_high_properties,
                         'minbidtype', min_bid_type_criterion, 'posefftype', position_effect_type,
                         'ncvminbidpctmult', ncv_min_bid_pct_multiplier,
                         'ncvreservepctmult', ncv_reserve_pct_multiplier])

    def _get_auctions(self, min_ads_shown):
        position_effects_from_data = PositionEffects.from_file_locs(self._file_locations)
        position_effects_flat = PositionEffects({2: [1.0, 1.0, 1.0, 1.0]})
        num_slots = 4
        properties_by_ad_calls_top_n = PropertiesByAdCallsTopN(self._file_locations, 0.2)
        properties_by_clicks_top_n = PropertiesByClicksTopN(self._file_locations, 0.2)
        properties_by_top_predicted_value_top_n = PropertiesByPredictedValueTopN(self._file_locations, 0.2)
        properties_by_predicted_value = PropertiesByPredictedValue(self._file_locations)
        minimum_bid_getter_clicks_top_n = MinimumBidGetter(properties_by_clicks_top_n,
                                                           min_bid_override=0.5,
                                                           min_bid_override_for_top_n_pct_properties=2.0)
        minimum_bid_getter_ad_calls_top_n = MinimumBidGetter(properties_by_ad_calls_top_n,
                                                             min_bid_override=0.5,
                                                             min_bid_override_for_top_n_pct_properties=2.0)

        qwrs = []  # [0.00005, 0.0001]
        uwrs = [0.0, 0.5, 0.75, 1.0, 1.5, 2.0]
        qs_exponents = [0.0]  # [0.0, 0.5, 1.0]
        min_bids = [0.0, 0.5, 0.75, 1.0, 1.5, 1.75, 2.0]
        competitive_qs_exponents = [0.0, 0.5]
        non_competitive_qs_exponents = [0.0, 0.5, 1.0]
        position_effect_types = ['fromdata']

        auctions = {'simple_auction': SimpleAuction(position_effects_from_data, num_slots, min_ads_shown),
                    'simple_auction_flat': SimpleAuction(position_effects_flat, num_slots, min_ads_shown)}

        for position_effect_type in position_effect_types:
            if position_effect_type == 'flat':
                position_effects = position_effects_flat
            else:
                position_effects = position_effects_from_data
            # for competitive_qs_exponent in competitive_qs_exponents:
            #     for non_competitive_qs_exponent in non_competitive_qs_exponents:
            #         for uwr in uwrs:
            #             for min_bid in min_bids:
            #                 auctions[self._get_auction_name_string('', 'uwr', str(uwr), '', '', '',
            #                                                        str(competitive_qs_exponent),
            #                                                        str(non_competitive_qs_exponent), str(min_bid), '', '',
            #                                                        '', position_effect_type, '', '')] =\
            #                 SimpleAuction(position_effects, num_slots, min_ads_shown, uwr,
            #                               reserve_price_type='uwr',
            #                               competitive_bid_squashing=True,
            #                               competitive_qs_exponent=competitive_qs_exponent,
            #                               non_competitive_qs_exponent=non_competitive_qs_exponent)
            # for min_bid in min_bids:
            #     minimum_bid_getter = MinimumBidGetter(min_bid_override=min_bid)
            #     for qs_exponent in qs_exponents:
            #         for qwr in qwrs:
            #             auctions[self._get_auction_name_string('', 'qwr', str(qwr), '', '', str(qs_exponent),
            #                                                    '', '', str(min_bid), '', '', '',
            #                                                    position_effect_type, '', '')] =\
            #                 SimpleAuction(position_effects, num_slots, min_ads_shown,
            #                               qs_exponent=qs_exponent, reserve_price=qwr, reserve_price_type='qwr',
            #                               minimum_bid_getter=minimum_bid_getter)
            #         for uwr in uwrs:
            #             auctions[self._get_auction_name_string('', 'uwr', str(uwr), '', '', str(qs_exponent),
            #                                                    '', '', str(min_bid), '', '', '',
            #                                                    position_effect_type, '', '')] =\
            #                 SimpleAuction(position_effects, num_slots, min_ads_shown,
            #                               qs_exponent=qs_exponent, reserve_price=uwr, reserve_price_type='uwr',
            #                               minimum_bid_getter=minimum_bid_getter)
            #     for qs_exponent in qs_exponents:
            #         for qwr_lower in qwrs:
            #             for qwr_higher in qwrs:
            #                 simple_auction_lower = SimpleAuction(position_effects, num_slots, min_ads_shown,
            #                                                      qs_exponent=qs_exponent, reserve_price=qwr_lower,
            #                                                      reserve_price_type='qwr',
            #                                                      minimum_bid_getter=minimum_bid_getter)
            #                 simple_auction_higher = SimpleAuction(position_effects, num_slots, min_ads_shown,
            #                                                       qs_exponent=qs_exponent, reserve_price=qwr_higher,
            #                                                       reserve_price_type='qwr',
            #                                                       minimum_bid_getter=minimum_bid_getter)
            #                 hpa_auction = HpaAuction(position_effects, simple_auction_lower, simple_auction_higher)
            #                 auctions[self._get_auction_name_string('hpa', 'qwr', '', str(qwr_lower), str(qwr_higher),
            #                                                        str(qs_exponent), '', '', str(min_bid), '', '', '',
            #                                                        position_effect_type, '', '')] =\
            #                     hpa_auction
            #         for uwr_lower in uwrs:
            #             for uwr_higher in uwrs:
            #                 simple_auction_lower = SimpleAuction(position_effects, num_slots, min_ads_shown,
            #                                                      qs_exponent=qs_exponent, reserve_price=uwr_lower,
            #                                                      reserve_price_type='uwr',
            #                                                      minimum_bid_getter=minimum_bid_getter)
            #                 simple_auction_higher = SimpleAuction(position_effects, num_slots, min_ads_shown,
            #                                                       qs_exponent=qs_exponent, reserve_price=uwr_higher,
            #                                                       reserve_price_type='uwr',
            #                                                       minimum_bid_getter=minimum_bid_getter)
            #                 hpa_auction = HpaAuction(position_effects, simple_auction_lower, simple_auction_higher)
            #                 auctions[self._get_auction_name_string('hpa', 'uwr', '', str(uwr_lower), str(uwr_higher),
            #                                                        str(qs_exponent), '', '', str(min_bid), '', '', '',
            #                                                        position_effect_type, '', '')] =\
            #                     hpa_auction
            min_bid_types = ['ncvtopn', 'adcallstopn', 'clickstopn', 'ncv']  # ['clickstopn', 'adcallstopn', 'ncvtopn', 'ncv']
            ncv_min_bid_pct_multipliers = [0.0, 0.5, 0.75, 1.0, 1.25]
            for min_bid_global in min_bids:
                for min_bid_top in min_bids:
                    if min_bid_top >= min_bid_global:
                        for min_bid_type in min_bid_types:
                            for ncv_min_bid_pct_multiplier in ncv_min_bid_pct_multipliers:
                                if min_bid_type == 'adcallstopn':
                                    minimum_bid_getter = MinimumBidGetter(properties_by_ad_calls_top_n,
                                                                          min_bid_override=min_bid_global,
                                                                          min_bid_override_for_top_n_pct_properties=min_bid_top)
                                elif min_bid_type == 'clickstopn':
                                    minimum_bid_getter = MinimumBidGetter(properties_by_clicks_top_n,
                                                                          min_bid_override=min_bid_global,
                                                                          min_bid_override_for_top_n_pct_properties=min_bid_top)
                                elif min_bid_type == 'ncvtopn':
                                    minimum_bid_getter = MinimumBidGetter(properties_by_top_predicted_value_top_n,
                                                                          min_bid_override=min_bid_global,
                                                                          min_bid_override_for_top_n_pct_properties=min_bid_top)
                                elif min_bid_type == 'ncv':
                                    minimum_bid_getter = MinimumBidGetter(properties_by_predicted_value,
                                                                          min_bid_ncv_pct_multiplier=ncv_min_bid_pct_multiplier)
                                if min_bid_type == 'ncv' or ncv_min_bid_pct_multiplier == 1.0:
                                    if min_bid_type != 'ncv':
                                        ncv_min_bid_pct_multiplier = ''
                                    for qs_exponent in qs_exponents:
                                        for qwr in qwrs:
                                            auctions[self._get_auction_name_string('', 'qwr', str(qwr), '', '', str(qs_exponent),
                                                                                   '', '', '', str(min_bid_global),
                                                                                   str(min_bid_top), min_bid_type,
                                                                                   position_effect_type,
                                                                                   str(ncv_min_bid_pct_multiplier), '')] =\
                                                SimpleAuction(position_effects, num_slots, min_ads_shown,
                                                              qs_exponent=qs_exponent, reserve_price=qwr,
                                                              reserve_price_type='qwr',
                                                              minimum_bid_getter=minimum_bid_getter)
                                        for uwr in uwrs:
                                            auctions[self._get_auction_name_string('', 'uwr', str(uwr), '', '', str(qs_exponent),
                                                                                   '', '', '', str(min_bid_global),
                                                                                   str(min_bid_top), min_bid_type,
                                                                                   position_effect_type,
                                                                                   str(ncv_min_bid_pct_multiplier), '')] =\
                                                SimpleAuction(position_effects, num_slots, min_ads_shown,
                                                              qs_exponent=qs_exponent, reserve_price=uwr,
                                                              reserve_price_type='uwr',
                                                              minimum_bid_getter=minimum_bid_getter)
            print 'creating new auctions'
            ncv_reserve_pct_multipliers = [0.0, 0.5, 0.75, 1.0, 1.25]
            for ncv_min_bid_pct_multiplier in ncv_min_bid_pct_multipliers:
                for ncv_reserve_pct_multiplier in ncv_reserve_pct_multipliers:
                    for qs_exponent in qs_exponents:
                        min_bid_getter =\
                            MinimumBidGetter(properties_by_predicted_value,
                                             min_bid_ncv_pct_multiplier=ncv_min_bid_pct_multiplier)
                        reserve_price_getter =\
                            MinimumBidGetter(properties_by_predicted_value,
                                             min_bid_ncv_pct_multiplier=ncv_reserve_pct_multiplier)
                        auction_name = self._get_auction_name_string('', 'uwr', '', '', '',
                                                                     str(qs_exponent), '', '', '', '', '',
                                                                     'ncv', position_effect_type,
                                                                     str(ncv_min_bid_pct_multiplier),
                                                                     str(ncv_reserve_pct_multiplier))
                        auctions[auction_name] = SimpleAuction(position_effects, num_slots, min_ads_shown,
                                                               qs_exponent=qs_exponent, reserve_price=uwr,
                                                               reserve_price_type='uwr',
                                                               minimum_bid_getter=min_bid_getter,
                                                               reserve_price_getter=reserve_price_getter)
                                    # for qs_exponent in qs_exponents:
                                    #     for qwr_lower in qwrs:
                                    #         for qwr_higher in qwrs:
                                    #             simple_auction_lower = SimpleAuction(position_effects, num_slots, min_ads_shown,
                                    #                                                  qs_exponent=qs_exponent, reserve_price=qwr_lower,
                                    #                                                  reserve_price_type='qwr',
                                    #                                                  minimum_bid_getter=minimum_bid_getter)
                                    #             simple_auction_higher = SimpleAuction(position_effects, num_slots, min_ads_shown,
                                    #                                                   qs_exponent=qs_exponent, reserve_price=qwr_higher,
                                    #                                                   reserve_price_type='qwr',
                                    #                                                   minimum_bid_getter=minimum_bid_getter)
                                    #             hpa_auction = HpaAuction(position_effects, simple_auction_lower, simple_auction_higher)
                                    #             auctions[self._get_auction_name_string('hpa', 'qwr', '', str(qwr_lower), str(qwr_higher),
                                    #                                                    str(qs_exponent), '', '', '', str(min_bid_global),
                                    #                                                    str(min_bid_top), min_bid_type,
                                    #                                                    position_effect_type,
                                    #                                                    str(ncv_min_bid_pct_multiplier),
                                    #                                                    str(ncv_reserve_pct_multiplier))] = hpa_auction
                                    #     for uwr_lower in uwrs:
                                    #         for uwr_higher in uwrs:
                                    #             simple_auction_lower = SimpleAuction(position_effects, num_slots, min_ads_shown,
                                    #                                                  qs_exponent=qs_exponent, reserve_price=uwr_lower,
                                    #                                                  reserve_price_type='uwr',
                                    #                                                  minimum_bid_getter=minimum_bid_getter)
                                    #             simple_auction_higher = SimpleAuction(position_effects, num_slots, min_ads_shown,
                                    #                                                   qs_exponent=qs_exponent, reserve_price=uwr_higher,
                                    #                                                   reserve_price_type='uwr',
                                    #                                                   minimum_bid_getter=minimum_bid_getter)
                                    #             hpa_auction = HpaAuction(position_effects, simple_auction_lower, simple_auction_higher)
                                    #             auctions[self._get_auction_name_string('hpa', 'uwr', '', str(uwr_lower), str(uwr_higher),
                                    #                                                    str(qs_exponent), '', '', '', str(min_bid_global),
                                    #                                                    str(min_bid_top), min_bid_type,
                                    #                                                    position_effect_type,
                                    #                                                    str(ncv_min_bid_pct_multiplier),
                                    #                                                    str(ncv_reserve_pct_multiplier))] = hpa_auction
        print 'num_auctions: ', len(auctions)
        return auctions, minimum_bid_getter_clicks_top_n, minimum_bid_getter_ad_calls_top_n

    def run_simulation(self):
        max_ad_calls = self._simulation_config.num_ad_calls_to_simulate
        ad_call_number = 0
        top_clicks_number = 0
        top_ad_calls_number = 0
        num_discarded_ad_calls = 0
        start_time = time.time()
        self._simulation_config.logger.info('Simulating auction with %d ad calls...', max_ad_calls)

        net_conversion_value_model = NetConversionValueModel(self._file_locations)
        ctr_model = ClickThroughRateModel.from_file(self._file_locations)
        click_to_conversion_model = ClickToConversionModel(self._file_locations, self._simulation_config)
        advertisers = Advertisers(self._file_locations, self._simulation_config, ctr_model, net_conversion_value_model,
                                  click_to_conversion_model)
        publisher = Publisher()
        min_ads_shown = 2
        auctions, min_bid_getter_clicks, min_bid_getter_ad_calls = self._get_auctions(min_ads_shown)
        print 'number of auctions: ', len(auctions)
        all_auctions = AllAuctions(self._simulation_config, self._file_locations.base_path,
                                   advertisers, publisher,
                                   auctions=auctions)
        ad_call_getter = AdCallGetterFromGenerateSignals(advertisers, self._file_locations,
                                                         self._simulation_config, verbose=False)
        while True:
            # self._simulation_config.logger.info('\nProcessing ad call number %d', ad_call_number + 1)
            ad_call = ad_call_getter.get_next_ad_call()
            if ad_call.is_valid(min_ads_shown, self._simulation_config.ad_call_required_fields):
                # self._simulation_config.logger.debug('publisher_price: %.2f',
                #                                      ad_call.get_unnormalized_field_value('PUBLISHER_HOTEL_PRICE'))
                if ad_call is None or ad_call_number == max_ad_calls:
                    all_auctions.print_stats()
                    self._clean_up_prediction_files()
                    break
                else:
                    print 'processing ad call: ' + str(ad_call_number)
                    all_auctions.run_auctions(ad_call)
                    ad_call_number += 1
                    hotel_property_id = ad_call.get_field_value('HOTEL_PROPERTY_ID')
                    is_top_clicks_property = min_bid_getter_clicks.is_top_property(hotel_property_id, None, None, None)
                    if is_top_clicks_property:
                        top_clicks_number += 1
                    is_top_ad_calls_property = min_bid_getter_ad_calls.is_top_property(hotel_property_id, None, None, None)
                    if is_top_ad_calls_property:
                        top_ad_calls_number += 1
            else:
                num_discarded_ad_calls += 1
        self._simulation_config.logger.info('Auction simulation finished in {0:.2f} seconds, processed {1} ad calls '
                                            '({2} top 20pct clicks, {3} top 20pct ad_calls), discarded {4} ad calls'.
                                            format(time.time() - start_time, ad_call_number, top_clicks_number,
                                                   top_ad_calls_number, num_discarded_ad_calls))

    def _clean_up_prediction_files(self):
        for file in os.listdir(self._file_locations.ctr_model_working_dir):
            if 'prediction' in file:  # TODO: make this more robust
                os.unlink(self._file_locations.ctr_model_working_dir + file)


def run_file():
    ctr_model_directory_name = 'ctr_model_files'
    ncv_directory_name = 'ncv_model_files'
    price_sampler_directory_name = 'price_sampler_files'
    position_effects_directory_name = 'position_effects_files'
    logs_directory_name = 'logs'
    s3_log_data_date = '20141202'
    base_path = os.getenv('HOME') + '/Datasets/' + s3_log_data_date + '-generate-signals-ppa-price-a/training/'
    num_advertisers = 12
    if num_advertisers == 12:
        us_advertiser_ids = [148241, 148684, 148708, 149985, 150060, 155752, 157259, 165435,
                             148241, 148684, 148708, 149985]
        us_advertiser_names = ['Priceline', 'Expedia', 'Orbitz', 'Booking.com', 'Hotels.com', 'Cheaptickets',
                               'Travelocity', 'Getaroom', 'Priceline', 'Expedia', 'Orbitz', 'Booking.com']
        us_corresponding_ct_advertiser_ids = [59414, 59777, 61224, 133787, 122112, 61224, 60462, 59777,
                                              59414, 59777, 61224, 133787]
        us_corresponding_ct_advertiser_names = ['Priceline', 'Expedia', 'Orbitz', 'Booking.com', 'Hotels.com',
                                                'Orbitz', 'Travelocity', 'Expedia', 'Priceline', 'Expedia', 'Orbitz',
                                                'Booking.com']
        us_sources_some_properties_from_affiliate_networks = [False, False, False, True, False, False, False, True, False,
                                                           False, False, True]
    else:
        us_advertiser_ids = [148241, 148684, 148708, 149985, 150060, 155752, 157259, 165435]
        us_advertiser_names = ['Priceline', 'Expedia', 'Orbitz', 'Booking.com', 'Hotels.com', 'Cheaptickets',
                               'Travelocity', 'Getaroom']
        us_corresponding_ct_advertiser_ids = [59414, 59777, 61224, 133787, 122112, 61224, 60462, 59777]
        us_corresponding_ct_advertiser_names = ['Priceline', 'Expedia', 'Orbitz', 'Booking.com', 'Hotels.com',
                                              'Orbitz', 'Travelocity', 'Expedia']
        us_sources_some_properties_from_affiliate_networks = [False, False, False, True, False, False, False, True]


    uk_advertiser_ids = [163284, 163325, 163361, 163392, 165443, 180018]
    file_locations = FileLocations(base_path=base_path,
                                   column_names_file='normalized_sampled.gz/.column_names',
                                   input_data_frame_file='normalized_sampled.gz/part-all',
                                   input_ctr_training_file_path='normalized_vw.gz/part-all.gz',
                                   input_simulation_file_path='normalized_vw_one_per.gz/part-all.gz',
                                   ctr_model_working_dir='normalized_vw.gz/',
                                   ctr_training_data_file_name='normalized_vw.gz/part-all-model-train.gz',
                                   ctr_model_file_name=ctr_model_directory_name + '/ctr_model.pkl',
                                   simulation_data_file_name='normalized_vw_one_per.gz/part-all-simulation.gz',
                                   ncv_model_file_name=ncv_directory_name + '/ncv_model.pkl',
                                   ncv_data_file_name='net_conversion_values_with_property_attributes.csv',
                                   grouped_advertiser_property_ids_file='grouped_advertiser_property_ids.csv',
                                   average_effective_bids_file='avg_effective_bids.csv',
                                   normalization_factors_file='variance/part-all',
                                   hotel_property_ids_with_advertiser_ids_file=
                                   'hotel_property_ids_with_advertiser_ids.csv',
                                   unique_advertiser_lists_file='unique_advertiser_lists.csv',
                                   property_ids_to_unique_advertiser_list_ids_file=
                                   'property_ids_to_unique_advertiser_list_ids.csv',
                                   hotel_property_ids_with_star_brand_market_file=
                                   'hotel_property_ids_with_star_brand_market.csv',
                                   hotel_property_ids_by_ad_calls_file=
                                   'hotel_property_ids_by_ad_calls.csv',
                                   hotel_property_ids_by_clicks_file=
                                   'hotel_property_ids_by_clicks.csv',
                                   roi_by_wlt_file='roi_by_wlt.csv',
                                   advertiser_price_wlt_corr_file='advertiser_price_wlt_corr.csv',
                                   advertiser_price_marginals_file='advertiser_price_wlt_marginals.csv',
                                   price_sampler_file=price_sampler_directory_name + '/price_sampler.pkl',
                                   ctr_position_parity_file='ctr_position_parity.csv',
                                   position_effects_file=position_effects_directory_name + '/position_effects.pkl',
                                   ctc_overall_avg_file='ctc_overall_avg.csv',
                                   ctc_rate_where_available_file='ctc_rate_where_available.csv',
                                   result_logs_file=logs_directory_name + '/results.csv')
    file_locations.create_model_directories(
        [ncv_directory_name, price_sampler_directory_name, position_effects_directory_name,
         logs_directory_name, ctr_model_directory_name])
    ctr_column_names_not_categorical = ['BROWSER_OTHER', 'DEVICE_TYPE_PHONE', 'IS_RANK_TIED',
                                        'PRICE_IS_EQUAL_TO_PUB_PRICE', 'PRICE_IS_GREATER_THAN_PUB_PRICE',
                                        'PRICE_IS_LESS_THAN_PUB_PRICE', 'RANK_IN_PAGE']
    ctr_column_names_categorical = ['ADVERTISER_ID', 'AD_UNIT_ID', 'CAMPAIGN_ID', 'PUBLISHER_ID', 'SITE_ID']
    simulation_config = SimulationConfig(
        base_path=base_path + logs_directory_name,
        num_ad_calls_to_simulate=100,
        ncv_model=sklearn.tree.DecisionTreeRegressor(),  # Significantly faster, Score ~0.2
        # ncv_model=sklearn.ensemble.RandomForestRegressor(n_estimators=10, n_jobs=7), #  Score ~0.3
        non_click_sample_rate_decimal=0.001,
        discrete_corr_num_samples_per_iteration=100000,
        discrete_corr_epsilon=0.03,
        discrete_corr_max_iterations=10,
        assumed_wlt_corr_value_when_missing=0.35,  # typical out of cartel corr from our analysis
        ad_call_required_fields=['PUBLISHER_HOTEL_PRICE'],
        us_advertiser_names=us_advertiser_names,
        uk_advertiser_names=['Priceline', 'Hotels.com', 'Expedia', 'Booking.com', 'Getaroom', 'EBookers'],
        us_advertiser_ids=us_advertiser_ids,
        uk_advertiser_ids=uk_advertiser_ids,
        us_corresponding_ct_advertiser_names=us_corresponding_ct_advertiser_names,
        uk_corresponding_ct_advertiser_names=['Priceline', 'Hotels.com', 'Expedia', 'Booking.com', 'Expedia',
                                              'EBookers'],
        us_corresponding_ct_advertiser_ids=us_corresponding_ct_advertiser_ids,
        uk_corresponding_ct_advertiser_ids=[166060, 153784, 157288, 150568, 157288, 152665],
        us_sources_some_properties_from_affiliate_networks=us_sources_some_properties_from_affiliate_networks,
        uk_sources_some_properties_from_affiliate_networks=[False, False, False, True, False, True],
        site_adv_blacklists={2: [148708] + uk_advertiser_ids,
                             3: [155752] + uk_advertiser_ids,
                             4: [] + us_advertiser_ids,
                             33: [] + us_advertiser_ids,
                             37: [] + us_advertiser_ids},
        ctr_column_names_not_categorical=ctr_column_names_not_categorical,
        ctr_column_names_categorical=ctr_column_names_categorical,
        verbose=True
    )
    flat_file_generator = FlatFileGenerator(file_locations, simulation_config,
                                            feature_names_to_keep=ctr_column_names_categorical +
                                                                  ctr_column_names_not_categorical,
                                            feature_names_set_to_one_in_simulation=['AUCTION_POSITION',
                                                                                    'CELL_ID',
                                                                                    'POSITION'],
                                            overwrite_existing_files=False)
    flat_file_generator.generate_flat_files()
    simulation_runner = SimulationRunner(file_locations, simulation_config)
    simulation_runner.run_simulation()


if __name__ == '__main__':
    run_file()
