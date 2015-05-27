REGISTER                    's3n://intentmedia-hadoop-production/jars/intentmedia.jar';
IMPORT                      's3n://intentmedia-hadoop-production/jars/scripts/pig/macros.pig';

SET default_parallel 55;

%default PUBLISHER              'TRAVELOCITY_ON_EXPEDIA';
%default PRODUCT_CATEGORY       'HOTELS';
%default START_DATE             '20141222';
%default END_DATE               '20150209';

%declare INPUT_PATH             's3n://intentmedia-hadoop-production/input/';
%default STORE_PATH             's3n://intentmedia-hawk-output/yoojong_bang/tvly_hotel_mapping_test';

%declare REDUCERS 100;

-- load data
ad_calls_raw = LOAD '$INPUT_PATH' USING com.intentmedia.pig.TypedDataJsonLoader('com.intentmedia.data.AdCallData', '$START_DATE', '$END_DATE', '$PUBLISHER', '$PRODUCT_CATEGORY');
request_statistics_raw = LOAD '$INPUT_PATH' USING com.intentmedia.pig.TypedDataJsonLoader('com.intentmedia.data.RequestStatisticsData', '$START_DATE', '$END_DATE', '', '');

-- process ad call data
ad_calls_filtered = FILTER ad_calls_raw BY ip_address_blacklisted == 0 AND ad_unit_type == 'CT' AND site_id == 29 AND multivariate_version_id >= 2124;
ad_calls = FOREACH ad_calls_filtered GENERATE request_id, toEST(requested_at), extractMVTVariant(multivariate_test_attributes_variable);

-- process request statistic data
request_statistics = FOREACH request_statistics_raw GENERATE request_id, processing_time;

-- join two datasource by request_id
joined_data = JOIN ad_calls BY request_id, request_statistics BY request_id;

-- group by the joined datasource by date and mvt bucket
grouped = GROUP joined_data BY (requested_at_date_in_et, mvt_variant);

-- compute average ad call response time for each mvt bucket
out = FOREACH grouped GENERATE FLATTEN(group) AS (requested_at_date_in_et, mvt_variant), AVG(joined_data.processing_time) AS avg_ad_call_response_time;

-- outputs the result
STORE out INTO '$STORE_PATH' USING PigStorage('\t');
