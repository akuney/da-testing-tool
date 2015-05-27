REGISTER                    's3n://intentmedia-hadoop-production/jars/intentmedia.jar';
IMPORT                      's3n://intentmedia-hadoop-production/jars/scripts/pig/macros.pig';

SET default_parallel 55;

%default PRODUCT_CATEGORY       'FLIGHTS';
%default START_DATE             '20150108';
%default END_DATE               '20150114';


%declare INPUT_PATH             's3n://intentmedia-hadoop-production/input/';
%default STORE_PATH             's3n://intentmedia-hawk-output/yoojong_bang/ad_call_user_profile';

%declare REDUCERS 100;

ad_calls_raw = LOAD '$INPUT_PATH' USING com.intentmedia.pig.TypedDataJsonLoader('com.intentmedia.data.AdCallData', '$START_DATE', '$END_DATE', '', '$PRODUCT_CATEGORY');

ad_calls_filtered = FILTER ad_calls_raw BY ip_address_blacklisted == 0 AND ad_unit_type == 'CT';

ad_calls = FOREACH ad_calls_filtered GENERATE publisher_id, request_id, org.apache.pig.piggybank.evaluation.datetime.truncate.ISOToDay(org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO(requested_at)) as ad_call_date, requested_at, device_family, toJSON(user_profile) as device_history;

ad_calls_grouped = GROUP ad_calls BY (ad_call_date, publisher_id, device_family, device_history);

out = FOREACH ad_calls_grouped GENERATE FLATTEN(group) AS (ad_call_date, publisher_id, device_family, device_history), COUNT(ad_calls.request_id) AS ad_calls;

STORE out INTO '$STORE_PATH' USING PigStorage('\t');
