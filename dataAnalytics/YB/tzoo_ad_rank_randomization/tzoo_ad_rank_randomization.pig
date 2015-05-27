-- Test Informaion
-- ad_unit_id = 161
-- Test was activated at:   '2014-06-14 13:15:26' 
-- Test was deactivated at: '2014-06-15 02:32:52'

REGISTER                    's3n://intentmedia-hadoop-production/jars/intentmedia.jar';
IMPORT                      's3n://intentmedia-hadoop-production/jars/scripts/pig/macros.pig';

SET default_parallel 55;

%default PUBLISHER              'TRAVELZOO';
%default SITE                   'TRAVELZOO';
%default PRODUCT_CATEGORY       'FLIGHTS';

%declare INPUT_PATH             's3n://intentmedia-hadoop-production/input/';
%default STORE_PATH             's3n://intentmedia-hawk-output/yoojong_bang/tzoo_randomization';

DEFINE MapToBagOfTuples com.intentmedia.pig.udf.util.MapToBagOfTuples();

%declare REDUCERS 100;

-- load raw data
ad_calls_raw = LOAD '$INPUT_PATH' USING com.intentmedia.pig.TypedDataJsonLoader('com.intentmedia.data.AdCallData', '20140614', '20140615', '$PUBLISHER', '$PRODUCT_CATEGORY');
impressions_raw = LOAD '$INPUT_PATH' USING com.intentmedia.pig.TypedDataJsonLoader('com.intentmedia.data.ImpressionData', '20140614', '20140615', '$PUBLISHER', '$PRODUCT_CATEGORY');
clicks_raw = LOAD '$INPUT_PATH' USING com.intentmedia.pig.TypedDataJsonLoader('com.intentmedia.data.ClickData', '20140614', '20140616', '$PUBLISHER', '$PRODUCT_CATEGORY');
conversions_raw = LOAD '$INPUT_PATH' USING com.intentmedia.pig.TypedDataJsonLoader('com.intentmedia.data.ConversionData', '20140614', '20140715', '', '');

-- filter data [ad calls]
ad_calls_filtered = FILTER ad_calls_raw BY 
                    ip_address_blacklisted == 0 AND
                    outcome_type == 'SERVED' AND
                    site_type == '$SITE' AND
                    ad_unit_type == 'CT' AND 
                    ad_unit_id == 161 AND
                    requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20140614') AND 
                    requested_at <= com.intentmedia.pig.udf.date.EndOfDayInMillis('20140615');

-- get ad calls
ad_calls = FOREACH ad_calls_filtered GENERATE publisher_user_id, webuser_id, request_id, toEST(requested_at), requested_at;

-- filter data [impressions]
impressions_filtered = FILTER impressions_raw BY
                       ip_address_blacklisted == 0 AND
                       ad_unit_id == 161 AND
                       (advertiser_id == 59528 OR advertiser_id == 59777 OR advertiser_id == 61224 OR advertiser_id == 87697 OR advertiser_id == 106574) AND
                       requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20140614') AND
                       requested_at <= com.intentmedia.pig.udf.date.EndOfDayInMillis('20140615');

-- get impressions
impressions = FOREACH impressions_filtered GENERATE request_id, external_id, advertiser_id, auction_position;

-- join ad calls and impressions
joined_ac_i = JOIN ad_calls BY request_id, impressions BY request_id;

ac_i = FOREACH joined_ac_i GENERATE
       ad_calls::request_id AS ac_request_id,
       ad_calls::publisher_user_id AS ac_publisher_user_id,
       ad_calls::webuser_id AS ac_webuser_id,
       ad_calls::requested_at_date_in_et AS ac_date,
       ad_calls::requested_at AS ac_timestamp,
       impressions::external_id AS i_external_id,
       impressions::advertiser_id AS i_advertiser_id,
       impressions::auction_position AS i_auction_position;

-- filter data [clicks]
clicks_filtered = FILTER clicks_raw BY
                  ip_address_blacklisted == 0 AND
                  fraudulent == 0 AND
                  site_type == '$SITE' AND
                  ad_call_request_id IS NOT NULL AND
                  requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20140614') AND 
                  requested_at <= com.intentmedia.pig.udf.date.EndOfDayInMillis('20140616');

-- get clicks
clicks = FOREACH clicks_filtered GENERATE ad_call_request_id, external_impression_id, request_id, webuser_id, actual_cpc, toEST(requested_at), requested_at;

-- join ad_calls, impressions, and clicks
joined_ac_i_c = JOIN ac_i BY i_external_id, clicks BY external_impression_id;

ac_i_c = FOREACH joined_ac_i_c GENERATE
         ac_i::ac_request_id,
         ac_i::ac_publisher_user_id,
         ac_i::ac_webuser_id,
         ac_i::ac_date,
         ac_i::ac_timestamp,
         ac_i::i_advertiser_id,
         ac_i::i_auction_position,
         clicks::request_id AS c_request_id,
         clicks::actual_cpc AS c_actual_cpc,
         clicks::requested_at_date_in_et AS c_date,
         clicks::requested_at AS c_timestamp;

-- for conversions, we would have 2 subsets, one with non-null order_id and another with null order_id
-- filter data [conversions]
conversions_filtered_1 = FILTER conversions_raw BY
                         ip_address_blacklisted == 0 AND
                         (entity_id == 59528 OR entity_id == 59777 OR entity_id == 61224 OR entity_id == 87697 OR entity_id == 106574) AND
                         requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20140614') AND
                         requested_at <= com.intentmedia.pig.udf.date.EndOfDayInMillis('20140715') AND
                         order_id IS NOT NULL;

conversions_filtered_2 = FILTER conversions_raw BY
                         ip_address_blacklisted == 0 AND
                         (entity_id == 59528 OR entity_id == 59777 OR entity_id == 61224 OR entity_id == 87697 OR entity_id == 106574) AND
                         requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20140614') AND
                         requested_at <= com.intentmedia.pig.udf.date.EndOfDayInMillis('20140715') AND
                         order_id IS NULL;

-- get conversions
conversions_1 = FOREACH conversions_filtered_1 GENERATE request_id, entity_id, order_id, webuser_id, conversion_value, net_conversion_value, toEST(requested_at), requested_at;
conversions_2 = FOREACH conversions_filtered_2 GENERATE request_id, entity_id, order_id, webuser_id, conversion_value, net_conversion_value, toEST(requested_at), requested_at;

-- dedup conversions
conversions_grouped_1 = GROUP conversions_1 BY (entity_id, order_id);
conversions_sorted_1 = FOREACH conversions_grouped_1 GENERATE FLATTEN(group), MIN(conversions_1.requested_at) AS min_requested_at;
conversions_joined_1 = JOIN conversions_1 BY (entity_id, order_id, requested_at), conversions_sorted_1 BY (entity_id, order_id, min_requested_at);
conversions_deduped_1 = FOREACH conversions_joined_1 GENERATE
                        conversions_1::request_id,
                        conversions_1::entity_id,
                        conversions_1::order_id,
                        conversions_1::webuser_id,
                        conversions_1::conversion_value,
                        conversions_1::net_conversion_value,
                        conversions_1::requested_at_date_in_et,
                        conversions_1::requested_at;
conversions_deduped = UNION conversions_deduped_1, conversions_2;

---- join all data
--joined_all = COGROUP ad_calls BY webuser_id, clicks BY webuser_id, impressions BY webuser_id, conversions_deduped BY webuser_id;
--joined_all_sorted = FOREACH joined_all {
--    ad_call_sorted = ORDER ad_calls BY requested_at;
--    clicks_sorted = ORDER clicks BY requested_at;
--    impressions_sorted = ORDER impressions BY requested_at;
--    conversions_deduped_sorted = ORDER conversions_deduped BY requested_at;
--    GENERATE ad_call_sorted, clicks_sorted, impressions_sorted, conversions_deduped_sorted;
--};
--joined_all_grouped = GROUP joined_all_sorted ALL;

joined_all = GROUP ac_i_c BY (ac_i::ac_webuser_id, ac_i::i_advertiser_id), conversions_deduped BY (webuser_id, entity_id);
joined_all_sorted = FOREACH joined_all {
    ac_i_c_sorted = ORDER ac_i_c BY c_timestamp;
    conversions_deduped_sorted = ORDER conversions_deduped BY requested_at;
    GENERATE ac_i_c_sorted, conversions_deduped_sorted;
};
joined_all_grouped = GROUP joined_all_sorted ALL;
out = FOREACH joined_all_grouped GENERATE group;

STORE out INTO '$STORE_PATH' USING PigStorage('\t');