REGISTER                    's3n://intentmedia-hadoop-production/jars/intentmedia.jar';
IMPORT                      's3n://intentmedia-hadoop-production/jars/scripts/pig/macros.pig';

SET default_parallel 100;

%default CL_START_DATE         '20140120';
%default CL_END_DATE           '20140120';
%default PRODUCT_CATEGORY   'HOTELS';

%declare INPUT_PATH         's3n://intentmedia-hadoop-production/input/';
%default STORE_PATH         's3n://intentmedia-hawk-output/Eric_Abis/competition_report/test26/GROUPED';
%default STORE_PATH2         's3n://intentmedia-hawk-output/Eric_Abis/competition_report/test26/PUB'

%declare REDUCERS 100;

------------Ad Calls
ad_calls_raw = LOAD '$INPUT_PATH' USING com.intentmedia.pig.TypedDataJsonLoader('com.intentmedia.data.AdCallData', '$CL_START_DATE', '$CL_END_DATE', '', '$PRODUCT_CATEGORY');

ad_calls = FOREACH (FILTER ad_calls_raw BY ip_address_blacklisted == 0 AND outcome_type == 'SERVED' AND ad_unit_type == 'META')
    GENERATE request_id, ad_unit_id, publisher_id, site_currency, positions_filled, number_of_advertisers_in_auction, publisher_hotel_price as publisher_price, ip_address_blacklisted, ad_unit_type,
            requested_at, org.apache.pig.piggybank.evaluation.datetime.truncate.ISOToDay(org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO(requested_at)) as ad_call_date, 
            CASE WHEN rank_in_page <= 5 THEN 'TOP 5' ELSE 'NOT TOP 5' END as rank_in_page;

--PUB =     FOREACH ad_calls GENERATE request_id, ad_unit_id, publisher_id, site_currency, publisher_price, ad_unit_type, requested_at, ad_call_date, rank_in_page, publisher_price as advertiser_price, publisher_id as advertiser_id, 0 as revenue, 0 as clicks, 1 as is_publisher;

---- Impressions
impressions_raw = LOAD '$INPUT_PATH' USING com.intentmedia.pig.TypedDataJsonLoader('com.intentmedia.data.ImpressionData', '$CL_START_DATE', '$CL_END_DATE', '', '$PRODUCT_CATEGORY');

impressions = FOREACH impressions_raw GENERATE external_id, request_id, effective_bid, hotel_average_nightly_rate as advertiser_price, auction_position, advertiser_id, base_bid;

-------------CLICKS
clicks_raw = LOAD '$INPUT_PATH' USING com.intentmedia.pig.TypedDataJsonLoader('com.intentmedia.data.ClickData', '$CL_START_DATE', '$CL_END_DATE', '', '$PRODUCT_CATEGORY');

clicks = FOREACH (FILTER clicks_raw BY fraudulent == 0 AND ip_address_blacklisted == 0) GENERATE  actual_cpc, external_impression_id;

clicks_grouped = FOREACH (GROUP clicks by external_impression_id) GENERATE flatten(group) as external_impression_id, SUM(clicks.actual_cpc) as revenue,
(int)COUNT(clicks.actual_cpc) as clicks;

--- JOIN 1
imp_clicks = JOIN impressions BY external_id LEFT OUTER, clicks_grouped BY external_impression_id;

imp_clicks_grouped = GROUP imp_clicks BY request_id;

JOINED = JOIN ad_calls BY request_id, imp_clicks_grouped BY group;

STORE JOINED INTO '$STORE_PATH' USING PigStorage('\t');