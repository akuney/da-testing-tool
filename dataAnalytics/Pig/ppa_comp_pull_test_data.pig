REGISTER                    's3n://intentmedia-hadoop-production/jars/intentmedia.jar';
IMPORT                      's3n://intentmedia-hadoop-production/jars/scripts/pig/macros.pig';

SET default_parallel 100;

%default CL_START_DATE         '20131230';
%default CL_END_DATE           '20131230';
%default PRODUCT_CATEGORY   'HOTELS';

%declare INPUT_PATH         's3n://intentmedia-hadoop-production/input/';
%default STORE_PATH         's3n://intentmedia-hawk-output/Eric_Abis/competition_report/test29/tup';

%declare REDUCERS 100;

------------Ad Calls
JOINED = LOAD 's3n://intentmedia-hadoop-production/input/total.csv' USING PigStorage(',') AS (
request_id:chararray,  ad_unit_id:int, publisher_id:int, site_currency:chararray, positions_filled:int,	number_of_advertisers_in_auction:int, publisher_price:double, ip_address_blacklisted:int, ad_unit_type:chararray,	requested_at:long, ad_call_date:chararray, rank_in_page:chararray, req2:chararray,
B: bag {T: tuple(external_id:chararray, request_id:chararray, advertiser_price:double, advertiser_id:int, external_impression_id:chararray, revenue:double, clicks:int)});

RESULT = FOREACH JOINED GENERATE request_id, ad_unit_id, publisher_id, site_currency, positions_filled,    number_of_advertisers_in_auction, publisher_price, ip_address_blacklisted, ad_unit_type, requested_at,  ad_call_date, rank_in_page, processBag(publisher_id, publisher_price, B);

STORE RESULT INTO '$STORE_PATH' USING PigStorage('\t');