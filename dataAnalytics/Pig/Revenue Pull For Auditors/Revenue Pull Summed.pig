REGISTER                    's3n://intentmedia-hadoop-production/jars/intentmedia.jar';
REGISTER                    's3n://intentmedia-hadoop-production/jars/udfs.py' using streaming_python;
IMPORT                      's3n://intentmedia-hadoop-production/macros.pig';

SET default_parallel 55;



%default ADVERTISEMENT_TYPE       'SsrAdvertisement';

%default CL_START_DATE         '20130101';
%default CL_END_DATE           '20140215';
%default PRODUCT_CATEGORY   'HOTELS';

%declare INPUT_PATH         's3n://intentmedia-hadoop-production/input/';
%default STORE_PATH         's3n://intentmedia-hawk-output/Eric_Abis/Revenue_For_Audit/test1';

--Revenue_For_Audit

%declare REDUCERS 100;


-------------CLICKS
clicks_raw = LOAD '$INPUT_PATH' USING com.intentmedia.pig.TypedDataJsonLoader('com.intentmedia.data.ClickData', '$CL_START_DATE', '$CL_END_DATE', '', '$PRODUCT_CATEGORY');

--AddDuration('$END_DATE', 'P5D')

clicks_filtered = FILTER clicks_raw BY 
            fraudulent == 0 AND
            ip_address_blacklisted == 0;

clicks = FOREACH clicks_filtered GENERATE org.apache.pig.piggybank.evaluation.datetime.truncate.ISOToDay(org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO(ad_call_requested_at)) as ad_call_date, ad_call_requested_at, actual_cpc, advertisement_id, site_type;


adlookup = LOAD 's3n://intentmedia-hadoop-production/input/Advertisement_IDs.csv' USING PigStorage(',') AS (advertisement_ID:int, advertisement_type:chararray, advertiser_id:int);

adlookupfiltered = FILTER adlookup BY 
            advertisement_type == '$ADVERTISEMENT_TYPE';

JOINED = JOIN clicks by advertisement_id, adlookupfiltered BY advertisement_ID;

JOINED_FILTERED = FILTER JOINED BY 
(ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20130101') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20130131') AND site_type == 'ORBITZ_GLOBAL' AND  advertiser_id == 71265 ) OR 
(ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20130101') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20130131') AND site_type == 'TRAVELOCITY' AND  advertiser_id == 35273 );

--FILTERED_DATA = FOREACH JOINED_FILTERED GENERATE actual_cpc, ad_call_date, advertiser_id, clicks::advertisement_id, site_type, adlookupfiltered::advertisement_type;

GROUPED = GROUP JOINED_FILTERED BY adlookupfiltered::advertiser_id;

STORE GROUPED INTO 's3n://intentmedia-hawk-output/Eric_Abis/Revenue_For_Audit/test2' USING PigStorage('\t');

--FLATTENED = FOREACH GROUPED GENERATE FLATTENED(group) as adlookupfiltered::advertiser_id;

SUMMED = FOREACH GROUPED GENERATE SUM(JOINED_FILTERED.clicks::actual_cpc) as revenue;

STORE SUMMED INTO '$STORE_PATH' USING PigStorage('\t');
