REGISTER                    's3n://intentmedia-hadoop-production/jars/intentmedia.jar';
REGISTER                    's3n://intentmedia-hadoop-production/jars/udfs.py' using streaming_python;
IMPORT                      's3n://intentmedia-hadoop-production/macros.pig';

SET default_parallel 100;



%default ADVERTISEMENT_TYPE  'SsrAdvertisement';
%default CL_START_DATE       '20130101';
%default CL_END_DATE         '20140215';
%default PRODUCT_CATEGORY    'HOTELS';

%declare INPUT_PATH         's3n://intentmedia-hadoop-production/input/';
%default STORE_PATH         's3n://intentmedia-hawk-output/Eric_Abis/Revenue_For_Audit/SSN_BID';

--Revenue_For_Audit

%declare REDUCERS 100;


-------------CLICKS
clicks_raw = LOAD '$INPUT_PATH' USING com.intentmedia.pig.TypedDataJsonLoader('com.intentmedia.data.ClickData', '$CL_START_DATE', '$CL_END_DATE', '', '$PRODUCT_CATEGORY');

--AddDuration('$END_DATE', 'P5D')

clicks_filtered = FILTER clicks_raw BY fraudulent == 0 AND ip_address_blacklisted == 0;

clicks = FOREACH clicks_filtered GENERATE org.apache.pig.piggybank.evaluation.datetime.truncate.ISOToDay(org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO(ad_call_requested_at)) as ad_call_date, 
	ad_call_requested_at, actual_cpc, advertisement_id, site_type, external_impression_id;

impressions = LOAD '$INPUT_PATH' USING com.intentmedia.pig.TypedDataJsonLoader('com.intentmedia.data.ImpressionData', '$CL_START_DATE', '$CL_END_DATE', '', '$PRODUCT_CATEGORY');

impressions_filtered = FOREACH impressions GENERATE external_id, effective_bid;

FIRST_JOINED = JOIN clicks by external_impression_id, impressions_filtered by external_id;

adlookup = LOAD 's3n://intentmedia-hadoop-production/input/Advertisement_IDs.csv' USING PigStorage(',') AS (advertisement_id:int, advertisement_type:chararray, advertiser_id:int);

adlookupfiltered = FILTER adlookup BY advertisement_type == '$ADVERTISEMENT_TYPE';

JOINED = JOIN FIRST_JOINED by advertisement_id, adlookupfiltered BY advertisement_id;

JOINED_FILTERED = FILTER JOINED BY 
(ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20130101') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20130131') AND  advertiser_id == 71265 ) OR 
(ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20130101') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20130131') AND  advertiser_id == 35273 ) OR 
(ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20130901') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20130930') AND  advertiser_id == 5877 ) OR 
(ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20130201') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20130228') AND  advertiser_id == 98349 ) OR 
(ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20130901') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20130930') AND  advertiser_id == 26080 ) OR 
(ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20130901') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20130930') AND  advertiser_id == 121269 ) OR 
(ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20130201') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20130228') AND  advertiser_id == 8311 ) OR 
(ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20130901') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20130930') AND  advertiser_id == 60680 ) OR 
(ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20130901') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20130930') AND  advertiser_id == 59233 ) OR 
(ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20131001') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20131031') AND  advertiser_id == 86787 ) OR 
(ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20131001') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20131031') AND  advertiser_id == 136940 ) OR 
(ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20131001') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20131031') AND  advertiser_id == 39974 ) OR 
(ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20131001') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20131031') AND  advertiser_id == 57641 ) OR 
(ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20131001') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20131031') AND  advertiser_id == 8928 ) OR 
(ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20131001') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20131031') AND  advertiser_id == 116010 ) OR 
(ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20131001') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20131031') AND  advertiser_id == 118170 ) OR 
(ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20130301') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20130331') AND  advertiser_id == 71265 ) OR 
(ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20130301') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20130331') AND  advertiser_id == 52934 ) OR 
(ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20131101') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20131130') AND  advertiser_id == 143001 ) OR 
(ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20131101') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20131130') AND  advertiser_id == 23463 ) OR 
(ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20131101') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20131130') AND  advertiser_id == 86295 ) OR 
(ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20131101') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20131130') AND  advertiser_id == 108925 ) OR 
(ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20131101') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20131130') AND  advertiser_id == 100869 ) OR 
(ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20131101') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20131130') AND  advertiser_id == 64707 ) OR 
(ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20131101') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20131130') AND  advertiser_id == 58307 ) OR 
(ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20131201') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20131231') AND  advertiser_id == 23163 ) OR 
(ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20131201') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20131231') AND  advertiser_id == 146887 ) OR 
(ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20131201') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20131231') AND  advertiser_id == 99667 ) OR 
(ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20131201') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20131231') AND  advertiser_id == 85466 ) OR 
(ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20131201') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20131231') AND  advertiser_id == 63882 ) OR 
(ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20131201') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20131231') AND  advertiser_id == 88264 ) OR 
(ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20130601') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20130630') AND  advertiser_id == 30255 ) OR 
(ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20130601') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20130630') AND  advertiser_id == 60589 ) OR 
(ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20130701') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20130731') AND  advertiser_id == 15773 ) OR 
(ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20130701') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20130731') AND  advertiser_id == 39686 ) OR 
(ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20130401') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20130430') AND  advertiser_id == 93163 ) OR 
(ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20130401') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20130430') AND  advertiser_id == 118065 ) OR 
(ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20130501') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20130531') AND  advertiser_id == 117983 ) OR 
(ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20130501') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20130531') AND  advertiser_id == 58307 ) OR 
(ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20130801') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20130831') AND  advertiser_id == 134790 ) OR 
(ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20130801') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20130831') AND  advertiser_id == 57641 ) ;

ANSWER = FOREACH JOINED_FILTERED GENERATE actual_cpc, ad_call_date, advertiser_id, clicks::advertisement_id, site_type, adlookupfiltered::advertisement_type, effective_bid;

STORE ANSWER INTO '$STORE_PATH' USING PigStorage('\t');
