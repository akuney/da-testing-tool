REGISTER                    's3n://intentmedia-hadoop-production/jars/intentmedia.jar';
REGISTER                    's3n://intentmedia-hadoop-production/jars/udfs.py' using streaming_python;
IMPORT                      's3n://intentmedia-hadoop-production/macros.pig';

SET default_parallel 55;



%default ADVERTISEMENT_TYPE       'SsrAdvertisement';

%default CL_START_DATE         '20131230';
%default CL_END_DATE           '20140105';
%default PRODUCT_CATEGORY   'HOTELS';

%declare INPUT_PATH         's3n://intentmedia-hadoop-production/input/';
%default STORE_PATH         's3n://intentmedia-hawk-output/Eric_Abis/Revenue_For_Audit/SSN_Two_Days_Test_9';

--Revenue_For_Audit

%declare REDUCERS 100;


-------------CLICKS
clicks_raw = LOAD '$INPUT_PATH' USING com.intentmedia.pig.TypedDataJsonLoader('com.intentmedia.data.ClickData', '$CL_START_DATE', '$CL_END_DATE', '', '');

clicks_filtered = FILTER clicks_raw BY 

			ad_call_requested_at >= (long) org.apache.pig.piggybank.evaluation.datetime.convert.ISOToUnix(org.apache.pig.piggybank.evaluation.datetime.convert.CustomFormatToISO('2013-12-30 05:00:00','yyyy-MM-dd HH:mm:ss')) AND
			ad_call_requested_at < (long) org.apache.pig.piggybank.evaluation.datetime.convert.ISOToUnix(org.apache.pig.piggybank.evaluation.datetime.convert.CustomFormatToISO('2014-01-03 05:00:00','yyyy-MM-dd HH:mm:ss')) AND
            ip_address_blacklisted == 0 ;


clicks = FOREACH clicks_filtered GENERATE   org.apache.pig.piggybank.evaluation.datetime.truncate.ISOToDay(org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO(ad_call_requested_at)) as ad_call_date,
                        					org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO(ad_call_requested_at) as ad_call_date_With_Hour, 
                        					org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO(requested_at) as click_date_with_Hour, 
										   actual_cpc, advertisement_id, site_type,  fraudulent;


adlookup = LOAD 's3n://intentmedia-hadoop-production/input/Advertisement_IDs.csv' USING PigStorage(',') AS (advertisement_ID:int, advertisement_type:chararray, advertiser_id:int);

--adlookupfiltered = FILTER adlookup BY 
--            advertisement_type == '$ADVERTISEMENT_TYPE';

JOINED = JOIN clicks by advertisement_id, adlookup BY advertisement_ID;




---Does not work because we need to get down to hour, would work if created a new StartOfDayinMillisWithHour function using from472d2m2h2s function fround in codebase
--ad_call_requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('20131230') AND ad_call_requested_at < com.intentmedia.pig.udf.date.EndOfDayInMillis('20140102'); 


--GROUPED = GROUP JOINED_FILTERED BY ad_call_date;
--SUMMED = FOREACH GROUPED GENERATE SUM(JOINED_FILTERED.clicks::actual_cpc) as revenue, JOINED_FILTERED.clicks.ad_call_date;
--STORE SUMMED INTO 's3n://intentmedia-hawk-output/Eric_Abis/Revenue_For_Audit/test8' USING PigStorage('\t');


ANSWER = FOREACH JOINED GENERATE actual_cpc, ad_call_date_With_Hour, click_date_with_Hour, advertiser_id, clicks::advertisement_id, site_type, adlookup::advertisement_type, fraudulent,
org.apache.pig.piggybank.evaluation.datetime.convert.CustomFormatToISO('2014-01-03 05:00:00','yyyy-MM-dd HH:mm:ss') as testisodate;

STORE ANSWER INTO '$STORE_PATH' USING PigStorage('\t');
