/* 2013 Analysis Version */

REGISTER                    's3n://intentmedia-hadoop-production/jars/intentmedia.jar';
REGISTER                    's3n://intentmedia-hadoop-production/jars/udfs.py' using streaming_python;
IMPORT                      's3n://intentmedia-hadoop-production/jars/macros.pig';

%default START_DATE         '20130101';
%default END_DATE           '20131231';

%default BLOCK_CSV_PATH     's3n://intentmedia-hawk-output/yoojong_bang/files/GeoLiteCity-Blocks.csv/';
%default LOCATION_CSV_PATH  's3n://intentmedia-hawk-output/yoojong_bang/files/GeoLiteCity-Locations.csv/';
%declare INPUT_PATH         's3n://intentmedia-hadoop-production/input/';
%default STORE_PATH         's3n://intentmedia-hawk-output/yoojong_bang/click_value_per_geo/';

%declare REDUCERS 100;
set default_parallel 55;

blocks = LOAD '$BLOCK_CSV_PATH' USING PigStorage(',') AS (startIpNum:int, endIpNum:int, locId:int);
locations = LOAD '$LOCATION_CSV_PATH' USING PigStorage(',') AS (locId:int, country:chararray, region:chararray, city:chararray, postalCode:chararray, latitude:chararray, longitude:chararray, metrocode:int, areacode:int);
location_trimmed = FOREACH locations GENERATE locId, country, region;

clicks_raw = LOAD '$INPUT_PATH' USING com.intentmedia.pig.TypedDataJsonLoader('com.intentmedia.data.ClickData', '$START_DATE', '$END_DATE', '', '');

clicks_filtered = FILTER clicks_raw BY
                    ip_address_blacklisted == 0 AND
                    fraudulent == 0 AND
                    ad_call_request_id IS NOT NULL AND
                    requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('$START_DATE') AND requested_at <= com.intentmedia.pig.udf.date.EndOfDayInMillis('$END_DATE');

clicks_tuple = FOREACH clicks_filtered GENERATE TOTUPLE(*) AS click;

clicks_selected = FOREACH clicks_tuple GENERATE click.request_id AS request_id, click.ip_address AS ip_address, click.actual_cpc AS actual_cpc;

clicks_sample = SAMPLE clicks_selected 0.1;

clicks_trimmed = FOREACH clicks_sample GENERATE request_id, actual_cpc, convert_ip_to_num(ip_address) AS ip_address;

clicks_by_ip_address = GROUP clicks_trimmed BY ip_address;

clicks = FOREACH clicks_by_ip_address GENERATE FLATTEN(group) AS ip_address, count(request_id) AS num_clicks, sum(actual_cpc) AS revenue;

click_blocks = FOREACH clicks {
                                    block_filtered = FILTER blocks BY startIpNum <= clicks::ip_address AND endIpNum >= clicks::ip_address;
                                    GENERATE clicks::ip_address AS ip_address, clicks::num_clicks AS num_clicks, clicks::revenue AS revenue, block_filtered::locId;
                              };

click_location = JOIN click_blocks BY locId LEFT OUTER, location_trimmed BY locId;
click_location_classfied = FOREACH click_location GENERATE (country == 'US'?'US':'Non-US') AS country, region, num_clicks, revenue;

click_grouped = GROUP click_location BY (country, region);
click_aggregated = FOREACH click_grouped GENERATE FLATTEN(group) AS (country, region), sum(num_clicks) AS click_count, sum(revenue) AS revenue;

STORE click_aggregated INTO '$STORE_PATH' USING PigStorage('\t');


/* UDF to convert ip_address to numeric */

 import socket

 def convert_ip_to_num(obj):
     try:
         return socket.inet_aton(obj)
     except socket.error:
         print "not ipv4 address"
