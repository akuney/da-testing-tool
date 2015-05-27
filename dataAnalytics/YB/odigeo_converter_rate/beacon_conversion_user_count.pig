REGISTER                    's3n://intentmedia-hadoop-production/jars/intentmedia.jar';
IMPORT                      's3n://intentmedia-hadoop-production/jars/macros.pig';

SET default_parallel 55;

%default PUBLISHER                  'ODIGEO';
--%default START_DATE_EDREAMS         '20141223';
--%default END_DATE_EDREAMS           '20150204';
%default START_DATE_OPODO           '20150123';
%default END_DATE_OPODO             '20150204';

%declare INPUT_PATH         's3n://intentmedia-hadoop-production/input/';
%default STORE_PATH         's3n://intentmedia-hawk-output/yoojong_bang/odigeo_converter_rate/';

%declare REDUCERS 100;

-- Load raw data with parameters
beacons_raw = LOAD '$INPUT_PATH' USING com.intentmedia.pig.TypedDataJsonLoader('com.intentmedia.data.BeaconData', '$START_DATE_OPODO', '$END_DATE_OPODO', '$PUBLISHER', '');
conversion_raw = LOAD '$INPUT_PATH' USING com.intentmedia.pig.TypedDataJsonLoader('com.intentmedia.data.ConversionData', '$START_DATE_OPODO', '$END_DATE_OPODO', '$PUBLISHER', '');

-- Beacons part
beacons_filtered = FILTER beacons_raw BY ip_address_blacklisted == 0 AND page_view_type == 'LIST' AND (site_type == 'OPODO' OR site_type == 'OPODO_UK');
beacon_grouped = GROUP beacons_filtered BY site_type;
beacon_count = FOREACH beacon_grouped {
               unique_publisher_user_id = DISTINCT beacons_filtered.publisher_user_id;
               GENERATE group AS site_type, COUNT(unique_publisher_user_id) AS visitors;
               };

-- Conversions part
conversions_filtered = FILTER conversion_raw BY ip_address_blacklisted == 0 AND (site_type == 'OPODO' OR site_type == 'OPODO_UK');
conversion_grouped = GROUP conversions_filtered BY site_type;
conversion_count = FOREACH conversion_grouped {
               unique_publisher_user_id = DISTINCT conversions_filtered.publisher_user_id;
               GENERATE group AS site_type, COUNT(unique_publisher_user_id) as visitors;
               };

joined_data = JOIN beacon_count BY site_type, conversion_count BY site_type;

STORE joined_data INTO '$STORE_PATH' USING PigStorage('\t');
