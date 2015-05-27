REGISTER                    's3n://intentmedia-hadoop-production/jars/intentmedia.jar';
REGISTER                    's3n://intentmedia-hadoop-production/jars/udfs.py' using streaming_python;
IMPORT                      's3n://intentmedia-hadoop-production/jars/macros.pig';

SET default_parallel 55;

%default PUBLISHER          'EXPEDIA';
%default PRODUCT_CATEGORY   'GENERAL';
%default START_DATE         '20141013';
%default END_DATE           '20141111';

%declare INPUT_PATH         's3n://intentmedia-hadoop-production/input/';
%default STORE_PATH         's3n://intentmedia-hawk-output/yoojong_bang/beacons/';

%declare REDUCERS 100;

-- Load raw data with parameters
beacons_raw = LOAD '$INPUT_PATH' USING com.intentmedia.pig.TypedDataJsonLoader('com.intentmedia.data.BeaconData', '$START_DATE', '$END_DATE', '$PUBLISHER', '$PRODUCT_CATEGORY');

/*
-- Email type beacon count per site, country, browser_family, has_member_id, has_referrer_url
beacons_filtered = FILTER beacons_raw BY ip_address_blacklisted == 0 AND page_view_type == 'EMAIL';

beacons = FOREACH beacons_filtered GENERATE TOTUPLE(*) AS beacon;

beacons_renamed = FOREACH beacons GENERATE
org.apache.pig.piggybank.evaluation.datetime.truncate.ISOToDay(org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO(beacon.requested_at)) AS requested_at,
beacon.site_type AS site_type,
(beacon.request_parameters IS NOT NULL ? 'has_request_parameters' : 'blank_request_parameters') AS request_parameters,
beacon.request_parameters#'site_country' AS site_country,
beacon.product_category_type AS product_category_type,
beacon.browser_family AS browser_family,
(beacon.request_parameters#'user_member_id' IS NOT NULL ? 'has_member_id' : 'blank_member_id') AS has_member_id,
(beacon.referrer_url IS NOT NULL ? 'has_referrer_url' : 'blank_referrer_url') AS has_referrer_url,
beacon.request_id AS request_id;

beacons_grouped = GROUP beacons_renamed BY (requested_at, site_type, site_country, product_category_type, browser_family, has_member_id, has_referrer_url);

beacons_aggregated = FOREACH beacons_grouped GENERATE FLATTEN(group) AS (requested_at, site_type, site_country, product_category_type, browser_family, has_member_id, has_referrer_url), COUNT(beacons_renamed.request_id) AS beacon_count;

STORE beacons_aggregated INTO '$STORE_PATH' USING PigStorage('\t');
*/

-- Email type beacon count per site, referrer_url
beacons_filtered = FILTER beacons_raw BY ip_address_blacklisted == 0 AND page_view_type == 'EMAIL' AND referrer_url IS NOT NULL;

beacons = FOREACH beacons_filtered GENERATE TOTUPLE(*) AS beacon;

beacons_renamed = FOREACH beacons GENERATE
beacon.site_type AS site_type,
beacon.browser_family AS browser_family,
(beacon.request_parameters#'user_member_id' IS NOT NULL ? 'has_member_id' : 'blank_member_id') AS has_member_id,
beacon.referrer_url AS referrer_url;

STORE beacons_renamed INTO '$STORE_PATH' USING PigStorage('\t');