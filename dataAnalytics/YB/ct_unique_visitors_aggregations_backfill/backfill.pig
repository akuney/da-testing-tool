REGISTER                    's3n://intentmedia-hadoop-production/jars/intentmedia.jar';
REGISTER                    's3n://intentmedia-hadoop-production/jars/udfs.py' using streaming_python;
IMPORT                      's3n://intentmedia-hadoop-production/jars/macros.pig';

SET default_parallel 55;

%default PUBLISHER          'EXPEDIA';
%default SITE               'EXPEDIA';
--%default START_DATE         '20110301';
--%default END_DATE           '20140608';
%default START_DATE         '20140621';
%default END_DATE           '20140622';
%default PRODUCT_CATEGORY   'FLIGHTS';

%declare INPUT_PATH         's3n://intentmedia-hadoop-production/input/';
%default STORE_PATH         's3n://intentmedia-hawk-output/yoojong_bang/uv/';

%declare REDUCERS 100;

ad_call_raw = LOAD '$INPUT_PATH' USING com.intentmedia.pig.TypedDataJsonLoader('com.intentmedia.data.AdCallData', '$START_DATE', '$END_DATE', '$PUBLISHER', '$PRODUCT_CATEGORY');

ad_call_filtered = FILTER ad_call_raw BY
            requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('$START_DATE') AND
            requested_at <= com.intentmedia.pig.udf.date.EndOfDayInMillis('$END_DATE') AND
            ip_address_blacklisted == 0 AND
            ad_unit_type == 'CT' AND
            site_type == '$SITE' AND
            publisher_user_id IS NOT NULL;

ad_calls = FOREACH ad_call_filtered GENERATE TOTUPLE(*) AS ad_call;

ad_call_selected = FOREACH ad_calls GENERATE ad_call.publisher_user_id AS publisher_user_id, ad_call.webuser_id AS webuser_id,
                                             org.apache.pig.piggybank.evaluation.datetime.truncate.ISOToDay(org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO(ad_call.requested_at)) AS requested_at,
                                             ad_call.product_category_type AS product_category_type, ad_call.site_id AS site_id,
                                             ad_call.ad_unit_id AS ad_unit_id, ad_call.ip_address_blacklisted AS ip_address_blacklisted, ad_call.ad_unit_type as ad_unit_type,
                                             (ad_call.publisher_user_id IS NOT NULL ? ad_call.publisher_user_id : ad_call.webuser_id) AS user_id;


ad_call_grouped_by_ad_unit = GROUP ad_call_selected BY (requested_at, site_id, product_category_type, ad_unit_id);
ad_call_aggregated_by_ad_unit = FOREACH ad_call_grouped_by_ad_unit {
    user_id  = DISTINCT ad_call_selected.user_id;
    GENERATE FLATTEN(group) AS (requested_at, site_id, product_category_type, ad_unit_id), COUNT(user_id) AS unique_visitors;
};
ad_call_by_ad_unit = FOREACH ad_call_aggregated_by_ad_unit GENERATE requested_at, site_id, product_category_type, ad_unit_id, unique_visitors;

ad_call_grouped_by_product_category = GROUP ad_call_selected BY (requested_at, site_id, product_category_type);
ad_call_aggregated_by_product_category = FOREACH ad_call_grouped_by_product_category {
    user_id  = DISTINCT ad_call_selected.user_id;
    GENERATE FLATTEN(group) AS (requested_at, site_id, product_category_type), COUNT(user_id) AS unique_visitors;
};
ad_call_by_product_category = FOREACH ad_call_aggregated_by_product_category GENERATE requested_at, site_id, product_category_type, null AS ad_unit_id, unique_visitors;

ad_call_grouped_by_site = GROUP ad_call_selected BY (requested_at, site_id);
ad_call_aggregated_by_site = FOREACH ad_call_grouped_by_site {
    user_id  = DISTINCT ad_call_selected.user_id;
    GENERATE FLATTEN(group) AS (requested_at, site_id), COUNT(user_id) AS unique_visitors;
};
ad_call_by_site = FOREACH ad_call_aggregated_by_site GENERATE requested_at, site_id, null AS product_category_type, null AS ad_unit_id, unique_visitors;

ad_call_aggregated = UNION ONSCHEMA ad_call_by_ad_unit, ad_call_by_product_category, ad_call_by_site;

STORE ad_call_aggregated INTO '$STORE_PATH' USING PigStorage('\t');