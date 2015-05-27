-- We start with the stuff that you have to declare in order to access IM UDFs and jars. This might be a bit out of date, but the data team maintains these, so one of our engineers will point you to the right place.

REGISTER                    's3n://intentmedia-hadoop-production/jars/intentmedia.jar';
REGISTER                    's3n://intentmedia-hadoop-production/jars/udfs.py' using streaming_python;
IMPORT                      's3n://intentmedia-hadoop-production/jars/macros.pig';

-- A few basic variables defined.

%default start_date         '20140101';
%default end_date           '20140114';
%default product_category   'FLIGHTS';

-- Paths. There are just two directories here: hadoop production, where all the log data for input lives, and my directory on hawk output, where output data gets stored. I also will sometimes put custom input data in my hawk output directory. That's what ad_units.tsv is about: we don't have access to any reference tables in our logs, so if I need to refer to one of them - entities and ad_units are the most common - I'll export them out of production into a .tsv file, and drop that into my hawk output directory. Then I can load in the file from there.

%default au_path            's3n://intentmedia-hawk-output/david_peer/files/ad_units.tsv/';
%declare input_path         's3n://intentmedia-hadoop-production/input/';
%default store_path         's3n://intentmedia-hawk-output/david_peer/advertiser_value/flights/';

-- Finally, some stuff we declare because it makes everything run better, apparently.

%declare REDUCERS 100;
set default_parallel 55;

-- I always start by loading the data I'll need, which seems like a pretty natural place to start. My custom ad units table first. Where did this file come from? Well, I opened up our MySQL production db, and ran this query:

--SELECT id, name, ad_type, product_category_type, active FROM ad_units;

-- Pretty simple. Then - in SequelPro, at least - you select "Export As CSV file", change the terminate from a comma to \t, get rid of text wrap, and hit "Export". I then plop the resulting file into my hawk directory, and load it as so:

ad_units = LOAD '$au_path' USING PigStorage('\t') AS (id:int, name:chararray, ad_type:chararray, product_category_type:chararray, active:int);

ad_units_flight_CT = FILTER ad_units BY ad_type == 'CT' AND product_category_type == 'FLIGHTS';

-- So now I have an alias that's just a list of all AfT flights ad units. Why might I do this, by the way? Well, I wouldn't have to if we had the ad_type and product_category_type fields in impressions, but we don't. So I use it to filter to the product that I want. Anyway, on to loading impressions:

impressions_raw = LOAD '$input_path' USING com.intentmedia.pig.TypedDataJsonLoader('com.intentmedia.data.ImpressionData', '$start_date', '$end_date', '', '');

-- That's our impression-loading UDF up there. The only part of it that specifies impressions is the "ImpressionData" bit; change that to "ClickData" or "AdCallData" or whatever and you should get clicks or ad calls or whatever, respectively. How do you know what to enter? Just guess. The last four variables are for the start date, end date, publisher, and product category type. I'll always specify the dates, to avoid loading a ridiculous amount of data, but specifying the others is a matter of preference.

-- Now I'm going to filter those impressions down, and then trim to just the fields I need. This is good practice to keep run times down. The auction position filter shouldn't be necessary, but I use it to keep out bad data.

impressions_filtered = FILTER impressions_raw BY
    requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('$start_date') AND 
    requested_at <= com.intentmedia.pig.udf.date.EndOfDayInMillis('$end_date') AND
    ip_address_blacklisted == 0 AND
    auction_position >= 1 AND auction_position <= 50;

impressions_au_filter = JOIN ad_units_flight_CT BY id, impressions_filtered BY ad_unit_id;

impressions_trimmed = FOREACH impressions_au_filter GENERATE impressions_filtered::request_id AS request_id, impressions_filtered::external_id AS external_id, impressions_filtered::ad_unit_id AS ad_unit_id, impressions_filtered::advertiser_id AS advertiser_id, impressions_filtered::effective_bid AS effective_bid, impressions_filtered::quality_score AS quality_score, impressions_filtered::auction_position AS auction_position;

-- Okay, so there we go. Since I joined on ad_units_flight_CT, we're now just dealing with AfT flight impressions, within our specified date range.

-- Now repeat with filtered advertisements. We're reconstructing the whole auction here, so we need to know who lost as well as who won. And remember that the filter_cause_type field allows us to specify why the advertiser was excluded, so for this purpose we just want to know those advertisers who didn't show because they didn't bid enough. That is, they were eligible participants in the auction, they just lost. That filter_cause_type is 'BID_TOO_LOW'. 

fa_raw = LOAD '$input_path' USING com.intentmedia.pig.TypedDataJsonLoader('com.intentmedia.data.FilteredAdvertisementData', '$start_date', '$end_date', '', '');

fa_filtered = FILTER fa_raw BY
    requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('$start_date') AND 
    requested_at <= com.intentmedia.pig.udf.date.EndOfDayInMillis('$end_date') AND
    ip_address_blacklisted == 0 AND
    auction_position >= 1 AND auction_position <= 50 AND
    filter_cause_type == 'BID_TOO_LOW';

fa_au_filter = JOIN ad_units_flight_CT BY id, fa_filtered BY ad_unit_id;

fa_trimmed = FOREACH fa_au_filter GENERATE fa_filtered::request_id AS request_id, 'None' AS external_id, fa_filtered::ad_unit_id AS ad_unit_id, fa_filtered::advertiser_id AS advertiser_id, fa_filtered::effectiveBid AS effective_bid, fa_filtered::quality_score AS quality_score, fa_filtered::auction_position AS auction_position;

-- Now clicks! Clicks has the product_category_type field, so we can filter by that, and since there are so few clicks (relative to impressions and filtered advertisements), I don't bother joining with the ad units table. Also, I don't think there's an ad_unit_id field in clicks anyway.

clicks_raw = LOAD '$input_path' USING com.intentmedia.pig.TypedDataJsonLoader('com.intentmedia.data.ClickData', '$start_date', '$end_date', '', '$product_category');

clicks_filtered = FILTER clicks_raw BY
    requested_at >= com.intentmedia.pig.udf.date.StartOfDayInMillis('$start_date') AND
    requested_at <= com.intentmedia.pig.udf.date.EndOfDayInMillis('$end_date') AND
    ip_address_blacklisted == 0 AND
    fraudulent == 0 AND
    product_category_type == '$product_category';
        
clicks_trimmed = FOREACH clicks_filtered GENERATE external_impression_id;

-- ALL RIGHT LET'S GET ROLLING. As you may or may not know, we just generate quality scores for auction position 1; we don't correct for the fact that auction position 5, say, gets way fewer clicks. So quality scores for any position greater than 1 will way overstate the likelihood of that advertiser being clicked. So I correct for this by calculating a "discount" on quality scores, by ad unit and position.

i_cl = JOIN impressions_trimmed BY external_id LEFT OUTER, clicks_trimmed BY external_impression_id;
i_cl_trimmed = FOREACH i_cl GENERATE impressions_trimmed::ad_unit_id AS ad_unit_id, impressions_trimmed::auction_position AS auction_position, impressions_trimmed::external_id AS external_id, impressions_trimmed::quality_score AS quality_score, clicks_trimmed::external_impression_id AS external_impression_id;
i_cl_grouped = GROUP i_cl_trimmed BY (ad_unit_id, auction_position);
i_cl_aggregated = FOREACH i_cl_grouped GENERATE FLATTEN(group) AS (ad_unit_id, auction_position), COUNT(i_cl_trimmed.external_id) AS impressions, SUM(i_cl_trimmed.quality_score) AS qs_sum, COUNT(i_cl_trimmed.external_impression_id) AS clicks;
qs_discount = FOREACH i_cl_aggregated GENERATE ad_unit_id, auction_position, (clicks/qs_sum) AS qs_discount;

-- Now let's throw impressions and filtered advertisements together and really get this party started. I'll also cut out any advertisers who finished worse than position 7, because they're not relevant for monetization. Why 7? As of right now, our biggest ad unit has 6 advertisers in it. Obviously the top 6 advertisers are helping monetization, but the advertiser that finishes 7th is helping too, because that advertiser provided price support for the 6th. But beyond position 7 the advertiser provided no value to that particular auction. Why didn't I filter down to auction_positions 1-7 when I was loading impressions and filtered advertisements above? Shut up, that's why.

i_fa_raw = UNION impressions_trimmed, fa_trimmed;
i_fa = FILTER i_fa_raw BY auction_position >= 1 AND auction_position <= 7;

-- And now commences the "drop" processes. Simple enough: filter out one position in the auction to "drop", then calculate CPCs under the new alignment. To calculate the unit's value, multiply the CPC by the quality score, then multiply that by the quality score "discount", which is ad unit-dependent. Do this dropping each of positions 1-6, and also without a drop ("position 0"), which is redundant, but keeps everything calculated consistently.

i_fa_drop_0_trimmed = FOREACH i_fa GENERATE ad_unit_id, request_id, effective_bid, quality_score, ap_new_0(auction_position) AS ap_new, (ap_new_0(auction_position) + 1) AS ap_next, (effective_bid * quality_score) AS expectation;
i_fa_drop_0_trimmed_next = FOREACH i_fa_drop_0_trimmed GENERATE ad_unit_id, request_id, effective_bid, quality_score, ap_new, ap_next, expectation;
i_fa_drop_0_with_next = JOIN i_fa_drop_0_trimmed BY (request_id, ap_next) LEFT OUTER, i_fa_drop_0_trimmed_next BY (request_id, ap_new);
i_fa_drop_0_with_next_trimmed = FOREACH i_fa_drop_0_with_next GENERATE i_fa_drop_0_trimmed::ad_unit_id AS ad_unit_id, i_fa_drop_0_trimmed::request_id AS request_id, i_fa_drop_0_trimmed::effective_bid AS effective_bid, i_fa_drop_0_trimmed::quality_score AS quality_score, i_fa_drop_0_trimmed::ap_new AS ap_new, i_fa_drop_0_trimmed::expectation AS expectation, i_fa_drop_0_trimmed_next::expectation AS expectation_next, raw_cpc(i_fa_drop_0_trimmed_next::expectation, i_fa_drop_0_trimmed::quality_score) AS raw_cpc;
i_fa_drop_0_contribution_joined = JOIN qs_discount BY (ad_unit_id, auction_position), i_fa_drop_0_with_next_trimmed BY (ad_unit_id, ap_new);
i_fa_drop_0_contribution_trimmed = FOREACH i_fa_drop_0_contribution_joined GENERATE i_fa_drop_0_with_next_trimmed::ad_unit_id AS ad_unit_id, i_fa_drop_0_with_next_trimmed::request_id AS request_id, i_fa_drop_0_with_next_trimmed::ap_new AS ap_new, i_fa_drop_0_with_next_trimmed::quality_score AS quality_score, i_fa_drop_0_with_next_trimmed::raw_cpc AS raw_cpc, qs_discount::qs_discount AS qs_discount, (i_fa_drop_0_with_next_trimmed::quality_score * qs_discount::qs_discount) AS clicks, (i_fa_drop_0_with_next_trimmed::raw_cpc * i_fa_drop_0_with_next_trimmed::quality_score * qs_discount::qs_discount) AS contribution;
i_fa_drop_0_contribution_grouped = GROUP i_fa_drop_0_contribution_trimmed BY request_id;
i_fa_drop_0_final = FOREACH i_fa_drop_0_contribution_grouped GENERATE group AS request_id, SUM(i_fa_drop_0_contribution_trimmed.clicks) AS total_clicks, SUM(i_fa_drop_0_contribution_trimmed.contribution) AS total_contribution;

i_fa_drop_1 = FILTER i_fa BY auction_position != 1;
i_fa_drop_1_trimmed = FOREACH i_fa_drop_1 GENERATE ad_unit_id, request_id, effective_bid, quality_score, ap_new_1(auction_position) AS ap_new, (ap_new_1(auction_position) + 1) AS ap_next, (effective_bid * quality_score) AS expectation;
i_fa_drop_1_trimmed_next = FOREACH i_fa_drop_1_trimmed GENERATE ad_unit_id, request_id, effective_bid, quality_score, ap_new, ap_next, expectation;
i_fa_drop_1_with_next = JOIN i_fa_drop_1_trimmed BY (request_id, ap_next) LEFT OUTER, i_fa_drop_1_trimmed_next BY (request_id, ap_new);
i_fa_drop_1_with_next_trimmed = FOREACH i_fa_drop_1_with_next GENERATE i_fa_drop_1_trimmed::ad_unit_id AS ad_unit_id, i_fa_drop_1_trimmed::request_id AS request_id, i_fa_drop_1_trimmed::effective_bid AS effective_bid, i_fa_drop_1_trimmed::quality_score AS quality_score, i_fa_drop_1_trimmed::ap_new AS ap_new, i_fa_drop_1_trimmed::expectation AS expectation, i_fa_drop_1_trimmed_next::expectation AS expectation_next, raw_cpc(i_fa_drop_1_trimmed_next::expectation, i_fa_drop_1_trimmed::quality_score) AS raw_cpc;
i_fa_drop_1_contribution_joined = JOIN qs_discount BY (ad_unit_id, auction_position), i_fa_drop_1_with_next_trimmed BY (ad_unit_id, ap_new);
i_fa_drop_1_contribution_trimmed = FOREACH i_fa_drop_1_contribution_joined GENERATE i_fa_drop_1_with_next_trimmed::ad_unit_id AS ad_unit_id, i_fa_drop_1_with_next_trimmed::request_id AS request_id, i_fa_drop_1_with_next_trimmed::ap_new AS ap_new, i_fa_drop_1_with_next_trimmed::quality_score AS quality_score, (i_fa_drop_1_with_next_trimmed::quality_score * qs_discount::qs_discount) AS clicks, i_fa_drop_1_with_next_trimmed::raw_cpc AS raw_cpc, qs_discount::qs_discount AS qs_discount, (i_fa_drop_1_with_next_trimmed::raw_cpc * i_fa_drop_1_with_next_trimmed::quality_score * qs_discount::qs_discount) AS contribution;
i_fa_drop_1_contribution_grouped = GROUP i_fa_drop_1_contribution_trimmed BY request_id;
i_fa_drop_1_final = FOREACH i_fa_drop_1_contribution_grouped GENERATE group AS request_id, SUM(i_fa_drop_1_contribution_trimmed.clicks) AS total_clicks, SUM(i_fa_drop_1_contribution_trimmed.contribution) AS total_contribution;

i_fa_drop_2 = FILTER i_fa BY auction_position != 2;
i_fa_drop_2_trimmed = FOREACH i_fa_drop_2 GENERATE ad_unit_id, request_id, effective_bid, quality_score, ap_new_2(auction_position) AS ap_new, (ap_new_2(auction_position) + 1) AS ap_next, (effective_bid * quality_score) AS expectation;
i_fa_drop_2_trimmed_next = FOREACH i_fa_drop_2_trimmed GENERATE ad_unit_id, request_id, effective_bid, quality_score, ap_new, ap_next, expectation;
i_fa_drop_2_with_next = JOIN i_fa_drop_2_trimmed BY (request_id, ap_next) LEFT OUTER, i_fa_drop_2_trimmed_next BY (request_id, ap_new);
i_fa_drop_2_with_next_trimmed = FOREACH i_fa_drop_2_with_next GENERATE i_fa_drop_2_trimmed::ad_unit_id AS ad_unit_id, i_fa_drop_2_trimmed::request_id AS request_id, i_fa_drop_2_trimmed::effective_bid AS effective_bid, i_fa_drop_2_trimmed::quality_score AS quality_score, i_fa_drop_2_trimmed::ap_new AS ap_new, i_fa_drop_2_trimmed::expectation AS expectation, i_fa_drop_2_trimmed_next::expectation AS expectation_next, raw_cpc(i_fa_drop_2_trimmed_next::expectation, i_fa_drop_2_trimmed::quality_score) AS raw_cpc;
i_fa_drop_2_contribution_joined = JOIN qs_discount BY (ad_unit_id, auction_position), i_fa_drop_2_with_next_trimmed BY (ad_unit_id, ap_new);
i_fa_drop_2_contribution_trimmed = FOREACH i_fa_drop_2_contribution_joined GENERATE i_fa_drop_2_with_next_trimmed::ad_unit_id AS ad_unit_id, i_fa_drop_2_with_next_trimmed::request_id AS request_id, i_fa_drop_2_with_next_trimmed::ap_new AS ap_new, i_fa_drop_2_with_next_trimmed::quality_score AS quality_score, (i_fa_drop_2_with_next_trimmed::quality_score * qs_discount::qs_discount) AS clicks, i_fa_drop_2_with_next_trimmed::raw_cpc AS raw_cpc, qs_discount::qs_discount AS qs_discount, (i_fa_drop_2_with_next_trimmed::raw_cpc * i_fa_drop_2_with_next_trimmed::quality_score * qs_discount::qs_discount) AS contribution;
i_fa_drop_2_contribution_grouped = GROUP i_fa_drop_2_contribution_trimmed BY request_id;
i_fa_drop_2_final = FOREACH i_fa_drop_2_contribution_grouped GENERATE group AS request_id, SUM(i_fa_drop_2_contribution_trimmed.clicks) AS total_clicks, SUM(i_fa_drop_2_contribution_trimmed.contribution) AS total_contribution;

i_fa_drop_3 = FILTER i_fa BY auction_position != 3;
i_fa_drop_3_trimmed = FOREACH i_fa_drop_3 GENERATE ad_unit_id, request_id, effective_bid, quality_score, ap_new_3(auction_position) AS ap_new, (ap_new_3(auction_position) + 1) AS ap_next, (effective_bid * quality_score) AS expectation;
i_fa_drop_3_trimmed_next = FOREACH i_fa_drop_3_trimmed GENERATE ad_unit_id, request_id, effective_bid, quality_score, ap_new, ap_next, expectation;
i_fa_drop_3_with_next = JOIN i_fa_drop_3_trimmed BY (request_id, ap_next) LEFT OUTER, i_fa_drop_3_trimmed_next BY (request_id, ap_new);
i_fa_drop_3_with_next_trimmed = FOREACH i_fa_drop_3_with_next GENERATE i_fa_drop_3_trimmed::ad_unit_id AS ad_unit_id, i_fa_drop_3_trimmed::request_id AS request_id, i_fa_drop_3_trimmed::effective_bid AS effective_bid, i_fa_drop_3_trimmed::quality_score AS quality_score, i_fa_drop_3_trimmed::ap_new AS ap_new, i_fa_drop_3_trimmed::expectation AS expectation, i_fa_drop_3_trimmed_next::expectation AS expectation_next, raw_cpc(i_fa_drop_3_trimmed_next::expectation, i_fa_drop_3_trimmed::quality_score) AS raw_cpc;
i_fa_drop_3_contribution_joined = JOIN qs_discount BY (ad_unit_id, auction_position), i_fa_drop_3_with_next_trimmed BY (ad_unit_id, ap_new);
i_fa_drop_3_contribution_trimmed = FOREACH i_fa_drop_3_contribution_joined GENERATE i_fa_drop_3_with_next_trimmed::ad_unit_id AS ad_unit_id, i_fa_drop_3_with_next_trimmed::request_id AS request_id, i_fa_drop_3_with_next_trimmed::ap_new AS ap_new, i_fa_drop_3_with_next_trimmed::quality_score AS quality_score, (i_fa_drop_3_with_next_trimmed::quality_score * qs_discount::qs_discount) AS clicks, i_fa_drop_3_with_next_trimmed::raw_cpc AS raw_cpc, qs_discount::qs_discount AS qs_discount, (i_fa_drop_3_with_next_trimmed::raw_cpc * i_fa_drop_3_with_next_trimmed::quality_score * qs_discount::qs_discount) AS contribution;
i_fa_drop_3_contribution_grouped = GROUP i_fa_drop_3_contribution_trimmed BY request_id;
i_fa_drop_3_final = FOREACH i_fa_drop_3_contribution_grouped GENERATE group AS request_id, SUM(i_fa_drop_3_contribution_trimmed.clicks) AS total_clicks, SUM(i_fa_drop_3_contribution_trimmed.contribution) AS total_contribution;

i_fa_drop_4 = FILTER i_fa BY auction_position != 4;
i_fa_drop_4_trimmed = FOREACH i_fa_drop_4 GENERATE ad_unit_id, request_id, effective_bid, quality_score, ap_new_4(auction_position) AS ap_new, (ap_new_4(auction_position) + 1) AS ap_next, (effective_bid * quality_score) AS expectation;
i_fa_drop_4_trimmed_next = FOREACH i_fa_drop_4_trimmed GENERATE ad_unit_id, request_id, effective_bid, quality_score, ap_new, ap_next, expectation;
i_fa_drop_4_with_next = JOIN i_fa_drop_4_trimmed BY (request_id, ap_next) LEFT OUTER, i_fa_drop_4_trimmed_next BY (request_id, ap_new);
i_fa_drop_4_with_next_trimmed = FOREACH i_fa_drop_4_with_next GENERATE i_fa_drop_4_trimmed::ad_unit_id AS ad_unit_id, i_fa_drop_4_trimmed::request_id AS request_id, i_fa_drop_4_trimmed::effective_bid AS effective_bid, i_fa_drop_4_trimmed::quality_score AS quality_score, i_fa_drop_4_trimmed::ap_new AS ap_new, i_fa_drop_4_trimmed::expectation AS expectation, i_fa_drop_4_trimmed_next::expectation AS expectation_next, raw_cpc(i_fa_drop_4_trimmed_next::expectation, i_fa_drop_4_trimmed::quality_score) AS raw_cpc;
i_fa_drop_4_contribution_joined = JOIN qs_discount BY (ad_unit_id, auction_position), i_fa_drop_4_with_next_trimmed BY (ad_unit_id, ap_new);
i_fa_drop_4_contribution_trimmed = FOREACH i_fa_drop_4_contribution_joined GENERATE i_fa_drop_4_with_next_trimmed::ad_unit_id AS ad_unit_id, i_fa_drop_4_with_next_trimmed::request_id AS request_id, i_fa_drop_4_with_next_trimmed::ap_new AS ap_new, i_fa_drop_4_with_next_trimmed::quality_score AS quality_score, (i_fa_drop_4_with_next_trimmed::quality_score * qs_discount::qs_discount) AS clicks, i_fa_drop_4_with_next_trimmed::raw_cpc AS raw_cpc, qs_discount::qs_discount AS qs_discount, (i_fa_drop_4_with_next_trimmed::raw_cpc * i_fa_drop_4_with_next_trimmed::quality_score * qs_discount::qs_discount) AS contribution;
i_fa_drop_4_contribution_grouped = GROUP i_fa_drop_4_contribution_trimmed BY request_id;
i_fa_drop_4_final = FOREACH i_fa_drop_4_contribution_grouped GENERATE group AS request_id, SUM(i_fa_drop_4_contribution_trimmed.clicks) AS total_clicks, SUM(i_fa_drop_4_contribution_trimmed.contribution) AS total_contribution;

i_fa_drop_5 = FILTER i_fa BY auction_position != 5;
i_fa_drop_5_trimmed = FOREACH i_fa_drop_5 GENERATE ad_unit_id, request_id, effective_bid, quality_score, ap_new_5(auction_position) AS ap_new, (ap_new_5(auction_position) + 1) AS ap_next, (effective_bid * quality_score) AS expectation;
i_fa_drop_5_trimmed_next = FOREACH i_fa_drop_5_trimmed GENERATE ad_unit_id, request_id, effective_bid, quality_score, ap_new, ap_next, expectation;
i_fa_drop_5_with_next = JOIN i_fa_drop_5_trimmed BY (request_id, ap_next) LEFT OUTER, i_fa_drop_5_trimmed_next BY (request_id, ap_new);
i_fa_drop_5_with_next_trimmed = FOREACH i_fa_drop_5_with_next GENERATE i_fa_drop_5_trimmed::ad_unit_id AS ad_unit_id, i_fa_drop_5_trimmed::request_id AS request_id, i_fa_drop_5_trimmed::effective_bid AS effective_bid, i_fa_drop_5_trimmed::quality_score AS quality_score, i_fa_drop_5_trimmed::ap_new AS ap_new, i_fa_drop_5_trimmed::expectation AS expectation, i_fa_drop_5_trimmed_next::expectation AS expectation_next, raw_cpc(i_fa_drop_5_trimmed_next::expectation, i_fa_drop_5_trimmed::quality_score) AS raw_cpc;
i_fa_drop_5_contribution_joined = JOIN qs_discount BY (ad_unit_id, auction_position), i_fa_drop_5_with_next_trimmed BY (ad_unit_id, ap_new);
i_fa_drop_5_contribution_trimmed = FOREACH i_fa_drop_5_contribution_joined GENERATE i_fa_drop_5_with_next_trimmed::ad_unit_id AS ad_unit_id, i_fa_drop_5_with_next_trimmed::request_id AS request_id, i_fa_drop_5_with_next_trimmed::ap_new AS ap_new, i_fa_drop_5_with_next_trimmed::quality_score AS quality_score, (i_fa_drop_5_with_next_trimmed::quality_score * qs_discount::qs_discount) AS clicks, i_fa_drop_5_with_next_trimmed::raw_cpc AS raw_cpc, qs_discount::qs_discount AS qs_discount, (i_fa_drop_5_with_next_trimmed::raw_cpc * i_fa_drop_5_with_next_trimmed::quality_score * qs_discount::qs_discount) AS contribution;
i_fa_drop_5_contribution_grouped = GROUP i_fa_drop_5_contribution_trimmed BY request_id;
i_fa_drop_5_final = FOREACH i_fa_drop_5_contribution_grouped GENERATE group AS request_id, SUM(i_fa_drop_5_contribution_trimmed.clicks) AS total_clicks, SUM(i_fa_drop_5_contribution_trimmed.contribution) AS total_contribution;

i_fa_drop_6 = FILTER i_fa BY auction_position != 6;
i_fa_drop_6_trimmed = FOREACH i_fa_drop_6 GENERATE ad_unit_id, request_id, effective_bid, quality_score, ap_new_6(auction_position) AS ap_new, (ap_new_6(auction_position) + 1) AS ap_next, (effective_bid * quality_score) AS expectation;
i_fa_drop_6_trimmed_next = FOREACH i_fa_drop_6_trimmed GENERATE ad_unit_id, request_id, effective_bid, quality_score, ap_new, ap_next, expectation;
i_fa_drop_6_with_next = JOIN i_fa_drop_6_trimmed BY (request_id, ap_next) LEFT OUTER, i_fa_drop_6_trimmed_next BY (request_id, ap_new);
i_fa_drop_6_with_next_trimmed = FOREACH i_fa_drop_6_with_next GENERATE i_fa_drop_6_trimmed::ad_unit_id AS ad_unit_id, i_fa_drop_6_trimmed::request_id AS request_id, i_fa_drop_6_trimmed::effective_bid AS effective_bid, i_fa_drop_6_trimmed::quality_score AS quality_score, i_fa_drop_6_trimmed::ap_new AS ap_new, i_fa_drop_6_trimmed::expectation AS expectation, i_fa_drop_6_trimmed_next::expectation AS expectation_next, raw_cpc(i_fa_drop_6_trimmed_next::expectation, i_fa_drop_6_trimmed::quality_score) AS raw_cpc;
i_fa_drop_6_contribution_joined = JOIN qs_discount BY (ad_unit_id, auction_position), i_fa_drop_6_with_next_trimmed BY (ad_unit_id, ap_new);
i_fa_drop_6_contribution_trimmed = FOREACH i_fa_drop_6_contribution_joined GENERATE i_fa_drop_6_with_next_trimmed::ad_unit_id AS ad_unit_id, i_fa_drop_6_with_next_trimmed::request_id AS request_id, i_fa_drop_6_with_next_trimmed::ap_new AS ap_new, i_fa_drop_6_with_next_trimmed::quality_score AS quality_score, (i_fa_drop_6_with_next_trimmed::quality_score * qs_discount::qs_discount) AS clicks, i_fa_drop_6_with_next_trimmed::raw_cpc AS raw_cpc, qs_discount::qs_discount AS qs_discount, (i_fa_drop_6_with_next_trimmed::raw_cpc * i_fa_drop_6_with_next_trimmed::quality_score * qs_discount::qs_discount) AS contribution;
i_fa_drop_6_contribution_grouped = GROUP i_fa_drop_6_contribution_trimmed BY request_id;
i_fa_drop_6_final = FOREACH i_fa_drop_6_contribution_grouped GENERATE group AS request_id, SUM(i_fa_drop_6_contribution_trimmed.clicks) AS total_clicks, SUM(i_fa_drop_6_contribution_trimmed.contribution) AS total_contribution;

-- Now we just have to take all those individual contributions and join them back onto the original records. We'll pick the appropriate contribution for each advertiser, then summarize by ad unit and advertiser.

i_fa_advertisers = FOREACH i_fa GENERATE ad_unit_id, request_id, advertiser_id, auction_position;

i_fa_advertisers_0 = JOIN i_fa_advertisers BY request_id, i_fa_drop_0_final BY request_id;
i_fa_advertisers_0_trimmed = FOREACH i_fa_advertisers_0 GENERATE i_fa_advertisers::ad_unit_id AS ad_unit_id, i_fa_advertisers::request_id AS request_id, i_fa_advertisers::advertiser_id AS advertiser_id, i_fa_advertisers::auction_position AS auction_position, i_fa_drop_0_final::total_clicks AS clicks_0, i_fa_drop_0_final::total_contribution AS contribution_0;

i_fa_advertisers_1 = JOIN i_fa_advertisers_0_trimmed BY request_id, i_fa_drop_1_final BY request_id;
i_fa_advertisers_1_trimmed = FOREACH i_fa_advertisers_1 GENERATE i_fa_advertisers_0_trimmed::ad_unit_id AS ad_unit_id, i_fa_advertisers_0_trimmed::request_id AS request_id, i_fa_advertisers_0_trimmed::advertiser_id AS advertiser_id, i_fa_advertisers_0_trimmed::auction_position AS auction_position, i_fa_advertisers_0_trimmed::clicks_0 AS clicks_0, i_fa_advertisers_0_trimmed::contribution_0 AS contribution_0, i_fa_drop_1_final::total_clicks AS clicks_1, i_fa_drop_1_final::total_contribution AS contribution_1;

i_fa_advertisers_2 = JOIN i_fa_advertisers_1_trimmed BY request_id, i_fa_drop_2_final BY request_id;
i_fa_advertisers_2_trimmed = FOREACH i_fa_advertisers_2 GENERATE i_fa_advertisers_1_trimmed::ad_unit_id AS ad_unit_id, i_fa_advertisers_1_trimmed::request_id AS request_id, i_fa_advertisers_1_trimmed::advertiser_id AS advertiser_id, i_fa_advertisers_1_trimmed::auction_position AS auction_position, i_fa_advertisers_1_trimmed::clicks_0 AS clicks_0, i_fa_advertisers_1_trimmed::contribution_0 AS contribution_0, i_fa_advertisers_1_trimmed::clicks_1 AS clicks_1, i_fa_advertisers_1_trimmed::contribution_1 AS contribution_1, i_fa_drop_2_final::total_clicks AS clicks_2, i_fa_drop_2_final::total_contribution AS contribution_2;

i_fa_advertisers_3 = JOIN i_fa_advertisers_2_trimmed BY request_id, i_fa_drop_3_final BY request_id;
i_fa_advertisers_3_trimmed = FOREACH i_fa_advertisers_3 GENERATE i_fa_advertisers_2_trimmed::ad_unit_id AS ad_unit_id, i_fa_advertisers_2_trimmed::request_id AS request_id, i_fa_advertisers_2_trimmed::advertiser_id AS advertiser_id, i_fa_advertisers_2_trimmed::auction_position AS auction_position, i_fa_advertisers_2_trimmed::clicks_0 AS clicks_0, i_fa_advertisers_2_trimmed::contribution_0 AS contribution_0, i_fa_advertisers_2_trimmed::clicks_1 AS clicks_1, i_fa_advertisers_2_trimmed::contribution_1 AS contribution_1, i_fa_advertisers_2_trimmed::clicks_2 AS clicks_2, i_fa_advertisers_2_trimmed::contribution_2 AS contribution_2, i_fa_drop_3_final::total_clicks AS clicks_3, i_fa_drop_3_final::total_contribution AS contribution_3;

i_fa_advertisers_4 = JOIN i_fa_advertisers_3_trimmed BY request_id, i_fa_drop_4_final BY request_id;
i_fa_advertisers_4_trimmed = FOREACH i_fa_advertisers_4 GENERATE i_fa_advertisers_3_trimmed::ad_unit_id AS ad_unit_id, i_fa_advertisers_3_trimmed::request_id AS request_id, i_fa_advertisers_3_trimmed::advertiser_id AS advertiser_id, i_fa_advertisers_3_trimmed::auction_position AS auction_position, i_fa_advertisers_3_trimmed::clicks_0 AS clicks_0, i_fa_advertisers_3_trimmed::contribution_0 AS contribution_0, i_fa_advertisers_3_trimmed::clicks_1 AS clicks_1, i_fa_advertisers_3_trimmed::contribution_1 AS contribution_1, i_fa_advertisers_3_trimmed::clicks_2 AS clicks_2, i_fa_advertisers_3_trimmed::contribution_2 AS contribution_2, i_fa_advertisers_3_trimmed::clicks_3 AS clicks_3, i_fa_advertisers_3_trimmed::contribution_3 AS contribution_3, i_fa_drop_4_final::total_clicks AS clicks_4, i_fa_drop_4_final::total_contribution AS contribution_4;

i_fa_advertisers_5 = JOIN i_fa_advertisers_4_trimmed BY request_id, i_fa_drop_5_final BY request_id;
i_fa_advertisers_5_trimmed = FOREACH i_fa_advertisers_5 GENERATE i_fa_advertisers_4_trimmed::ad_unit_id AS ad_unit_id, i_fa_advertisers_4_trimmed::request_id AS request_id, i_fa_advertisers_4_trimmed::advertiser_id AS advertiser_id, i_fa_advertisers_4_trimmed::auction_position AS auction_position, i_fa_advertisers_4_trimmed::clicks_0 AS clicks_0, i_fa_advertisers_4_trimmed::contribution_0 AS contribution_0, i_fa_advertisers_4_trimmed::clicks_1 AS clicks_1, i_fa_advertisers_4_trimmed::contribution_1 AS contribution_1, i_fa_advertisers_4_trimmed::clicks_2 AS clicks_2, i_fa_advertisers_4_trimmed::contribution_2 AS contribution_2, i_fa_advertisers_4_trimmed::clicks_3 AS clicks_3, i_fa_advertisers_4_trimmed::contribution_3 AS contribution_3, i_fa_advertisers_4_trimmed::clicks_4 AS clicks_4, i_fa_advertisers_4_trimmed::contribution_4 AS contribution_4, i_fa_drop_5_final::total_clicks AS clicks_5, i_fa_drop_5_final::total_contribution AS contribution_5;

i_fa_advertisers_6 = JOIN i_fa_advertisers_5_trimmed BY request_id, i_fa_drop_6_final BY request_id;
i_fa_advertisers_6_trimmed = FOREACH i_fa_advertisers_6 GENERATE i_fa_advertisers_5_trimmed::ad_unit_id AS ad_unit_id, i_fa_advertisers_5_trimmed::request_id AS request_id, i_fa_advertisers_5_trimmed::advertiser_id AS advertiser_id, i_fa_advertisers_5_trimmed::auction_position AS auction_position, i_fa_advertisers_5_trimmed::clicks_0 AS clicks_0, i_fa_advertisers_5_trimmed::contribution_0 AS contribution_0, i_fa_advertisers_5_trimmed::clicks_1 AS clicks_1, i_fa_advertisers_5_trimmed::contribution_1 AS contribution_1, i_fa_advertisers_5_trimmed::clicks_2 AS clicks_2, i_fa_advertisers_5_trimmed::contribution_2 AS contribution_2, i_fa_advertisers_5_trimmed::clicks_3 AS clicks_3, i_fa_advertisers_5_trimmed::contribution_3 AS contribution_3, i_fa_advertisers_5_trimmed::clicks_4 AS clicks_4, i_fa_advertisers_5_trimmed::contribution_4 AS contribution_4, i_fa_advertisers_5_trimmed::clicks_5 AS clicks_5, i_fa_advertisers_5_trimmed::contribution_5 AS contribution_5, i_fa_drop_6_final::total_clicks AS clicks_6, i_fa_drop_6_final::total_contribution AS contribution_6;

advertiser_value = FOREACH i_fa_advertisers_6_trimmed GENERATE ad_unit_id, request_id, advertiser_id, auction_position, advertiser_clicks(auction_position, clicks_0, clicks_1, clicks_2, clicks_3, clicks_4, clicks_5, clicks_6) AS advertiser_clicks, advertiser_value(auction_position, contribution_0, contribution_1, contribution_2, contribution_3, contribution_4, contribution_5, contribution_6) AS advertiser_value;

-- Roll it up!

advertiser_value_grouped = GROUP advertiser_value BY (ad_unit_id, advertiser_id);
advertiser_value_final = FOREACH advertiser_value_grouped GENERATE FLATTEN(group) AS (ad_unit_id, advertiser_id), AVG(advertiser_value.auction_position) AS avg_position, SUM(advertiser_value.advertiser_clicks) AS advertiser_clicks, SUM(advertiser_value.advertiser_value) AS advertiser_value;

-- Store this.

STORE advertiser_value_final INTO '$store_path' USING PigStorage('\t');

-- And we're done. The output can be compared to actual clicked value during the same period: so if American generated $1000 in this period, but the sum of their advertiser_value is $600, then only $600 of the $1000 in revenue they generated was incremental. If we took American out of all of those auctions, revenue would only go down by $600, not $1000. You can determine the impact of an advertiser on clicks and CPCs too, because we calcualte incremental clicks (advertiser_clicks) as well as incremental revenue.

-- Note that advertiser_value should always be positive, because if the advertiser wasn't contributing to monetization, they shouldn't have won the auction. The only exception is for when we have a new advertiser: we give them a bunch of "learning impressions" for which we shoot them into the unit whether they perform well in the auction or not, just to get a reliable feel for their quality score. So these learning impressions could be sub-optimal for monetization. advertiser_clicks could be positive or negative: just because an advertiser increases monetization, that advertiser may or may increase clicks. Advertisers that bid very high but have poor quality scores will end up with negative advertiser_clicks values.

-- A quick summary way of thinking about advertiser_value is the value of that advertiser over a replacement advertiser. It's a very baseball concept.
