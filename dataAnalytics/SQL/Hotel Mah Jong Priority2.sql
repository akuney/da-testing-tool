drop table if exists intent_media_sandbox_production.PQ_mismapping_adcalls;

CREATE TABLE intent_media_sandbox_production.PQ_mismapping_adcalls AS
(SELECT publisher_id,
        request_id, 
        publisher_hotel_property_ID as pub_hotel_ID
FROM intent_media_log_data_production.ad_calls
WHERE requested_at_date_in_et = ('2014-09-21')
AND ip_address_blacklisted = FALSE
AND site_type IN ('LASTMINUTE_DOT_COM')
AND ad_unit_type = 'META'
AND outcome_type = 'SERVED');


/* put impressions onto ad calls */
drop table if exists intent_media_sandbox_production.PQ_mismapping_impressions1;

CREATE TABLE intent_media_sandbox_production.PQ_mismapping_impressions1 AS (
SELECT PQ_mismapping_adcalls.request_id AS request_id,
	   PQ_mismapping_adcalls.pub_hotel_id,
	   PQ_mismapping_adcalls.publisher_ID,
	   intent_media_log_data_production.impressions.request_id AS impression_req_id
FROM intent_media_sandbox_production.PQ_mismapping_adcalls
LEFT JOIN intent_media_log_data_production.impressions
ON intent_media_log_data_production.impressions.request_id = intent_media_sandbox_production.PQ_mismapping_adcalls.request_id);



/* put filtered impressions onto results */
drop table if exists intent_media_sandbox_production.PQ_mismapping_impressions2;

CREATE TABLE intent_media_sandbox_production.PQ_mismapping_impressions2 AS (
SELECT PQ1.*, fa.request_ID as filtered_request_ID
FROM intent_media_sandbox_production.PQ_mismapping_impressions1 as pq1
LEFT JOIN intent_media_log_data_production.filtered_advertisements fa
ON fa.request_id = PQ1.request_id
);



/* THE NEW MONEY QUERY */

---drop table if exists intent_media_sandbox_production.PQ_LMN_fill_rate;


SELECT pub_hotel_ID, publisher_ID, count(request_ID) as missed_ad_calls, avg(count_filtered) as Average_Filtered_Impression_Count
FROM    (
        select request_ID, publisher_ID, pub_hotel_ID, count(Filtered_request_ID) as count_filtered
        from intent_media_sandbox_production.PQ_mismapping_impressions2
        WHERE impression_req_id IS NULL
        GROUP BY request_ID, publisher_ID, pub_hotel_ID
        ) filt_count
Group BY pub_hotel_ID, publisher_ID
Having avg(count_filtered) <=2.5
ORDER BY missed_ad_calls desc, Average_Filtered_Impression_Count Asc

