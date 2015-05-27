--- Aggregate query for GDS channel --
---Author : Sushmit Roy--
---Creation Date-- 2nd Jan 2015--
--Purpose Ananlysis for Bid Changes ---

SELECT
    publisher_performance_report_aggregations.aggregation_level_date_in_et,
    publisher_performance_report_aggregations.market_id,
    intent_media_markets.name as Market,
    publisher_performance_report_aggregations.advance_purchase_range_type,
    SUM(click_count)                                    AS Total_click_count,
    SUM(gross_actual_cpc_sum)                           AS Total_CPC,
    ROUND(SUM(gross_actual_cpc_sum)/SUM(click_count),2) AS Avg_CPC,
    SUM(effective_bid_sum)                              AS Total_Bid,
    SUM(auction_participant_count)                      AS Total_Participant,
    CASE
        WHEN (SUM(effective_bid_sum)/SUM(auction_participant_count)) < 1
        THEN 1
        ELSE (SUM(effective_bid_sum)/SUM(auction_participant_count))
    END AS Avg_Bid
FROM
    intent_media_production.publisher_performance_report_aggregations
LEFT JOIN
    intent_media_production.intent_media_markets_publisher_markets
ON
    publisher_performance_report_aggregations.market_id =
    intent_media_markets_publisher_markets.market_id
LEFT JOIN
    intent_media_production.intent_media_markets
ON
    intent_media_markets_publisher_markets.intent_media_market_id = intent_media_markets.id
WHERE
    ad_unit_page_type_id IN
    (
        SELECT
            id
        FROM
            intent_media_production.legal_page_types
        WHERE
            page_type LIKE '%GDS%')
AND click_count > 0E-8
AND auction_participant_count IS NOT NULL
AND auction_participant_count <> 0
GROUP BY
    publisher_performance_report_aggregations.aggregation_level_date_in_et,
    publisher_performance_report_aggregations.market_id,
    intent_media_markets.name,
    publisher_performance_report_aggregations.advance_purchase_range_type;
    
--- Aggregate query for OTA channel --
---Author : Sushmit Roy--
---Creation Date-- 2nd Jan 2015--
--Purpose Ananlysis for Bid Changes ---



SELECT
    publisher_performance_report_aggregations.aggregation_level_date_in_et,
    publisher_performance_report_aggregations.market_id,
    intent_media_markets.name as Market,
    publisher_performance_report_aggregations.advance_purchase_range_type,
    SUM(click_count)                                    AS Total_click_count,
    SUM(gross_actual_cpc_sum)                           AS Total_CPC,
    ROUND(SUM(gross_actual_cpc_sum)/SUM(click_count),2) AS Avg_CPC,
    SUM(effective_bid_sum)                              AS Total_Bid,
    SUM(auction_participant_count)                      AS Total_Participant,
    CASE
        WHEN (SUM(effective_bid_sum)/SUM(auction_participant_count)) < 0.25
        THEN 0.25
        ELSE (SUM(effective_bid_sum)/SUM(auction_participant_count))
    END AS Avg_Bid
FROM
    intent_media_production.publisher_performance_report_aggregations
LEFT JOIN
    intent_media_production.intent_media_markets_publisher_markets
ON
    publisher_performance_report_aggregations.market_id =
    intent_media_markets_publisher_markets.market_id
LEFT JOIN
    intent_media_production.intent_media_markets
ON
    intent_media_markets_publisher_markets.intent_media_market_id = intent_media_markets.id
WHERE
    ad_unit_page_type_id not IN
    (
        SELECT
            id
        FROM
            intent_media_production.legal_page_types
        WHERE
            page_type LIKE '%GDS%')
AND click_count > 0E-8
AND auction_participant_count IS NOT NULL
AND auction_participant_count <> 0
GROUP BY
    publisher_performance_report_aggregations.aggregation_level_date_in_et,
    publisher_performance_report_aggregations.market_id,
    intent_media_markets.name,
    publisher_performance_report_aggregations.advance_purchase_range_type
    ;


