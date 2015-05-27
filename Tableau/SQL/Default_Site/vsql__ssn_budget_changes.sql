-- Author : SUSHMIT ROY
-- Tableau Report :  SSN budget Changes Report
-- Expected Number of Rows : 30000
-- Creation Date : 24th Dec 2014
-- This pulls data hotel_ssr_bugdet_changes. The left outer join with users table leads to multiple
-- rows .
-- Select Primary user 1 or Null for unique changes
-- Expected Increase per month : 500
-- Primary purpose is to Check the count of budget changes
SELECT
    hsac_change_data_daily.DATE AS Date,
    (
        CASE
            WHEN hsac_change_data_daily.Starting_Time < first_date.starting_change
            THEN 'Wizard'
            WHEN hsac_change_data_daily.Starting_Time = first_date.starting_change
            AND e.first_auction_participation IS NULL
            THEN 'Wizard'
            WHEN hsac_change_data_daily.Starting_Time = first_date.starting_change
            AND e.first_auction_participation IS NOT NULL
            THEN (
                    CASE
                        WHEN users.'Channel Status' = 'Latter Channel'
                        THEN 'New Channel'
                        ELSE 'Brand New Hotel'
                    END)
            ELSE 'Active'
        END)                            AS 'Account Status',
    e.name                              AS 'Advertiser Name',
    e.id                                AS 'Advertiser ID',
    e.ssn_channel_type                  AS 'SSN Channel Type',
    ifnull(imm.name, 'Other')           AS 'Market Name',
    ifnull(imm.report_segment, 'Other') AS 'Segment Name',
    FIRST_VALUE_BUDGET_DAY              AS 'Old Amount',
    LAST_VALUE_BUDGET_DAY               AS 'New Amount',
    FIRST_VALUE_BUDGET_TYPE             AS 'Old Budget Type',
    LAST_VALUE_BUDGET_TYPE              AS 'New Budget Type',
    users.'Phone Number',
    users.'User Name',
    users.'User Email',
    users.'Channel Status',
    users.'Is Primary User',
    users.'Distinct Hotel Properties',
    users.'Distinct Entities',
    users.'User First Auction Participation',
    users.'Strategic Account Type',
    users.'Known Rotator Type'
FROM
    (
        SELECT
            hsac_ranked.DATE,
            hsac_ranked.market_id,
            hsac_ranked.advertiser_id,
            MIN(hsac_ranked.Rank_ROWS_DAY)          AS Min_Row_Rank,
            MIN(hsac_ranked.FIRST_VALUE_TIME)       AS Starting_Time,
            MIN(hsac_ranked.FIRST_VALUE_BUDGET_DAY) AS FIRST_VALUE_BUDGET_DAY,
            MIN(FIRST_VALUE_BUDGET_TYPE)            AS FIRST_VALUE_BUDGET_TYPE,
            MAX(hsac_ranked.Rank_ROWS_DAY)          AS Last_Rank_day,
            MAX(hsac_ranked.LAST_VALUE_TIME)        AS LastChange_Time,
            MAX(hsac_ranked.LAST_VALUE_BUDGET_DAY)  AS LAST_VALUE_BUDGET_DAY,
            MAX(hsac_ranked.LAST_VALUE_BUDGET_TYPE) AS LAST_VALUE_BUDGET_TYPE
        FROM
            (
                SELECT
                    *,
                    DATE(NEW_TIME(hsac.created_at,'UTC','America/New_York')) AS DATE,
                    CASE
                        WHEN (old_settings IS NULL)
                        OR  (REPLACE((trim(SPLIT_PART(trim(old_settings),'$',2))),',','') ='')
                        THEN 0.00
                        ELSE CAST(REPLACE((trim(SPLIT_PART(trim(old_settings),'$',2))),',','') AS
                            FLOAT)
                    END AS 'Old Amount' ,
                    CASE
                        WHEN (new_settings IS NULL)
                        OR  (REPLACE((trim(SPLIT_PART(trim(new_settings),'$',2))),',','') ='')
                        THEN 0.00
                        ELSE CAST(REPLACE((trim(SPLIT_PART(trim(new_settings),'$',2))),',','') AS
                            FLOAT)
                    END AS 'New Amount' ,
                    CASE
                        WHEN change_type = 'Budget Changed'
                        THEN trim(SPLIT_PART(SPLIT_PART(trim(old_settings),',',1),':',2))
                        ELSE NULL
                    END AS 'Old Budget Type',
                    CASE
                        WHEN change_type = 'Budget Changed'
                        THEN trim(SPLIT_PART(SPLIT_PART(trim(new_settings),',',1),':',2))
                        ELSE NULL
                    END AS 'New Budget Type',
                    RANK() OVER ( PARTITION BY DATE(hsac.created_at) , hsac.market_id ,
                    hsac.advertiser_id ORDER BY hsac.created_at ASC ) AS Rank_ROWS_DAY,
                    FIRST_VALUE(hsac.created_at) OVER ( PARTITION BY DATE(hsac.created_at) ,
                    hsac.market_id , hsac.advertiser_id ORDER BY hsac.created_at ASC ) AS
                    FIRST_VALUE_TIME,
                    FIRST_VALUE(
                        CASE
                            WHEN (old_settings IS NULL)
                            OR  (REPLACE((trim(SPLIT_PART(trim(old_settings),'$',2))),',','') ='')
                            THEN 0.00
                            ELSE CAST(REPLACE((trim(SPLIT_PART(trim(old_settings),'$',2))),',','')
                                AS FLOAT)
                        END ) OVER ( PARTITION BY DATE(hsac.created_at) , hsac.market_id ,
                    hsac.advertiser_id ORDER BY hsac.created_at ASC ) AS FIRST_VALUE_BUDGET_DAY,
                    FIRST_VALUE (
                        CASE
                            WHEN change_type = 'Budget Changed'
                            THEN trim(SPLIT_PART(SPLIT_PART(trim(old_settings),',',1),':',2))
                            ELSE NULL
                        END ) OVER ( PARTITION BY DATE(hsac.created_at) , hsac.market_id ,
                    hsac.advertiser_id ORDER BY hsac.created_at ASC ) AS FIRST_VALUE_BUDGET_TYPE,
                    LAST_VALUE(hsac.created_at) OVER ( PARTITION BY DATE(hsac.created_at) ,
                    hsac.market_id , hsac.advertiser_id ORDER BY hsac.created_at ASC ROWS BETWEEN
                    UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS LAST_VALUE_TIME,
                    LAST_VALUE(
                        CASE
                            WHEN (new_settings IS NULL)
                            OR  (REPLACE((trim(SPLIT_PART(trim(new_settings),'$',2))),',','') ='')
                            THEN 0.00
                            ELSE CAST(REPLACE((trim(SPLIT_PART(trim(new_settings),'$',2))),',','')
                                AS FLOAT)
                        END ) OVER ( PARTITION BY DATE(hsac.created_at) , hsac.market_id ,
                    hsac.advertiser_id ORDER BY hsac.created_at ASC ROWS BETWEEN UNBOUNDED
                    PRECEDING AND UNBOUNDED FOLLOWING) AS LAST_VALUE_BUDGET_DAY,
                    LAST_VALUE(
                        CASE
                            WHEN change_type = 'Budget Changed'
                            THEN trim(SPLIT_PART(SPLIT_PART(trim(new_settings),',',1),':',2))
                            ELSE NULL
                        END ) OVER ( PARTITION BY DATE(hsac.created_at) , hsac.market_id ,
                    hsac.advertiser_id ORDER BY hsac.created_at ASC ROWS BETWEEN UNBOUNDED
                    PRECEDING AND UNBOUNDED FOLLOWING) AS LAST_VALUE_BUDGET_TYPE
                FROM
                    intent_media_production.hotel_ssr_advertiser_changes hsac
                WHERE
                    change_type = 'Budget Changed'
                AND DATE(NEW_TIME(hsac.created_at,'UTC','America/New_York')) < DATE(NEW_TIME(CURRENT_TIMESTAMP,'UTC','America/New_York'))
                    /*ORDER BY
                    DATE(hsac.created_at) DESC ,
                    hsac.market_id ,
                    hsac.advertiser_id*/
            ) hsac_ranked
        GROUP BY
            hsac_ranked.DATE,
            hsac_ranked.market_id,
            hsac_ranked.advertiser_id
            /*    hsac_ranked.DATE desc,
            hsac_ranked.market_id,
            hsac_ranked.advertiser_id*/
    ) hsac_change_data_daily
LEFT JOIN
    intent_media_production.entities e
ON
    e.id = hsac_change_data_daily.advertiser_id
LEFT JOIN
    intent_media_production.hotel_property_advertisers hpa
ON
    hpa.hotel_ssr_advertiser_id = hsac_change_data_daily.advertiser_id
LEFT JOIN
    intent_media_production.intent_media_hotel_properties_markets imhpm
ON
    imhpm.hotel_property_id = hpa.hotel_property_id
LEFT JOIN
    intent_media_production.intent_media_markets imm
ON
    imm.id = imhpm.intent_media_market_id
LEFT JOIN
    (
        SELECT
            advertiser_id,
            MAX(hsac.created_at) AS starting_change
        FROM
            intent_media_production.hotel_ssr_advertiser_changes hsac
        LEFT JOIN
            intent_media_production.entities e
        ON
            e.id = hsac.advertiser_id
        WHERE
            (
                hsac.created_at <= e.first_auction_participation
            OR  e.first_auction_participation IS NULL)
        AND hsac.change_type = 'Budget Changed'
        GROUP BY
            advertiser_id ) first_date
ON
    first_date.advertiser_id = hsac_change_data_daily.advertiser_id
LEFT JOIN
    (
        SELECT
            entities_to_users.*,
            (
                CASE
                    WHEN entities_to_users.'User ID' = primary_users.primary_user
                    THEN 1
                    ELSE 0
                END) AS 'Is Primary User',
            hotel_count.'Distinct Hotel Properties',
            hotel_count.'Distinct Entities',
            hotel_count.'User First Auction Participation',
            hotel_count.'Strategic Account Type',
            hotel_count.'Known Rotator Type'
        FROM
            (
                SELECT
                    e.id                           AS 'Advertiser ID',
                    e.telephone                    AS 'Phone Number',
                    u.first_name||' '||u.last_name AS 'User Name',
                    u.email                        AS 'User Email',
                    u.id                           AS 'User ID',
                    e.ssn_channel_type             AS 'SSN Channel Type',
                    channel_types.'Channel Status'
                FROM
                    intent_media_production.entities e
                LEFT JOIN
                    (
                        SELECT
                            e.id AS advertiser_id,
                            (
                                CASE
                                    WHEN multi_channel_hotel_properties.hotel_property_id IS NULL
                                    THEN 'Only Channel'
                                    WHEN
                                        multi_channel_hotel_properties.min_first_auction_participation
                                        = e.first_auction_participation
                                    THEN 'First Channel'
                                    ELSE 'Latter Channel'
                                END) AS 'Channel Status'
                        FROM
                            intent_media_production.entities e
                        LEFT JOIN
                            intent_media_production.hotel_property_advertisers hpa
                        ON
                            hpa.hotel_ssr_advertiser_id = e.id
                        LEFT JOIN
                            (
                                SELECT
                                    hpa.hotel_property_id,
                                    MIN(first_auction_participation) AS
                                    min_first_auction_participation
                                FROM
                                    intent_media_production.hotel_property_advertisers hpa
                                LEFT JOIN
                                    intent_media_production.entities e
                                ON
                                    hpa.hotel_ssr_advertiser_id = e.id
                                WHERE
                                    e.active = 1
                                AND e.entity_type = 'HotelSsrAdvertiser'
                                GROUP BY
                                    hpa.hotel_property_id
                                HAVING
                                    COUNT(e.id) > 1 ) multi_channel_hotel_properties
                        ON
                            multi_channel_hotel_properties.hotel_property_id =
                            hpa.hotel_property_id
                        WHERE
                            e.entity_type = 'HotelSsrAdvertiser'
                        AND e.active = 1 ) channel_types
                ON
                    channel_types.advertiser_id = e.id
                RIGHT JOIN
                    intent_media_production.memberships m
                ON
                    m.entity_id = e.id
                RIGHT JOIN
                    intent_media_production.users u
                ON
                    u.id = m.user_id
                WHERE
                    entity_type = 'HotelSsrAdvertiser'
                AND e.active = 1
                AND e.first_auction_participation IS NOT NULL
                AND m.active = 1 ) entities_to_users
        LEFT JOIN
            (
                SELECT
                    m.entity_id  AS entity_id,
                    MIN(user_id) AS primary_user
                FROM
                    intent_media_production.memberships m
                WHERE
                    m.active = 1
                GROUP BY
                    m.entity_id) primary_users
        ON
            entities_to_users.'Advertiser ID' = primary_users.entity_id
        LEFT JOIN
            (
                SELECT
                    u.email                                                         AS 'User Email',
                    COUNT(DISTINCT (hpa.hotel_property_id))          AS 'Distinct Hotel Properties',
                    COUNT(DISTINCT (e.id))                                   AS 'Distinct Entities',
                    MIN(NEW_TIME(e.first_auction_participation,'UTC','America/New_York')) AS
                    'User First Auction Participation',
                    (
                        CASE
                            WHEN SUM(CAST (u.strategic_account AS INTEGER))> 0
                            THEN 'Strategic Accounts'
                            ELSE 'Other Accounts'
                        END) AS 'Strategic Account Type',
                    (
                        CASE
                            WHEN SUM (CAST(u.known_property_rotator AS INTEGER)) > 0
                            THEN 'Known Rotators'
                            ELSE 'Other Accounts'
                        END) AS 'Known Rotator Type'
                FROM
                    intent_media_production.users u
                LEFT JOIN
                    intent_media_production.memberships m
                ON
                    m.user_id = u.id
                LEFT JOIN
                    intent_media_production.entities e
                ON
                    e.id = m.entity_id
                LEFT JOIN
                    intent_media_production.hotel_property_advertisers hpa
                ON
                    hpa.hotel_ssr_advertiser_id = e.id
                WHERE
                    e.entity_type = 'HotelSsrAdvertiser'
                AND e.active = 1
                AND m.active = 1
                GROUP BY
                    (u.first_name||' '||u.last_name),
                    u.email ) hotel_count
        ON
            entities_to_users.'User Email' = hotel_count.'User Email') users
ON
    users.'Advertiser ID' = hsac_change_data_daily.advertiser_id;