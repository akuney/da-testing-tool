/* Main Select - from [users, imm, hpa, z, status_changes] */
/* Author :Sushmit Roy*  Story id : 78843182*/
/* Refer documentation https://docs.google.com/a/intentmedia.com/document/d/
1YwAY_fjGeP1H1kFw5GgjHXuSb4XAc6qnvZnY2aI1zOQ/edit*/
/* Changed query on Mar 24th 2015 to include suspended status */
SELECT
    users.*,
    (
        CASE
            WHEN users."User First Auction Participation" =
                status_changes."First Auction Participation"
            THEN 1
            ELSE 0
        END) AS "First Advertiser for User",
    (
        CASE
            WHEN users."User First Auction Participation" =
                hotel_property_status_changes."First Auction Participation"
            THEN 1
            ELSE 0
        END)                            AS "First Hotel Property for User",
    ifnull(imm.name, 'Other')           AS "Market Name",
    ifnull(imm.report_segment, 'Other') AS "Segment Name",
    hpa.hotel_property_id               AS "Hotel Property ID",
    z.can_serve_ads                     AS "Can Serve Ads",
    status_changes.Date,
    status_changes."Advertiser Name",
    status_changes."Sold Date",
    status_changes."Is New",
    status_changes."Advertising Status",
    status_changes."Budget Type",
    status_changes."Budget",
    status_changes."Previous Advertising Status",
    status_changes."Previous Budget Type",
    status_changes."Previous Budget",
    (
        CASE
            WHEN "Advertising Status" = "Previous Advertising Status"
            THEN 'No Change'
            WHEN "Is New"
            THEN (
                    CASE
                        WHEN users."Channel Status" = 'Latter Channel'
                        THEN 'New Channel'
                        ELSE 'Brand New Hotel'
                    END)
            WHEN "Advertising Status" LIKE '%Active%'
            AND "Previous Advertising Status" LIKE '%Paused%'
            THEN (
                    CASE
                        WHEN "Change Yesterday or Today"
                        THEN 'Manually Reactivated'
                        ELSE 'Reactivated No Traffic'
                    END)
            WHEN "Advertising Status" = 'Paused with Zeroed Out Non-Recurring Budget'
            AND "Previous Advertising Status" LIKE '%Active%'
            THEN 'Paused for Budget'
            WHEN "Advertising Status" = 'Manually Paused'
            AND "Previous Advertising Status" LIKE '%Active%'
            THEN 'Manually Paused'
            WHEN "Advertising Status" = 'Paused No Traffic'
            AND "Previous Advertising Status" LIKE '%Active%'
            THEN 'Paused No Traffic'
            WHEN "Advertising Status" = 'Suspended'
            AND "Previous Advertising Status" != 'Suspended'
            THEN 'Suspended'
            WHEN "Advertising Status" != 'Suspended'
            AND "Previous Advertising Status" = 'Suspended'
            THEN 'Manually Reactivated'
            ELSE 'No Change'
        END) AS "Advertising Status Change",
    (
        CASE
            WHEN hotel_property_status_changes."Hotel Property Advertising Status Value" > 0
            THEN 'Active'
            WHEN hotel_property_status_changes."Hotel Property Advertising Status Value" = 0
            THEN 'Paused'
        END) AS "Hotel Property Advertising Status",
    (
        CASE
            WHEN hotel_property_status_changes."Previous Hotel Property Advertising Status Value" >
                0
            THEN 'Active'
            WHEN hotel_property_status_changes."Previous Hotel Property Advertising Status Value" =
                0
            THEN 'Paused'
        END) AS "Previous Hotel Property Advertising Status",
    (
        CASE
            WHEN hotel_property_status_changes."Hotel Property Is New Value" > 0
            THEN 'New'
            WHEN hotel_property_status_changes."Hotel Property Advertising Status Change Value" < 0
            THEN 'Paused'
            WHEN hotel_property_status_changes."Hotel Property Advertising Status Change Value" > 0
            THEN 'Reactivated'
            ELSE 'No Change'
        END) AS "Hotel Property Advertising Status Change"
FROM
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    -------------START 1st main subquery [users]
    ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    (
        SELECT
            entities_to_users.*,
            (
                CASE
                    WHEN entities_to_users."User ID" = primary_users.primary_user
                    THEN 1
                    ELSE 0
                END) AS "Is Primary User",
            hotel_count."Distinct Hotel Properties",
            hotel_count."Distinct Entities",
            hotel_count."User First Auction Participation",
            hotel_count."Strategic Account Type",
            hotel_count."Known Rotator Type"
        FROM
            (
                SELECT
                    e.id                                 AS "Advertiser ID",
                    e.telephone                          AS "Phone Number",
                    (u.first_name || ' ' || u.last_name) AS "User Name",
                    u.email                              AS "User Email",
                    u.id                                 AS "User ID",
                    e.ssn_channel_type                   AS "SSN Channel Type",
                    channel_types."Channel Status"
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
                                END) AS "Channel Status"
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
                    m.entity_id ) primary_users
        ON
            entities_to_users."Advertiser ID" = primary_users.entity_id
        LEFT JOIN
            (
                SELECT
                    u.email "User Email",
                    COUNT(DISTINCT(hpa.hotel_property_id)) AS "Distinct Hotel Properties",
                    COUNT(DISTINCT(e.id))                  AS "Distinct Entities",
                    MIN(e.first_auction_participation at timezone 'UTC' at timezone
                    'America/New_York') AS "User First Auction Participation",
                    (
                        CASE
                            WHEN u.strategic_account = 1
                            THEN 'Strategic Accounts'
                            ELSE 'Other Accounts'
                        END) AS "Strategic Account Type",
                    (
                        CASE
                            WHEN u.known_property_rotator = 1
                            THEN 'Known Rotators'
                            ELSE 'Other Accounts'
                        END) AS "Known Rotator Type"
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
                    u.email,
                    (
                        CASE
                            WHEN u.strategic_account = 1
                            THEN 'Strategic Accounts'
                            ELSE 'Other Accounts'
                        END),
                    (
                        CASE
                            WHEN u.known_property_rotator = 1
                            THEN 'Known Rotators'
                            ELSE 'Other Accounts'
                        END) ) hotel_count
        ON
            entities_to_users."User Email" = hotel_count."User Email") users
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    -------------END 1st main subquery [users]
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
LEFT JOIN
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    -------------START 2nd main subquery [status_changes]
    -------------------------------------------------------------------------------------------------------------------------------------------------------------------
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    (
        -----------------This the scond part of the query  For definitions refer to SSN definition
        -- doc
        ----------
        ------------URL https://docs.google.com/a/intentmedia.com/document/d/
        -- 1YwAY_fjGeP1H1kFw5GgjHXuSb4XAc6qnvZnY2aI1zOQ/edit- Author Sushmit Roy Changed on 13th
        -- Feb 2015-------------------------
        SELECT
            status_change_each_previous_day.Date                          AS DATE,
            status_change_each_previous_day.'Advertiser ID'               AS "Advertiser ID",
            status_change_each_previous_day.'Advertiser Name'             AS "Advertiser Name",
            status_change_each_previous_day.'Hotel Property ID'           AS "Hotel Property ID",
            status_change_each_previous_day.'First Auction Participation' AS
            "First Auction Participation",
            status_change_each_previous_day.'Sold Date'          AS "Sold Date",
            status_change_each_previous_day.'Is New'             AS "Is New",
            status_change_each_previous_day.'Advertising Status' AS "Advertising Status",
            status_change_each_previous_day.'Budget Type'        AS"Budget Type",
            status_change_each_previous_day.'Budget'             AS "Budget",
            status_change_each_previous_day."Change Yesterday or Today",
            LAG(status_change_each_previous_day.'Advertising Status',1) OVER(PARTITION BY
            status_change_each_previous_day.'Advertiser ID' ORDER BY
            status_change_each_previous_day.Date)                  AS 'Previous Advertising Status',
            status_change_each_previous_day.'Previous Budget Type' AS "Previous Budget Type",
            status_change_each_previous_day.'Previous Budget'      AS "Previous Budget"
        FROM
            (
                /* Inner Query 4 : starts  */
                SELECT
                    status_change_each_day.date_each_day_et            AS DATE,
                    status_change_each_day.advertiser_id               AS "Advertiser ID",
                    status_change_each_day.name                        AS "Advertiser Name",
                    status_change_each_day.hotel_property_id           AS "Hotel Property ID",
                    status_change_each_day.first_auction_participation AS
                    "First Auction Participation",
                    DATE(NEW_TIME(status_change_each_day.first_auction_participation,'UTC','EST'))
                    AS "Sold Date",
                    CASE
                        WHEN (status_change_each_day.date_each_day_et <= DATE(NEW_TIME
                                (status_change_each_day.first_auction_participation,'UTC','EST')))
                        THEN 1
                        ELSE 0
                    END AS "Is New",
                    CASE
                        WHEN (status_change_each_day.pa_date IS NOT NULL)
                        OR  (status_change_each_day.date_each_day_et <= DATE(NEW_TIME
                                (status_change_each_day.first_auction_participation,'UTC','EST')))
                        THEN 'Active'
                        WHEN suspended_status_daily = 'Suspended_Status'
                        THEN 'Suspended'
                        WHEN (status_change_each_day.pa_date IS NULL)
                        AND (status_change_each_day.advertiser_budget_type ='MONTHLY'
                            OR  status_change_each_day.Last_Budget_Type ='MONTHLY') -- Review and
                            -- check if status != suspended
                        AND (status_change_each_day.advertiser_budget < 0.25
                            OR  status_change_each_day.Last_Budget < 0.25)
                        THEN 'Active with Zeroed Out Recurring Budget'
                        WHEN (status_change_each_day.pa_date IS NULL)
                        AND (status_change_each_day.advertiser_budget_type !='MONTHLY'
                            OR  status_change_each_day.Last_Budget_Type !='MONTHLY') -- Review and
                            -- check if status != suspended
                        AND (status_change_each_day.advertiser_budget < 0.25
                            OR  status_change_each_day.Last_Budget < 0.25)
                        THEN 'Paused with Zeroed Out Non-Recurring Budget'
                        WHEN (status_change_each_day.pa_date IS NULL)
                        AND (status_change_each_day.advertiser_budget >= 0.25
                            OR  status_change_each_day.Last_Budget >= 0.25)
                        AND (status_change_each_day.max_change_date >=
                                status_change_each_day.max_participation_date
                            OR  (status_change_each_day.max_change_date IS NOT NULL
                                AND status_change_each_day.max_participation_date IS NULL))
                        THEN 'Manually Paused'
                        WHEN (status_change_each_day.pa_date IS NULL)
                        AND (status_change_each_day.advertiser_budget >= 0.25
                            OR  status_change_each_day.Last_Budget >= 0.25)
                        AND ((status_change_each_day.max_change_date <
                                    status_change_each_day.max_participation_date )
                            OR  (status_change_each_day.max_change_date IS NULL
                                AND status_change_each_day.max_participation_date IS NOT NULL))
                        THEN 'Paused No Traffic'
                        ELSE 'Invalid Status'
                    END                                           AS "Advertising Status",
                    status_change_each_day.advertiser_budget_type AS "Budget Type",
                    status_change_each_day.advertiser_budget      AS "Budget",
                    CASE
                        WHEN (status_change_each_day.date_each_day_et =
                                status_change_each_day.max_change_date)
                        OR  (status_change_each_day.date_each_day_et =
                                (status_change_each_day.max_change_date + interval '1 day'))
                        THEN 1
                        ELSE 0
                    END AS "Change Yesterday or Today",
                    LAG(status_change_each_day.advertiser_budget,1) OVER(PARTITION BY
                    status_change_each_day.advertiser_id ORDER BY
                    status_change_each_day.date_each_day_et) AS "Previous Budget",
                    LAG(status_change_each_day.advertiser_budget_type,1) OVER(PARTITION BY
                    status_change_each_day.advertiser_id ORDER BY
                    status_change_each_day.date_each_day_et) AS "Previous Budget Type"
                FROM
                    (
                        /* Inner Query 3 : starts  */
                        SELECT
                            advertiser_each_day_table.*,
                            hpa.hotel_property_id ,
                            pa.advertiser_id                AS pa_advertiser_id,
                            pa.aggregation_level_date_in_et AS pa_date,
                            MAX(aggregation_level_date_in_et) OVER (PARTITION BY
                            advertiser_each_day_table.advertiser_id ORDER BY date_each_day_et ) AS
                                                                 max_participation_date,
                            max_date_calc_table.advertiser_id   AS max_date_calc_advertiser_id,
                            max_date_calc_table.date_in_et_hsac AS date_in_et_hsac,
                            MAX(date_in_et_hsac) OVER (PARTITION BY
                            advertiser_each_day_table.advertiser_id ORDER BY date_each_day_et ) AS
                                                          max_change_date,
                            budget_table.advertiser_id AS budget_advertiser_id ,
                            budget_table.date_in_et    AS budget_date_each_day_et,
                            budget_table.advertiser_budget,
                            budget_table.advertiser_budget_type,
                            LAST_VALUE(budget_table.advertiser_budget_type IGNORE NULLS ) OVER
                            (PARTITION BY advertiser_each_day_table.advertiser_id ORDER BY
                            date_each_day_et ) AS Last_Budget_Type,
                            LAST_VALUE(budget_table.advertiser_budget IGNORE NULLS ) OVER
                            (PARTITION BY advertiser_each_day_table.advertiser_id ORDER BY
                            date_each_day_et )                  AS Last_Budget,
                            suspend_status_change.advertiser_id AS
                            suspend_status_change_advertiser_id,
                            suspend_status_change.date_susp_day_est,
                            suspend_status_change.suspended_status,
                            LAST_VALUE(suspended_status IGNORE NULLS ) OVER (PARTITION BY
                            advertiser_each_day_table.advertiser_id ORDER BY date_each_day_et ) AS
                            suspended_status_daily
                        FROM
                            (
                                /* Inner Query 2 : starts */
                                SELECT
                                    id AS advertiser_id,
                                    name,
                                    first_auction_participation,
                                    DATE(slice_time) AS date_each_day_et
                                FROM
                                    (
                                        /* Inner Query 1 : starts */
                                        SELECT
                                            id,
                                            name,
                                            first_auction_participation,
                                            DATE(NEW_TIME(first_auction_participation,'GMT','EST'))
                                            AS date_interpol_field
                                        FROM
                                            intent_media_production.entities
                                        WHERE
                                            active = 1
                                        AND entity_type = 'HotelSsrAdvertiser'
                                        AND first_auction_participation IS NOT NULL
                                        UNION
                                        SELECT
                                            id,
                                            name,
                                            first_auction_participation,
                                            DATE(NEW_TIME(CURRENT_TIMESTAMP,'GMT','EST') -1) AS
                                            date_interpol_field
                                        FROM
                                            intent_media_production.entities
                                        WHERE
                                            active = 1
                                        AND entity_type = 'HotelSsrAdvertiser'
                                        AND first_auction_participation IS NOT NULL )
                                    advertiser_min_max_day_table
                                    /* Inner Query 1 : ends  */
                                    TIMESERIES slice_time AS '1 day' OVER (PARTITION BY id,name,
                                    first_auction_participation ORDER BY CAST (date_interpol_field
                                    AS TIMESTAMP)) )
                            /* Inner Query 2 : ends  */
                            advertiser_each_day_table
                            /* Purpose: advertiser_each_day_table for this to create rows for each
                            advertiser id begining first auction day to Current day -1*/
                            /* left join starts to get Hotel id */
                            /* not for all left joins its necessary that we have one row per day
                            per advertiser id else the left join leads to multple rows for a day
                            .The purpose is to check the daily change ,hence the maximun */
                        LEFT JOIN
                            intent_media_production.hotel_property_advertisers hpa
                        ON
                            hpa.hotel_ssr_advertiser_id = advertiser_each_day_table.advertiser_id
                            /* to get the Hotel  id */
                            /* left join starts to get participating advertiser on particular days
                            for max participating date refer doc*/
                        LEFT JOIN
                            (
                                SELECT
                                    aggregation_level_date_in_et ,
                                    advertiser_id
                                FROM
                                    intent_media_production.participating_advertisers
                                GROUP BY
                                    aggregation_level_date_in_et ,
                                    advertiser_id) pa
                        ON
                            (
                                advertiser_each_day_table.advertiser_id = pa.advertiser_id)
                        AND (
                                advertiser_each_day_table.date_each_day_et =
                                pa.aggregation_level_date_in_et)
                            /* left join starts to get max date change --For max_change_date refer
                            docs*/
                        LEFT JOIN
                            (
                                SELECT
                                    advertiser_id,
                                    DATE(NEW_TIME(created_at, 'UTC','America/New_York') ) AS
                                    date_in_et_hsac
                                FROM
                                    intent_media_production.hotel_ssr_advertiser_changes
                                WHERE
                                    change_type = 'Budget Changed'
                                OR  change_type = 'Bid Changed'
                                OR  (
                                        change_type = 'Campaign Status'
                                    AND new_settings = 'Paused')
                                    /*The Where condition is specified in the doc */
                                GROUP BY
                                    advertiser_id,
                                    DATE(NEW_TIME(created_at, 'UTC','America/New_York' )) )
                            max_date_calc_table
                        ON
                            (
                                advertiser_each_day_table.advertiser_id =
                                max_date_calc_table.advertiser_id )
                        AND (
                                advertiser_each_day_table.date_each_day_et =
                                max_date_calc_table.date_in_et_hsac)
                            /* left join starts to get budget type and  budget each day*/
                        LEFT JOIN
                            (
                                SELECT
                                    latest.date_in_et,
                                    latest.advertiser_id,
                                    hb.effective_budget AS advertiser_budget,
                                    hb.budget_type      AS advertiser_budget_type
                                FROM
                                    (
                                        SELECT
                                            date_in_et,
                                            advertiser_id,
                                            MAX(id) AS latest_id
                                        FROM
                                            intent_media_production.historical_budgets
                                        GROUP BY
                                            date_in_et,
                                            advertiser_id) latest
                                LEFT JOIN
                                    intent_media_production.historical_budgets hb
                                ON
                                    latest.latest_id = hb.id ) AS budget_table
                        ON
                            (
                                advertiser_each_day_table.advertiser_id =
                                budget_table.advertiser_id)
                        AND (
                                advertiser_each_day_table.date_each_day_et =
                                budget_table.date_in_et)
                            /* left join starts to get day the advertiser was susupended*/
                        LEFT JOIN
                            (
                                SELECT
                                    advertiser_id,
                                    date_susp_day_est,
                                    change_type ,
                                    old_settings,
                                    new_settings,
                                    CASE
                                        WHEN old_settings = 'False'
                                        AND new_settings = 'True'
                                        THEN 'Suspended_Status'
                                        WHEN old_settings = 'True'
                                        AND new_settings = 'False'
                                        THEN 'Unsuspended_Status'
                                        ELSE 'Invalid'
                                    END AS suspended_status
                                FROM
                                    (
                                        /*this inner query captures the last change for a
                                        particular advertiser : If there are multiple changes in a
                                        day only the last change is relevant */
                                        SELECT
                                            *,
                                            DATE (NEW_TIME(created_at ,'UTC','EST')) AS
                                            date_susp_day_est,
                                            RANK() OVER ( PARTITION BY advertiser_id , DATE
                                            (NEW_TIME (created_at ,'UTC','EST')) ORDER BY
                                            created_at DESC) AS RANK_DAY_STATUS
                                        FROM
                                            intent_media_production.hotel_ssr_advertiser_changes
                                        WHERE
                                            change_type = 'Suspended'
                                        ORDER BY
                                            advertiser_id,
                                            created_at) rank_susp_table_daily
                                WHERE
                                    RANK_DAY_STATUS=1 ) suspend_status_change
                        ON
                            (
                                advertiser_each_day_table.advertiser_id =
                                suspend_status_change.advertiser_id)
                        AND (
                                advertiser_each_day_table.date_each_day_et =
                                suspend_status_change.date_susp_day_est)
                            /*ORDER BY
                            advertiser_each_day_table.advertiser_id,
                            advertiser_each_day_table.date_each_day_et*/
                    )
                    /* Inner Query 3 : ends  */
                    status_change_each_day )
            /* Inner Query 4 : ends  */
            status_change_each_previous_day) status_changes
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    -------------END 2nd main subquery [status_changes]
    ---------------------------------------------------------------------------------------------------------------------------------------------------------------------
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
ON
    users."Advertiser ID" = status_changes."Advertiser ID"
LEFT JOIN
    intent_media_production.hotel_property_advertisers hpa
ON
    hpa.hotel_ssr_advertiser_id = users."Advertiser ID"
LEFT JOIN
    intent_media_production.intent_media_hotel_properties_markets imhpm
ON
    imhpm.hotel_property_id = hpa.hotel_property_id
LEFT JOIN
    intent_media_production.intent_media_markets imm
ON
    imm.id = imhpm.intent_media_market_id
LEFT JOIN
    intent_media_production.z_hotel_ssr_advertiser_status z
ON
    z.advertiser_id = users."Advertiser ID"
LEFT JOIN
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    -------------START 3rd main subquery [hotel_property_status_changes]
    ----------------------------------------------------------------------------------------------------------------------------------------------------
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    (
        SELECT
            DATE,
            "Hotel Property ID",
            MIN("First Auction Participation")       AS "First Auction Participation",
            MIN("Is New Value")                      AS "Hotel Property Is New Value",
            MAX("Advertising Status Value")          AS "Hotel Property Advertising Status Value",
            MAX("Previous Advertising Status Value") AS
            "Previous Hotel Property Advertising Status Value",
            SUM("Advertising Status Value" - "Previous Advertising Status Value") AS
            "Hotel Property Advertising Status Change Value"
        FROM
            (
                SELECT
                    sold.date_in_et                        AS DATE,
                    hpa.hotel_property_id                  AS "Hotel Property ID",
                    sold.advertiser_id                     AS "Advertiser ID",
                    sold.first_auction_participation_in_et AS "First Auction Participation",
                    (
                        CASE
                            WHEN sold.date_in_et = sold.sold_date_in_et
                            THEN 1
                            ELSE 0
                        END) AS "Is New Value",
                    (
                        CASE
                            WHEN pas.advertiser_id IS NOT NULL
                            THEN 1
                            WHEN pas.advertiser_id IS NULL
                            AND (budgets.effective_budget < 0.25
                                AND budgets.budget_type = 'MONTHLY')
                            THEN 1
                            WHEN pas.advertiser_id IS NULL
                            AND (budgets.effective_budget < 0.25
                                AND budgets.budget_type <> 'MONTHLY')
                            THEN 0
                            WHEN pas.advertiser_id IS NULL
                            AND budgets.effective_budget >= 0.25
                            THEN 0
                        END) AS "Advertising Status Value",
                    (
                        CASE
                            WHEN previous_pas.advertiser_id IS NOT NULL
                            THEN 1
                            WHEN previous_pas.advertiser_id IS NULL
                            AND (previous_budgets.effective_budget < 0.25
                                AND previous_budgets.budget_type = 'MONTHLY')
                            THEN 1
                            WHEN previous_pas.advertiser_id IS NULL
                            AND (previous_budgets.effective_budget < 0.25
                                AND previous_budgets.budget_type <> 'MONTHLY')
                            THEN 0
                            WHEN previous_pas.advertiser_id IS NULL
                            AND previous_budgets.effective_budget >= 0.25
                            THEN 0
                        END) AS "Previous Advertising Status Value"
                FROM
                    (
                        SELECT
                            *
                        FROM
                            (
                                /* dates */
                                SELECT DISTINCT
                                    (aggregation_level_date_in_et) AS date_in_et
                                FROM
                                    intent_media_production.participating_advertisers ) dates,
                            (
                                /* sold_hotels */
                                SELECT
                                    id   AS advertiser_id,
                                    name AS advertiser_name,
                                    (first_auction_participation at timezone 'UTC' at timezone
                                    'America/New_York') AS first_auction_participation_in_et,
                                    DATE(first_auction_participation at timezone 'UTC' at timezone
                                    'America/New_York') AS sold_date_in_et
                                FROM
                                    intent_media_production.entities
                                WHERE
                                    entity_type = 'HotelSsrAdvertiser'
                                AND active = 1
                                AND first_auction_participation IS NOT NULL ) sold_hotels
                        WHERE
                            sold_hotels.sold_date_in_et <= dates.date_in_et ) sold
                    /* get hotel property */
                LEFT JOIN
                    intent_media_production.hotel_property_advertisers hpa
                ON
                    hpa.hotel_ssr_advertiser_id = sold.advertiser_id
                    /* get all participating advertisers for that day */
                LEFT JOIN
                    (
                        SELECT
                            pa.aggregation_level_date_in_et,
                            pa.advertiser_id
                        FROM
                            intent_media_production.participating_advertisers pa
                        GROUP BY
                            pa.aggregation_level_date_in_et,
                            pa.advertiser_id ) pas
                ON
                    sold.date_in_et = pas.aggregation_level_date_in_et
                AND sold.advertiser_id = pas.advertiser_id
                    /* get all participating advertisers for the previous day */
                LEFT JOIN
                    (
                        SELECT
                            pa.aggregation_level_date_in_et,
                            pa.advertiser_id
                        FROM
                            intent_media_production.participating_advertisers pa
                        GROUP BY
                            pa.aggregation_level_date_in_et,
                            pa.advertiser_id ) previous_pas
                ON
                    sold.date_in_et = DATE(previous_pas.aggregation_level_date_in_et + interval
                    '1 day')
                AND sold.advertiser_id = previous_pas.advertiser_id
                    /* get all budgets for that day */
                LEFT JOIN
                    (
                        SELECT
                            latest.date_in_et,
                            latest.advertiser_id,
                            effective_budget,
                            budget_type
                        FROM
                            (
                                SELECT
                                    date_in_et,
                                    advertiser_id,
                                    MAX(id) AS latest_id
                                FROM
                                    intent_media_production.historical_budgets
                                GROUP BY
                                    date_in_et,
                                    advertiser_id ) latest
                        LEFT JOIN
                            intent_media_production.historical_budgets hb
                        ON
                            latest.date_in_et = hb.date_in_et
                        AND latest.advertiser_id = hb.advertiser_id
                        AND latest.latest_id = hb.id ) budgets
                ON
                    sold.date_in_et = budgets.date_in_et
                AND sold.advertiser_id = budgets.advertiser_id
                    /* get all budgets for the previous day */
                LEFT JOIN
                    (
                        SELECT
                            latest.date_in_et,
                            latest.advertiser_id,
                            effective_budget,
                            budget_type
                        FROM
                            (
                                SELECT
                                    date_in_et,
                                    advertiser_id,
                                    MAX(id) AS latest_id
                                FROM
                                    intent_media_production.historical_budgets
                                GROUP BY
                                    date_in_et,
                                    advertiser_id ) latest
                        LEFT JOIN
                            intent_media_production.historical_budgets hb
                        ON
                            latest.date_in_et = hb.date_in_et
                        AND latest.advertiser_id = hb.advertiser_id
                        AND latest.latest_id = hb.id ) previous_budgets
                ON
                    sold.date_in_et = DATE(previous_budgets.date_in_et + interval '1 day')
                AND previous_budgets.advertiser_id = sold.advertiser_id )
            hotel_status_changes_with_property
        GROUP BY
            DATE,
            "Hotel Property ID") hotel_property_status_changes
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    -------------END 3rd main subquery [hotel_property_status_changes]
    ------------------------------------------------------------------------------------------------------------------------------------------------------
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
ON
    status_changes.Date = hotel_property_status_changes.Date
AND status_changes."Hotel Property ID" = hotel_property_status_changes."Hotel Property ID"
AND (
        users."Channel Status" = 'Only Channel'
    OR  users."Channel Status" = 'First Channel')
WHERE
    status_changes.Date < DATE(CURRENT_TIMESTAMP at timezone 'UTC' at timezone 'America/New_York')
    --order by advertiser_id, Date desc