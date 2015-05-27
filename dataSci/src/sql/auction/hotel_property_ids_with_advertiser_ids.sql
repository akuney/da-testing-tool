-- Get list of hotel_property_id, advertiser_id for all meta
-- Generates the file 'hotel_property_ids_with_advertiser_ids.csv' for use in auction simulation
SELECT
intent_targets.intent_id    intent_target_id,
aid_cid_adgid.advertiser_id advertiser_id
FROM     (
           SELECT    aid_cid.advertiser_id,
                     aid_cid.campaign_id,
                     ad_grp.id AS ad_group_id
           FROM      (
                       SELECT    advertiser_ids.advertiser_id AS advertiser_id,
                                 c.id                         AS campaign_id
                       FROM      (
                                     SELECT DISTINCT *
                                     FROM            (
                                                       SELECT    c.advertiser_id
                                                       FROM      (
                                                                    SELECT *
                                                                    FROM   intent_media_production.ad_groups ag
                                                                    JOIN
                                                                           (
                                                                              SELECT ad_group_id
                                                                              FROM   intent_media_sandbox_production.advertisements
                                                                              WHERE  advertisement_type = '"MetaAdvertisement"') ad
                                                                    ON     ag.id = ad.ad_group_id) adag
                                                       LEFT JOIN intent_media_production.campaigns c
                                                       ON        adag.campaign_id = c.id) aid
                                     LEFT JOIN       intent_media_production.entities e
                                     ON              aid.advertiser_id = e.id
                                     AND             last_auction_participation > Now() - interval '1 day') advertiser_ids
                       LEFT JOIN
                                 (
                                    SELECT *
                                    FROM   intent_media_production.campaigns) c
                       ON        advertiser_ids.advertiser_id = c.advertiser_id) aid_cid
           LEFT JOIN
                     (
                        SELECT *
                        FROM   intent_media_production.ad_groups) ad_grp
           ON        aid_cid.campaign_id = ad_grp.campaign_id) aid_cid_adgid
JOIN
          (
           SELECT id,
                   ad_group_id,
                   intent_id
           FROM   intent_media_production.intent_targets
           WHERE  intent_type = 'HotelProperty') intent_targets
ON   aid_cid_adgid.ad_group_id = intent_targets.ad_group_id
ORDER BY intent_target_id;