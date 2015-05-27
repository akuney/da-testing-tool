SELECT bids.advertiser_id         AS advertiser_id,
       bids.advertiser_name       AS advertiser_name, 
       Avg(bids.effective_bid)    AS avg_effective_bid, 
       Stddev(bids.effective_bid) AS stddev_effective_bid 
FROM   (SELECT id 
        FROM   intent_media_production.ad_units 
        WHERE  NAME LIKE '%Hotel%' 
               AND ad_type = 'CT' 
               AND active = 1) ad_units 
       LEFT JOIN (SELECT ent.NAME AS advertiser_name, 
                         impr.* 
                  FROM   (SELECT advertiser_id, 
                                 ad_unit_id, 
                                 effective_bid 
                          FROM   intent_media_log_data_production.impressions 
                          WHERE  requested_at_date_in_et BETWEEN 
                                 '2014-10-01' AND '2014-10-31'
                                 AND ip_address_blacklisted = false) impr 
                         JOIN intent_media_production.entities ent 
                           ON ent.id = impr.advertiser_id) bids 
              ON ad_units.id = bids.ad_unit_id
              WHERE advertiser_id IS NOT NULL
GROUP  BY advertiser_id, 
          advertiser_name
ORDER BY avg_effective_bid DESC;