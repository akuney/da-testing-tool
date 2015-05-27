

        
-------------------------------
        

SELECT ad_unit_type, imp.advertiser_id, SUM(cl.actual_cpc) as Revenue
FROM Intent_media_log_data_production.ad_Calls ad
LEFT JOIN Intent_media_log_data_production.clicks cl
on ad.request_ID = cl.ad_Call_Request_id 
LEFT JOIN   Intent_media_log_data_production.Impressions imp
ON imp.external_id = cl.external_impression_ID
WHERE   ad.requested_at BETWEEN '2014/06/01' AND '2014/06/30' AND
        ad.ip_address_blacklisted = 0 AND
        ad.site_id = 2 AND
        cl.fraudulent = false AND
        cl.ip_address_blacklisted = false AND
        imp.ip_address_blacklisted = False AND
        imp.advertiser_id = 71265 
        ---ad_unit_type = 'SSR'
 GROUP BY imp.advertiser_ID, ad_unit_Type


 --1864 With reqeusted at
 
 --1974.74
 
 --1856.00
SELECT SUM(actual_cpc)
FROM Intent_media_log_data_production.clicks cl
WHERE   cl.fraudulent = false AND
        cl.ip_address_blacklisted = false AND 
        ad_call_requested_at BETWEEN '2014/06/01' AND '2014/07/01' AND
        requested_at BETWEEN '2014/06/01' AND '2014/08/30' AND
        SITE_TYPE = 'ORBITZ_GLOBAL' AND
        product_category_type = 'HOTELS' AND
        
advertisement_id in (
328833,
1303540,
1376536,
1376546,
1412977,
1413024,
1413067,
1413090,
1413128,
1413150,
1413194,
1413202,
2673775,
2673804,
2673817,
2673845,
2673846,
2673865,
2673906,
2673908,
4141926,
4555473,
4555521,
4555550,
4555553,
4555580,
4555604,
4555642,
4555664,
4898647,
4898678
)

