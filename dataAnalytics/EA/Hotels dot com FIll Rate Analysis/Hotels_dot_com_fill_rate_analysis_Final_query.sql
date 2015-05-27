--- By Date
SELECT requested_at_date_in_et, 
---Current
SUM(case served_impressions WHEN 0 THEN 0 ELSE 1 END) as Current_Served_Ads,
COUNT(request_ID) as Current_Ad_Calls,  
SUM(case served_impressions WHEN 0 THEN 0 ELSE 1 END)/  COUNT(request_ID) AS Curent_Fill_Rate,
SUM(case WHEN served_impressions > 0 THEN served_impressions ELSE 0 END) as Current_Num_Prices_Shown,
SUM(case WHEN served_impressions > 0 THEN 4 ELSE 0 END) as Current_Available_Price_Slots,
SUM(case WHEN served_impressions > 0 THEN served_impressions ELSE 0 END) / SUM(case WHEN served_impressions > 0 THEN 4 ELSE 0 END) as Curr_Position_Fill_Rate_Served_Ads,

--If Hotels.com Droped House Brands
SUM(case WHEN served_impressions + unserved_Non_House_Brand_impressions - House_Brands_Served < 2 THEN 0 ELSE 1 END) as Proposed_Served_Ads,
COUNT(request_ID) as Current_Ad_Calls,  
SUM(case WHEN served_impressions + unserved_Non_House_Brand_impressions - House_Brands_Served < 2 THEN 0 ELSE 1 END)/  COUNT(request_ID) AS Proposed_Fill_Rate,

SUM(case WHEN served_impressions + unserved_Non_House_Brand_impressions - House_Brands_Served < 2 THEN 0 ELSE 
CASE WHEN served_impressions + unserved_Non_House_Brand_impressions - House_Brands_Served > 4 THEN 4 ELSE served_impressions + unserved_Non_House_Brand_impressions - House_Brands_Served  END
END) as Proposed_Num_Prices_Shown,

SUM(case WHEN served_impressions + unserved_Non_House_Brand_impressions - House_Brands_Served < 2 THEN 0 ELSE 4 END) as Proposed_Available_Price_Slots,

SUM(case WHEN served_impressions + unserved_Non_House_Brand_impressions - House_Brands_Served < 2 THEN 0 ELSE 
CASE WHEN served_impressions + unserved_Non_House_Brand_impressions - House_Brands_Served > 4 THEN 4 ELSE served_impressions + unserved_Non_House_Brand_impressions - House_Brands_Served  END
END) /
SUM(case WHEN served_impressions + unserved_Non_House_Brand_impressions - House_Brands_Served < 2 THEN 0 ELSE 4 END) 
as Proposed_Position_Fill_rate_Served_Ads

FROM Intent_media_sandbox_production.EA_ppa_hotels_fill_rate
GROUP By requested_at_date_in_et


---Overall

SELECT 
---Current
SUM(case served_impressions WHEN 0 THEN 0 ELSE 1 END) as Current_Served_Ads,
COUNT(request_ID) as Current_Ad_Calls,  
SUM(case served_impressions WHEN 0 THEN 0 ELSE 1 END)/  COUNT(request_ID) AS Curent_Fill_Rate,
SUM(case WHEN served_impressions > 0 THEN served_impressions ELSE 0 END) as Current_Num_Prices_Shown,
SUM(case WHEN served_impressions > 0 THEN 4 ELSE 0 END) as Current_Available_Price_Slots,
SUM(case WHEN served_impressions > 0 THEN served_impressions ELSE 0 END) / SUM(case WHEN served_impressions > 0 THEN 4 ELSE 0 END) as Curr_Position_Fill_Rate_Served_Ads,

--If Hotels.com Droped House Brands
SUM(case WHEN served_impressions + unserved_Non_House_Brand_impressions - House_Brands_Served < 2 THEN 0 ELSE 1 END) as Proposed_Served_Ads,
COUNT(request_ID) as Current_Ad_Calls,  
SUM(case WHEN served_impressions + unserved_Non_House_Brand_impressions - House_Brands_Served < 2 THEN 0 ELSE 1 END)/  COUNT(request_ID) AS Proposed_Fill_Rate,

SUM(case WHEN served_impressions + unserved_Non_House_Brand_impressions - House_Brands_Served < 2 THEN 0 ELSE 
CASE WHEN served_impressions + unserved_Non_House_Brand_impressions - House_Brands_Served > 4 THEN 4 ELSE served_impressions + unserved_Non_House_Brand_impressions - House_Brands_Served  END
END) as Proposed_Num_Prices_Shown,

SUM(case WHEN served_impressions + unserved_Non_House_Brand_impressions - House_Brands_Served < 2 THEN 0 ELSE 4 END) as Proposed_Available_Price_Slots,

SUM(case WHEN served_impressions + unserved_Non_House_Brand_impressions - House_Brands_Served < 2 THEN 0 ELSE 
CASE WHEN served_impressions + unserved_Non_House_Brand_impressions - House_Brands_Served > 4 THEN 4 ELSE served_impressions + unserved_Non_House_Brand_impressions - House_Brands_Served  END
END) /
SUM(case WHEN served_impressions + unserved_Non_House_Brand_impressions - House_Brands_Served < 2 THEN 0 ELSE 4 END) 
as Proposed_Position_Fill_rate_Served_Ads

FROM Intent_media_sandbox_production.EA_ppa_hotels_fill_rate


        ---Example of 2 or more Advertiser_minimum not met filtered impressions and no actuall impressions.  Can delete if figured out
        SELECT a.request_id, a.ad_unit_id, a.requested_at_date_in_et, i.advertiser_ID, a.site_currency, number_of_advertisers_in_auction,  i.ip_address_blacklisted , outcome_type, a.publisher_id, filter_cause_type,
        i.*
        FROM intent_media_log_data_production.ad_calls a
        LEFT OUTER JOIN intent_media_log_data_production.filtered_advertisements i on i.request_id = a.request_id
        WHERE ad_unit_type = 'META' and a.ip_address_blacklisted = false AND a.Publisher_ID = 109
        AND a.requested_at_date_in_et = to_date('01/06/2015','mm/dd/yyyy') AND i.request_ID = 'fbdb3d39-cd0f-47c8-9429-55109da22e90'
