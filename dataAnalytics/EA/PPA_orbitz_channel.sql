----7078268

------ First Query 
------ Basic Data Pull from Ad Calls, Impressions, Clicks

DELETE FROM  Intent_media_sandbox_production.EA_ppa_orbitz_channel_1;
INSERT INTO  Intent_media_sandbox_production.EA_ppa_orbitz_channel_1

DROP TABLE IF EXISTS Intent_media_sandbox_production.EA_ppa_orbitz_channel_1;

CREATE TABLE Intent_media_sandbox_production.EA_ppa_orbitz_channel_1 AS

       SELECT case WHEN publisher_price -  Min(advertiser_price) OVER (Partition BY a.request_ID) <= 1 AND
                        publisher_price -  MIN(advertiser_price) OVER (Partition BY a.request_ID) >= -1 THEN 1 ELSE 0 END as Pub_tie,
                        
             case WHEN publisher_price -  Min(advertiser_price) OVER (Partition BY a.request_ID) > 1 THEN 1 ELSE 0 END as Pub_loss,
             case WHEN publisher_price -  Min(advertiser_price) OVER (Partition BY a.request_ID) < -1 THEN 1 ELSE 0 END as Pub_win,
             a.*
        FROM
        (
        SELECT a.request_id, a.ad_unit_id, auction_position, 
        travelers, travel_date_end, travel_date_start, rooms, adults, children, a.requested_at_in_et,
         a.publisher_ID,  a.requested_at_date_in_et, advertiser_ID, a.site_currency, 
               case WHEN a.rank_in_page <= 5 THEN 'TOP 5' ELSE 'NOT TOP 5'END as rank_in_page,
               CASE WHEN hp.country IN ('United States', 'US', 'USA') THEN 'Domestic' 
                    WHEN hp.country is null  THEN 'Unknown' 
                    ELSE 'International' END As Dom_Intl,
                hotel_average_nightly_rate  as advertiser_price, publisher_hotel_price as publisher_price,      
             SUM(CASE WHEN c.fraudulent = TRUE THEN 0 ELSE ISNULL(c.actual_cpc,0) END) as revenue, 
             SUM(CASE WHEN c.fraudulent = FALSE and c.actual_cpc > 0 THEN 1 ELSE 0 END) as clicks, 
             'SERVED' as filter_cause_type, i.base_bid as bid,
             number_of_advertisers_in_auction, positions_filled
        FROM intent_media_log_data_production.ad_calls a
        LEFT OUTER JOIN intent_media_production.hotel_properties hp  ON a.hotel_property_id = hp.id
        INNER JOIN intent_media_log_data_production.impressions i on i.request_id = a.request_id
        LEFT OUTER JOIN Intent_media_log_data_production.clicks c on i.external_id = c.external_impression_id
        WHERE ad_unit_type = 'META' and a.ip_address_blacklisted = false and outcome_type = 'SERVED' 
        and i.ip_address_blacklisted = false  AND a.requested_at_date_in_et = '02/12/2015'  AND i.requested_at_date_in_et = '02/12/2015'
        AND   hotel_average_nightly_rate  > 0 AND publisher_hotel_price > 0 
        AND a.ad_unit_id = 129 AND Advertiser_id in (150060, 157259, 148684)
        GROUP BY a.request_id, a.ad_unit_id, a.publisher_id,
           a.requested_at_date_in_et, a.requested_at_in_et, advertiser_ID, a.site_currency, auction_position, positions_filled,
               CASE WHEN hp.country IN ('United States', 'US', 'USA') THEN 'Domestic' 
                    WHEN hp.country is null THEN 'Unknown' 
                    ELSE 'International' END,
             case WHEN a.rank_in_page <= 5 THEN 'TOP 5' ELSE 'NOT TOP 5'END, 
                hotel_average_nightly_rate, publisher_hotel_price, i.base_bid, number_of_advertisers_in_auction
                , travelers, travel_date_end, travel_date_start, rooms, adults, children, a.requested_at_in_et
                ) a;
                
GRANT ALL on Intent_media_sandbox_production.EA_ppa_orbitz_channel_1 to PUBLIC;
 
 

------ Second Query 
------ Ads in data for names, Ad Units, and Sites.
------ The first inner query does this for the Advertisers
------ The second inner query that is unioned is just pulling the publisher data, and adding it as a row as if it were another advertiser.  The is_publisher field distinguishes between Advertisers and Publishers
------ The Outer query determines for each request_id the winning_price, whether each price is a win or a tie, or a loss, and then simply creates an order rank for each row
------ A win is defined as a price beating all other prices by more then $1
------ Since I'm only able to pull the minimum price, for each row I can only determine if the prices is within a dollar of that price so also a win, but I don't know if other prices are also within a dollar
------ so I call it a win_or_tie, and will figure out which later

--DELETE FROM  Intent_media_sandbox_production.EA_ppa_orbitz_channel_2;
--INSERT INTO Intent_media_sandbox_production.EA_ppa_orbitz_channel_2

DROP TABLE IF EXISTS Intent_media_sandbox_production.EA_ppa_orbitz_channel_2;
CREATE TABLE Intent_media_sandbox_production.EA_ppa_orbitz_channel_2 AS

SELECT a.*, Min(advertiser_price) OVER (Partition BY request_ID, filter_cause_type) as winning_Price, 
case WHEN advertiser_price -  Min(advertiser_price) OVER (Partition BY request_ID, filter_cause_type) < 1 Then 1 ELSE 0 END as win_or_tie,
case WHEN advertiser_price -  Min(advertiser_price) OVER (Partition BY request_ID, filter_cause_type) < 1 Then 0 ELSE 1 END as loss,
RANK() OVER (PARTITION BY request_ID, filter_cause_type ORDER BY filter_cause_type, Advertiser) as torder

FROM 
        (
        
        --------Advertisers
        SELECT pub_win, pub_tie, pub_loss, filter_cause_type, auction_position, request_ID,  a.publisher_id,
        
          case
	    when e2.ad_display_name = 'Air Fast Tickets' then 'AirFastTickets'
	    when e2.ad_display_name = 'Amoma' then 'AMOMA.com'
	    when e2.ad_display_name = 'Bookit' then 'BookIt.com'
	    when e2.ad_display_name = 'Expedia' then 'Expedia Inc.'
	    when e2.ad_display_name = 'Hotwire' then 'Expedia Inc.' 
	    when e2.ad_display_name = 'Travelocity on Expedia' then 'Expedia Inc.' 
	    when e2.ad_display_name = 'Travelocity' then 'Expedia Inc.'		    
	    when e2.ad_display_name = 'Kayak Software Corporation' then 'KAYAK' 
	    when e2.ad_display_name = 'lastminute.com' then 'Sabre Holdings' 
	    when e2.ad_display_name = 'Orbitz' then 'Orbitz Worldwide, Inc.'
	    when e2.ad_display_name = 'Oversee' then 'Oversee.net'
	    when e2.ad_display_name = 'Hotels.com' then 'Expedia Inc.'
	    when e2.ad_display_name = 'Trivago' then 'Expedia Inc.'
	    when e2.ad_display_name = 'Cheaptickets' then 'Orbitz Worldwide, Inc.' 
	    when e2.ad_display_name = 'ebookers.com' then 'Orbitz Worldwide, Inc.'	
	    when e2.ad_display_name = 'ebookers' then 'Orbitz Worldwide, Inc.'		        	        
	    when e2.ad_display_name = 'Booking.com' then 'Priceline'			        	    
          else e2.ad_display_name end as Publisher,
          
          ---normalizes display names between Publishers and Advertisers which are different entity_IDs for the same company.  Removes capitals and .com
          CONCAT(LEFT(s.display_name,1) , LOWER(Right(s.display_name, length(s.display_name) -1))) as site, 
          ad.name as "Ad Unit",  a.ad_unit_id, lp.page_type,  a.requested_at_date_in_et, requested_at_in_et, a.site_currency,  a.rank_in_page, 
          revenue, clicks, bid, a.advertiser_id, Dom_Intl,
          REPLACE(CONCAT(LEFT( e1.ad_display_name,1), LOWER(Right(e1.ad_display_name, length( e1.ad_display_name) -1))),'.com','') as Advertiser, 
          advertiser_price, 0 as is_publisher, number_of_advertisers_in_auction, positions_filled,
          
          ----HARDCODED Publisher Region -- Need to update as new ad_units come online
          CASE WHEN ad.id in (131,130,89,187,184,173,129,206,207, 203, 188, 204) THEN 'US' 
             WHEN ad.id in (216,147,143,215) THEN 'UK'
             ELSE 'Unknown' END AS Publisher_Region,
         
          case
	    when e1.ad_display_name = 'Air Fast Tickets' then 'AirFastTickets'
	    when e1.ad_display_name = 'Amoma' then 'AMOMA.com'
	    when e1.ad_display_name = 'Bookit' then 'BookIt.com'
	    when e1.ad_display_name = 'Expedia' then 'Expedia Inc.'
	    when e1.ad_display_name = 'Hotwire' then 'Expedia Inc.' 
	    when e1.ad_display_name = 'Travelocity on Expedia' then 'Expedia Inc.' 
	    when e1.ad_display_name = 'Travelocity' then 'Expedia Inc.'		    
	    when e1.ad_display_name = 'Kayak Software Corporation' then 'KAYAK' 
	    when e1.ad_display_name = 'lastminute.com' then 'Sabre Holdings' 
	    when e1.ad_display_name = 'Orbitz' then 'Orbitz Worldwide, Inc.'
	    when e1.ad_display_name = 'Oversee' then 'Oversee.net'
	    when e1.ad_display_name = 'Hotels.com' then 'Expedia Inc.'
	    when e1.ad_display_name = 'Trivago' then 'Expedia Inc.'	
	    when e1.ad_display_name = 'Cheaptickets' then 'Orbitz Worldwide, Inc.' 	
	    when e1.ad_display_name = 'ebookers.com' then 'Orbitz Worldwide, Inc.'
	    when e1.ad_display_name = 'ebookers' then 'Orbitz Worldwide, Inc.'	    		   
	    when e1.ad_display_name = 'Booking.com' then 'Priceline'			    	         		    
          else e1.ad_display_name end as Cartel,
          travelers, travel_date_end, travel_date_start, rooms, adults, children
                         
                
        FROM Intent_media_sandbox_production.EA_ppa_orbitz_channel_1 a
        INNER JOIN intent_media_production.ad_units ad on ad.id = a.ad_unit_id
        INNER JOIN intent_media_production.sites s on ad.site_ID = s.ID
        INNER JOIN intent_media_production.legal_page_types lp on lp.id = ad.legal_page_type_id
        LEFT JOIN (SELECT id, case WHEN ad_display_name is null then name else ad_display_name END as ad_display_name from intent_media_production.entities) e1 on e1.ID = a.advertiser_ID
        LEFT JOIN (SELECT id, case WHEN ad_display_name is null then name else ad_display_name END as ad_display_name from intent_media_production.entities) e2 on e2.ID = a.publisher_id
        
        UNION
        
        --Publishers
        SELECT DISTINCT 0 as pub_win, 0 as pub_tie, 0 as pub_loss, 'SERVED' as filter_cause_type, 0 as auction_position, request_ID,  a.publisher_id,
        
        case
	  when e2.ad_display_name = 'Air Fast Tickets' then 'AirFastTickets'
	  when e2.ad_display_name = 'Amoma' then 'AMOMA.com'
	  when e2.ad_display_name = 'Bookit' then 'BookIt.com'
	  when e2.ad_display_name = 'Expedia' then 'Expedia Inc.'
	  when e2.ad_display_name = 'Hotwire' then 'Expedia Inc.'
	  when e2.ad_display_name = 'Travelocity on Expedia' then 'Expedia Inc.' 
	  when e2.ad_display_name = 'Travelocity' then 'Expedia Inc.'		  
	  when e2.ad_display_name = 'Kayak Software Corporation' then 'KAYAK' 
	  when e2.ad_display_name = 'lastminute.com' then 'Sabre Holdings' 
	  when e2.ad_display_name = 'Orbitz' then 'Orbitz Worldwide, Inc.'
	  when e2.ad_display_name = 'Oversee' then 'Oversee.net'
	  when e2.ad_display_name = 'Hotels.com' then 'Expedia Inc.'
	  when e2.ad_display_name = 'Trivago' then 'Expedia Inc.'
	  when e2.ad_display_name = 'Cheaptickets' then 'Orbitz Worldwide, Inc.'
	  when e2.ad_display_name = 'ebookers.com' then 'Orbitz Worldwide, Inc.'
	  when e2.ad_display_name = 'ebookers' then 'Orbitz Worldwide, Inc.'	  	
	  when e2.ad_display_name = 'Booking.com' then 'Priceline'			  		  	  	  
        else e2.ad_display_name end as Publisher, 
                        
        CONCAT(LEFT(s.display_name,1) , LOWER(Right(s.display_name, length(s.display_name) -1))) as site,
        ad.name as "Ad Unit",  a.ad_unit_id, lp.page_type, a.requested_at_date_in_et, requested_at_in_et, a.site_currency,a.rank_in_page, 
        0 as revenue, 0 as clicks, 0 as bid, a.publisher_id, Dom_Intl,
        REPLACE(CONCAT(LEFT(s.display_name,1) , LOWER(Right(s.display_name, length(s.display_name) -1))),'.com','') as Advertiser, 
        publisher_price as advertiser_price, 1 as is_publisher, number_of_advertisers_in_auction, positions_filled,
        
      ----HARDCODED Publisher Region -- Need to update as new ad_units come online
        CASE WHEN ad.id in (131,130,89,187,184,173,129,206,207, 203, 188, 204) THEN 'US' 
             WHEN ad.id in (216,147,143,215) THEN 'UK'
             ELSE 'Unknown' END AS Publisher_Region,       
                     
        case
	  when e2.ad_display_name = 'Air Fast Tickets' then 'AirFastTickets'
	  when e2.ad_display_name = 'Amoma' then 'AMOMA.com'
	  when e2.ad_display_name = 'Bookit' then 'BookIt.com'
	  when e2.ad_display_name = 'Expedia' then 'Expedia Inc.'
	  when e2.ad_display_name = 'Hotwire' then 'Expedia Inc.'
	  when e2.ad_display_name = 'Travelocity on Expedia' then 'Expedia Inc.' 
	  when e2.ad_display_name = 'Travelocity' then 'Expedia Inc.'		  
	  when e2.ad_display_name = 'Kayak Software Corporation' then 'KAYAK' 
	  when e2.ad_display_name = 'lastminute.com' then 'Sabre Holdings' 
	  when e2.ad_display_name = 'Orbitz' then 'Orbitz Worldwide, Inc.'
	  when e2.ad_display_name = 'Oversee' then 'Oversee.net'
	  when e2.ad_display_name = 'Hotels.com' then 'Expedia Inc.'
	  when e2.ad_display_name = 'Trivago' then 'Expedia Inc.'
	  when e2.ad_display_name = 'Cheaptickets' then 'Orbitz Worldwide, Inc.' 
	  when e2.ad_display_name = 'ebookers.com' then 'Orbitz Worldwide, Inc.'
	  when e2.ad_display_name = 'ebookers' then 'Orbitz Worldwide, Inc.'		  
          when e2.ad_display_name = 'Booking.com' then 'Priceline'		          		  		  	    
        else e2.ad_display_name end as Cartel,
        travelers, travel_date_end, travel_date_start, rooms, adults, children
        
        FROM Intent_media_sandbox_production.EA_ppa_orbitz_channel_1 a
        INNER JOIN intent_media_production.ad_units ad on ad.id = a.ad_unit_id
        INNER JOIN intent_media_production.sites s on ad.site_ID = s.ID
        INNER JOIN intent_media_production.legal_page_types lp on lp.id = ad.legal_page_type_id
        LEFT JOIN (SELECT id, case WHEN ad_display_name is null then name else ad_display_name END as ad_display_name from intent_media_production.entities) e1 on e1.ID = a.advertiser_ID
        LEFT JOIN (SELECT id, case WHEN ad_display_name is null then name else ad_display_name END as ad_display_name from intent_media_production.entities) e2 on e2.ID = a.publisher_id) a;


GRANT ALL on Intent_media_sandbox_production.EA_ppa_orbitz_channel_2 to PUBLIC;



------ Fourth Query 
------ Transpose each set into a single line
------ For each of the 5 sets of data created above, we want to transpose the rows into a single line for analysis reasons
------ query 1 starts with the first row for each of the 5 sets of data (torder = 1) 
------ Each subsequent query left joins on the additional rows within each of the five sets as determined by the meta_rank above.
------ The outer query simply cleans it all up and assigns numers to each of the 5 players


--DELETE FROM  Intent_media_sandbox_production.EA_ppa_orbitz_channel_4;
--INSERT INTO Intent_media_sandbox_production.EA_ppa_orbitz_channel_4

DROP TABLE IF EXISTS Intent_media_sandbox_production.EA_ppa_orbitz_channel_4;
CREATE TABLE Intent_media_sandbox_production.EA_ppa_orbitz_channel_4 AS

SELECT  a.request_ID, publisher, site, publisher_ID,  Publisher_Region, "Ad Unit", ad_unit_id,  page_type, requested_at_date_in_et, site_currency,  rank_in_page,  
        Dom_Intl, is_publisher_1, Advertiser_1, advertiser_id_1, advertiser_price_1, advertiser_1_revenue, advertiser_1_clicks, is_publisher_2, Advertiser_2, advertiser_id_2, 
        advertiser_price_2, advertiser_2_revenue, advertiser_2_clicks, is_publisher_3, Advertiser_3, advertiser_id_3, advertiser_price_3, advertiser_3_revenue, advertiser_3_clicks, is_publisher_4, Advertiser_4, 
        advertiser_id_4, advertiser_price_4, advertiser_4_revenue, advertiser_4_clicks,
        cartel_1, cartel_2, cartel_3, cartel_4,  a.winning_price, ru.runner_up, win_or_tie_1, isnull(win_or_tie_2,0) + isnull(win_or_tie_3,0) + isnull(win_or_tie_4,0) as other_win_or_tie, loss_1
FROM 
        
        ----query 1
        (SELECT request_ID, site, Publisher, publisher_ID,  Publisher_Region,  "Ad Unit", ad_unit_id,  page_type, requested_at_date_in_et, site_currency,  rank_in_page, Dom_Intl, cartel as cartel_1,
        is_publisher as is_publisher_1, Advertiser as Advertiser_1, advertiser_id as advertiser_id_1, advertiser_price as advertiser_price_1, revenue as advertiser_1_revenue, clicks as advertiser_1_clicks,
        win_or_tie as win_or_tie_1, loss as loss_1, winning_price
        FROM Intent_media_sandbox_production.EA_ppa_orbitz_channel_2
        WHERE Advertiser = 'Orbitz' ) a
        
        LEFT OUTER JOIN
        
        (SELECT request_ID, cartel as cartel_2,
        is_publisher as is_publisher_2, Advertiser as Advertiser_2, advertiser_id as advertiser_id_2, advertiser_price as advertiser_price_2, revenue as advertiser_2_revenue, clicks as advertiser_2_clicks,
        win_or_tie as win_or_tie_2, loss as loss_2
        FROM Intent_media_sandbox_production.EA_ppa_orbitz_channel_2
        WHERE Advertiser = 'Hotels') b
        
        ON a.request_ID = b.request_ID 
        
        LEFT OUTER JOIN
        
        (SELECT request_ID, cartel as cartel_3,
        is_publisher as is_publisher_3, Advertiser as Advertiser_3, advertiser_id as advertiser_id_3, advertiser_price as advertiser_price_3, revenue as advertiser_3_revenue, clicks as advertiser_3_clicks,
        win_or_tie as win_or_tie_3, loss as loss_3
        FROM Intent_media_sandbox_production.EA_ppa_orbitz_channel_2
         WHERE Advertiser = 'Expedia' ) c
        
        ON a.request_ID = c.request_ID 

        LEFT OUTER JOIN
        
        (SELECT request_ID, cartel as cartel_4,
        is_publisher as is_publisher_4, Advertiser as Advertiser_4, advertiser_id as advertiser_id_4, advertiser_price as advertiser_price_4, revenue as advertiser_4_revenue, clicks as advertiser_4_clicks,
        win_or_tie as win_or_tie_4, loss as loss_4
        FROM Intent_media_sandbox_production.EA_ppa_orbitz_channel_2
         WHERE Advertiser = 'Travelocity' ) d
        
        ON a.request_ID = d.request_ID
        
        LEFT OUTER JOIN
        
       (SELECT request_id, min(advertiser_price) as runner_up
        FROM Intent_media_sandbox_production.EA_ppa_orbitz_channel_2
        Where loss = 1 AND  filter_cause_type = 'SERVED'
        GROUP BY request_ID) ru
        
        ON a.request_ID = ru.request_ID      ;   
        
GRANT ALL on Intent_media_sandbox_production.EA_ppa_orbitz_channel_4 to PUBLIC;


SELECT COUNT(*) FROM (

SELECT  requested_at_date_in_et,
        CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie = 0 THEN 1 ELSE 0 END as WIN,
        CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie > 0 THEN 1 ELSE 0 END as TIE,                                            
        Loss_1 as LOSS,
        --- Advertiser_price_1, Advertiser_price_2, Advertiser_price_3, Advertiser_price_4,
        
        CASE WHEN ISNULL(advertiser_price_2,9999999) - ISNULL(advertiser_price_1,9999999) <= -1 THEN 1 ELSE 0 END as Hotels_Beats_Pub, 
        CASE WHEN ISNULL(advertiser_price_3,9999999) - ISNULL(advertiser_price_1,9999999) <= -1 THEN 1 ELSE 0 END as Expedia_Beats_Pub,
        CASE WHEN ISNULL(advertiser_price_4,9999999) - ISNULL(advertiser_price_1,9999999) <= -1 THEN 1 ELSE 0 END as Travelocity_Beats_Pub,
        
        CASE WHEN advertiser_price_2 is null THEN 0 ELSE 1 END as Hotels_Present,
        CASE WHEN advertiser_price_3 is null THEN 0 ELSE 1 END as Expedia_Present,
        CASE WHEN advertiser_price_4 is null THEN 0 ELSE 1 END as Travelocity_Present,        
        case WHEN (advertiser_price_2 is null AND advertiser_price_3 is null) OR (advertiser_price_2 is null AND advertiser_price_4 is null) OR (advertiser_price_3 is null AND advertiser_price_4 is null) THEN 'No Contest' 
             WHEN ISNULL(advertiser_price_2,9999999) - ISNULL(advertiser_price_3,9999999) <= -1 AND  ISNULL(advertiser_price_2,9999999) - ISNULL(advertiser_price_4,99999) <= -1 Then 'Hotels Win'
             WHEN ISNULL(advertiser_price_3,9999999) - ISNULL(advertiser_price_2,9999999) <= -1 AND  ISNULL(advertiser_price_3,9999999) - ISNULL(advertiser_price_4,9999999) <= -1 Then 'Expedia'
             WHEN ISNULL(advertiser_price_4,9999999) - ISNULL(advertiser_price_2,9999999) <= -1 AND  ISNULL(advertiser_price_4,9999999) - ISNULL(advertiser_price_3,9999999) <= -1 Then 'Travelocity Win' 
             ELSE 'No Win' END as Outcome, advertiser_price_1, advertiser_price_2, advertiser_price_3, advertiser_price_4, 
             request_ID, advertiser_1_revenue, advertiser_1_clicks, advertiser_2_revenue, advertiser_2_clicks, advertiser_3_revenue, advertiser_3_clicks, advertiser_4_revenue, advertiser_4_clicks, 
 
        (advertiser_price_1 - advertiser_price_2)/advertiser_price_2 as PRCT_Differences_1_2,
        (advertiser_price_1 - advertiser_price_3)/advertiser_price_3 as PRCT_Differences_1_3,
        (advertiser_price_1 - advertiser_price_4)/advertiser_price_4 as PRCT_Differences_1_4,
        
        (advertiser_price_2 - advertiser_price_1)/advertiser_price_1 as PRCT_Differences_2_1,
        (advertiser_price_2 - advertiser_price_3)/advertiser_price_3 as PRCT_Differences_2_3,
        (advertiser_price_2 - advertiser_price_4)/advertiser_price_4 as PRCT_Differences_2_4,     
        
        (advertiser_price_3 - advertiser_price_1)/advertiser_price_1 as PRCT_Differences_3_1,
        (advertiser_price_3 - advertiser_price_2)/advertiser_price_2 as PRCT_Differences_3_2,
        (advertiser_price_3 - advertiser_price_4)/advertiser_price_4 as PRCT_Differences_3_4,      
        
        (advertiser_price_4 - advertiser_price_1)/advertiser_price_1 as PRCT_Differences_4_1,
        (advertiser_price_4 - advertiser_price_2)/advertiser_price_2 as PRCT_Differences_4_2,
        (advertiser_price_4 - advertiser_price_3)/advertiser_price_3 as PRCT_Differences_4_3

FROM Intent_media_sandbox_production.EA_ppa_orbitz_channel_4

) a


GROUP BY 
        CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie = 0 THEN 1 ELSE 0 END ,
        CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie > 0 THEN 1 ELSE 0 END ,                                            
        Loss_1 ,
        --- Advertiser_price_1, Advertiser_price_2, Advertiser_price_3, Advertiser_price_4,

        CASE WHEN advertiser_price_2 is null THEN 0 ELSE 1 END ,
        CASE WHEN advertiser_price_3 is null THEN 0 ELSE 1 END ,
        CASE WHEN advertiser_price_4 is null THEN 0 ELSE 1 END ,  

        case WHEN (advertiser_price_2 is null AND advertiser_price_3 is null) OR (advertiser_price_2 is null AND advertiser_price_4 is null) OR (advertiser_price_3 is null AND advertiser_price_4 is null) THEN 'No Contest' 
             WHEN ISNULL(advertiser_price_2,9999999) - ISNULL(advertiser_price_3,9999999) <= -1 AND  ISNULL(advertiser_price_2,9999999) - ISNULL(advertiser_price_4,99999) <= -1 Then 'Hotels Win'
             WHEN ISNULL(advertiser_price_3,9999999) - ISNULL(advertiser_price_2,9999999) <= -1 AND  ISNULL(advertiser_price_3,9999999) - ISNULL(advertiser_price_4,9999999) <= -1 Then 'Expedia'
             WHEN ISNULL(advertiser_price_4,9999999) - ISNULL(advertiser_price_2,9999999) <= -1 AND  ISNULL(advertiser_price_4,9999999) - ISNULL(advertiser_price_3,9999999) <= -1 Then 'Travelocity Win' 
             ELSE 'No Win' END, requested_at_date_in_et
             
GRANT ALL on Intent_media_sandbox_production.EA_ppa_orbitz_channel_4 to PUBLIC;

SELECT *
FROM Intent_media_sandbox_production.EA_ppa_orbitz_channel_4

---Expedia Vs Travelocity
            SELECT CAST(advertiser_price_3 AS FLOAT), CAST(advertiser_price_4 AS FLOAT), CAST(PRCT_Differences_3_4 AS FLOAT), CAST(0 AS FLOAT) as dummy
            FROM Intent_media_sandbox_production.EA_ppa_orbitz_channel_all_02012015
            WHERE Expedia_Present = 1 AND travelocity_present = 1

            UNION
            SELECT CAST(AVG(advertiser_price_3) AS FLOAT), CAST(AVG(advertiser_price_4) AS FLOAT), CAST(AVG(PRCT_Differences_3_4) AS FLOAT), CAST(.05 AS FLOAT) as dummy
            FROM Intent_media_sandbox_production.EA_ppa_orbitz_channel_all_02012015
            WHERE Expedia_Present = 1 AND travelocity_present = 1



---Hotels Vs Expedia
SELECT CAST(advertiser_price_2 AS FLOAT), CAST(advertiser_price_3 AS FLOAT), CAST(PRCT_Differences_2_3 AS FLOAT), CAST(PRCT_Differences_3_2 AS FLOAT),
FROM Intent_media_sandbox_production.EA_ppa_orbitz_channel_all_02012015
WHERE Expedia_Present = 1 AND Hotels_present = 1


SELECT 
CAST(SUM(Case  WHEN abs(advertiser_price_2 - advertiser_price_3) > 1 THEN 1 ELSE 0 END) as float) as Differences, 
--CAST(SUM(Case  WHEN advertiser_price_2 - advertiser_price_3 < -1 THEN 1 ELSE 0 END) as float) as wins,  
cast( COUNT(*) as float) as pop
FROM Intent_media_sandbox_production.EA_ppa_orbitz_channel_all
WHERE Expedia_Present = 1 AND Hotels_present = 1


---Expedia vs Travelocity

SELECT 
CAST(SUM(Case  WHEN abs(advertiser_price_3 - advertiser_price_4) > 1 THEN 1 ELSE 0 END) as float) as Differences,  
CAST(SUM(Case  WHEN advertiser_price_4 - advertiser_price_3 < -1 THEN 1 ELSE 0 END) as float) as wins,
cast( COUNT(*) as float) as pop
FROM Intent_media_sandbox_production.EA_ppa_orbitz_channel_all
WHERE Expedia_Present = 1 AND Travelocity_present = 1

--- Test
SELECT cast( COUNT(*) as float) * .01 as Differences, cast( COUNT(*) as float) as pop
FROM Intent_media_sandbox_production.EA_ppa_orbitz_channel_all
WHERE Expedia_Present = 1 AND Hotels_present = 1


-------- Binary T Test
--Exp , Travel
SELECT 
Case  WHEN abs(advertiser_price_3 - advertiser_price_4) > 1 THEN 1 ELSE 0 END as Differences
FROM Intent_media_sandbox_production.EA_ppa_orbitz_channel_all
WHERE Expedia_Present = 1 AND Travelocity_present = 1



--- Main Summary Query
SELECT 
CASE WHEN WIN > 0 THEN 'WIN'
     WHEN TIE > 0 THEN 'TIE'
     WHEN Loss > 0 THEN 'LOSS' 
     ELSE 'ERROR' END as Orbitz, 
      Outcome, hotels_Beats_pub, Expedia_Beats_pub, Travelocity_Beats_pub, COUNT(request_id) as ad_calls, SUM(Advertiser_2_Clicks) as Hotels_clicks
FROM Intent_media_sandbox_production.EA_ppa_orbitz_channel_all
WHERE Hotels_present = 1 
GROUP BY  
CASE WHEN WIN > 0 THEN 'WIN'
     WHEN TIE > 0 THEN 'TIE'
     WHEN Loss > 0 THEN 'LOSS' 
     ELSE 'ERROR' END , 
     hotels_Beats_pub, Expedia_Beats_pub, Travelocity_Beats_pub, Outcome





--- Abbreviated Summary Query
SELECT 
CASE WHEN WIN > 0 THEN 'WIN'
     WHEN TIE > 0 THEN 'TIE'
     WHEN Loss > 0 THEN 'LOSS' 
     ELSE 'ERROR' END as Orbitz, 
      Outcome,  COUNT(request_id) as ad_calls, SUM(Advertiser_2_Clicks) as Hotels_clicks
FROM Intent_media_sandbox_production.EA_ppa_orbitz_channel_all
WHERE Hotels_present = 1 
GROUP BY  
CASE WHEN WIN > 0 THEN 'WIN'
     WHEN TIE > 0 THEN 'TIE'
     WHEN Loss > 0 THEN 'LOSS' 
     ELSE 'ERROR' END , 
   Outcome


--cartel only
SELECT 
      Outcome,  COUNT(request_id) as ad_calls, SUM(Advertiser_2_Clicks) as Hotels_clicks
FROM Intent_media_sandbox_production.EA_ppa_orbitz_channel_all
WHERE Hotels_present = 1 
GROUP BY  
   Outcome




--- Total Ad Calls and Clicks


SELECT SUM(ad_call_count) as ad_calls, sum(click_count) as clicks
FROM intent_media_production.hotel_meta_media_performance_aggregations
WHERE ad_unit_id  = 129 AND aggregation_level_date_in_et >= '02/01/2015' AND aggregation_level_date_in_et < '02/24/2015'
