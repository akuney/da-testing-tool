
import numpy as np
import pandas as pd
import vertica_python as vp
import datetime
import sys


def create_connection():
    conn = vp.connect({
        'host': 'production-vertica-cluster-with-failover.internal.intentmedia.net',
        'port': 5433,
        'user': 'Tableau',
        'password': '9WOGfffN',
        'database': 'intent_media',
        'option': 'ConnectionLoadBalance=1; autocommit = true'
    })

    return conn

 # to_date('""" + strmonth + strday + stryear + """','mm/dd/yyyy')

def do_analysis1(stryear, strmonth, strday):

    print "infunction_1"
    print strmonth + strday + stryear

    conn = create_connection()
    cur = conn.cursor()
    sql_string = """ 

      DROP TABLE IF EXISTS  Intent_media_sandbox_production.EA_ppa_competition_stage_1;
      CREATE TABLE Intent_media_sandbox_production.EA_ppa_competition_stage_1
       AS


              SELECT a.request_id, a.ad_unit_id, a.product_category_type, 
               a.publisher_ID,  a.requested_at_date_in_et, advertiser_ID, a.site_currency, 
                     case WHEN a.rank_in_page <= 5 THEN 'TOP 5' ELSE 'NOT TOP 5'END as rank_in_page,
                     CASE WHEN hp.country IN ('United States', 'US', 'USA') THEN 'Domestic' 
                          WHEN hp.country is null  THEN 'Unknown' 
                          ELSE 'International' END As Dom_Intl,
                   CAST(CASE WHEN is_per_person_rate = TRUE AND is_per_night_rate = FALSE THEN hotel_average_nightly_rate/travelers/(travel_date_end - travel_date_start)
                   WHEN is_per_person_rate = TRUE AND is_per_night_rate = TRUE THEN hotel_average_nightly_rate/travelers
                   WHEN is_per_person_rate = FALSE AND is_per_night_rate = FALSE THEN hotel_average_nightly_rate/(travel_date_end - travel_date_start)
                   WHEN is_per_person_rate = FALSE AND is_per_night_rate = TRUE THEN hotel_average_nightly_rate END as float) as advertiser_price,
                   CAST(CASE WHEN is_per_person_rate = TRUE AND is_per_night_rate = FALSE THEN publisher_hotel_price/travelers/(travel_date_end - travel_date_start)
                   WHEN is_per_person_rate = TRUE AND is_per_night_rate = TRUE THEN publisher_hotel_price/travelers
                   WHEN is_per_person_rate = FALSE AND is_per_night_rate = FALSE THEN publisher_hotel_price/(travel_date_end - travel_date_start)
                   WHEN is_per_person_rate = FALSE AND is_per_night_rate = TRUE THEN publisher_hotel_price END as float) as publisher_price,      
                   SUM(ISNULL(c.actual_cpc,0)) as revenue, count(c.actual_cpc) as clicks
              FROM intent_media_log_data_production.ad_calls a
              LEFT OUTER JOIN intent_media_production.hotel_properties hp  ON a.hotel_property_id = hp.id
              INNER JOIN intent_media_log_data_production.impressions i on i.request_id = a.request_id
              LEFT OUTER JOIN Intent_media_log_data_production.clicks c on i.external_id = c.external_impression_id
              WHERE ad_unit_type = 'META' and a.ip_address_blacklisted = false and outcome_type = 'SERVED' 
              and i.ip_address_blacklisted = false         
                      AND hotel_average_nightly_rate > 0 
                      and publisher_hotel_price > 0 AND a.requested_at_date_in_et = to_date('""" + strmonth + strday + stryear + """','mm/dd/yyyy')
                      AND (c.fraudulent = 'false' or c.fraudulent is null) AND (c.ip_address_blacklisted ='false' OR c.ip_address_blacklisted is null)
              GROUP BY a.request_id, a.ad_unit_id, a.product_category_type, a.publisher_id,
                 a.requested_at_date_in_et, advertiser_ID, a.site_currency,
                     CASE WHEN hp.country IN ('United States', 'US', 'USA') THEN 'Domestic' 
                          WHEN hp.country is null THEN 'Unknown' 
                          ELSE 'International' END,
                   case WHEN a.rank_in_page <= 5 THEN 'TOP 5' ELSE 'NOT TOP 5'END, 
                   CAST(CASE WHEN is_per_person_rate = TRUE AND is_per_night_rate = FALSE THEN hotel_average_nightly_rate/travelers/(travel_date_end - travel_date_start)
                   WHEN is_per_person_rate = TRUE AND is_per_night_rate = TRUE THEN hotel_average_nightly_rate/travelers
                   WHEN is_per_person_rate = FALSE AND is_per_night_rate = FALSE THEN hotel_average_nightly_rate/(travel_date_end - travel_date_start)
                   WHEN is_per_person_rate = FALSE AND is_per_night_rate = TRUE THEN hotel_average_nightly_rate END as float),
                                CAST(CASE WHEN is_per_person_rate = TRUE AND is_per_night_rate = FALSE THEN publisher_hotel_price/travelers/(travel_date_end - travel_date_start)
                   WHEN is_per_person_rate = TRUE AND is_per_night_rate = TRUE THEN publisher_hotel_price/travelers
                   WHEN is_per_person_rate = FALSE AND is_per_night_rate = FALSE THEN publisher_hotel_price/(travel_date_end - travel_date_start)
                   WHEN is_per_person_rate = FALSE AND is_per_night_rate = TRUE THEN publisher_hotel_price END as float);

      GRANT ALL on Intent_media_sandbox_production.EA_ppa_competition_stage_1 to PUBLIC

                            """
    cur.execute(sql_string)
    conn.close()
       
def do_analysis2(stryear, strmonth, strday):

    print "infunction_2"
    print strmonth + strday + stryear

    conn = create_connection()
    cur = conn.cursor()
    sql_string = """ 

      --------- CREATE a row for publisherID and then rank each row by Advertiser

      DROP TABLE IF EXISTS  Intent_media_sandbox_production.EA_ppa_competition_stage_2;
      CREATE TABLE Intent_media_sandbox_production.EA_ppa_competition_stage_2
      AS


      SELECT a.*, Min(advertiser_price) OVER (Partition BY request_ID) as winning_Price, 
      case WHEN advertiser_price -  Min(advertiser_price) OVER (Partition BY request_ID) < 1 Then 1 ELSE 0 END as win_or_tie,
      case WHEN advertiser_price -  Min(advertiser_price) OVER (Partition BY request_ID) < 1 Then 0 ELSE 1 END as loss,
      RANK() OVER (PARTITION BY request_ID ORDER BY Advertiser) as torder

      FROM 

              (SELECT request_ID,  a.publisher_id,
              
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
                ad.name as "Ad Unit",  a.ad_unit_id, lp.page_type,  a.requested_at_date_in_et, a.product_category_type, a.site_currency,  a.rank_in_page, 
                revenue, clicks, a.advertiser_id, Dom_Intl,
                REPLACE(CONCAT(LEFT( e1.ad_display_name,1), LOWER(Right(e1.ad_display_name, length( e1.ad_display_name) -1))),'.com','') as Advertiser, 
                advertiser_price, 0 as is_publisher,
               
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
                else e1.ad_display_name end as Cartel
                               
                      
              FROM Intent_media_sandbox_production.EA_ppa_competition_stage_1 a
              INNER JOIN intent_media_production.ad_units ad on ad.id = a.ad_unit_id
              INNER JOIN intent_media_production.sites s on ad.site_ID = s.ID
              INNER JOIN intent_media_production.legal_page_types lp on lp.id = ad.legal_page_type_id
              LEFT JOIN (SELECT id, case WHEN ad_display_name is null then name else ad_display_name END as ad_display_name from intent_media_production.entities) e1 on e1.ID = a.advertiser_ID
              LEFT JOIN (SELECT id, case WHEN ad_display_name is null then name else ad_display_name END as ad_display_name from intent_media_production.entities) e2 on e2.ID = a.publisher_id
              
              UNION
              
              SELECT DISTINCT request_ID,  a.publisher_id,
              
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
              ad.name as "Ad Unit",  a.ad_unit_id, lp.page_type, a.requested_at_date_in_et, a.product_category_type, a.site_currency,a.rank_in_page, 
              0 as revenue, 0 as clicks, a.publisher_id, Dom_Intl,
              REPLACE(CONCAT(LEFT(s.display_name,1) , LOWER(Right(s.display_name, length(s.display_name) -1))),'.com','') as Advertiser, 
              publisher_price as advertiser_price, 1 as is_publisher,
                           
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
              else e2.ad_display_name end as Cartel
              
              FROM Intent_media_sandbox_production.EA_ppa_competition_stage_1 a
              INNER JOIN intent_media_production.ad_units ad on ad.id = a.ad_unit_id
              INNER JOIN intent_media_production.sites s on ad.site_ID = s.ID
              INNER JOIN intent_media_production.legal_page_types lp on lp.id = ad.legal_page_type_id
              LEFT JOIN (SELECT id, case WHEN ad_display_name is null then name else ad_display_name END as ad_display_name from intent_media_production.entities) e1 on e1.ID = a.advertiser_ID
              LEFT JOIN (SELECT id, case WHEN ad_display_name is null then name else ad_display_name END as ad_display_name from intent_media_production.entities) e2 on e2.ID = a.publisher_id) a;


      GRANT ALL on Intent_media_sandbox_production.EA_ppa_competition_stage_2 to PUBLIC

                            """
    cur.execute(sql_string)
    conn.close()

def do_analysis3(stryear, strmonth, strday):

    print "infunction_3"
    print strmonth + strday + stryear

    conn = create_connection()
    cur = conn.cursor()
    sql_string = """ 

      ---- Create 5 sets of the same data with each Advertiser occupying the first slot once

      DROP TABLE IF EXISTS  Intent_media_sandbox_production.EA_ppa_competition_stage_3;
      CREATE TABLE Intent_media_sandbox_production.EA_ppa_competition_stage_3
      AS

      SELECT b.*, ru.runner_up
      FROM
      (SELECT a.*, 1 as meta_rank FROM Intent_media_sandbox_production.EA_ppa_competition_stage_2 a

      UNION

      SELECT request_ID, publisher_id, Publisher, site, "Ad Unit", ad_unit_id, page_type, requested_at_date_in_et, product_category_type, site_currency, rank_in_page, 
       revenue, clicks, advertiser_id, Dom_Intl, Advertiser, advertiser_price, is_publisher, cartel, winning_price, win_or_tie, loss,
      CASE  
      WHEN torder = 2 THEN 1
      WHEN torder < 2 THEN torder + 1
      ELSE torder END as torder, 2 as meta_rank
      FROM Intent_media_sandbox_production.EA_ppa_competition_stage_2

      UNION

      SELECT request_ID, publisher_id, Publisher, site, "Ad Unit", ad_unit_id, page_type, requested_at_date_in_et, product_category_type, site_currency, rank_in_page, 
       revenue, clicks, advertiser_id, Dom_Intl,  Advertiser, advertiser_price, is_publisher,  cartel, winning_price, win_or_tie, loss,
      CASE  
      WHEN torder = 3 THEN 1
      WHEN torder < 3 THEN torder + 1
      ELSE torder END as torder, 3 as meta_rank
      FROM Intent_media_sandbox_production.EA_ppa_competition_stage_2

      UNION

      SELECT request_ID, publisher_id, Publisher, site, "Ad Unit", ad_unit_id,  page_type, requested_at_date_in_et, product_category_type, site_currency, rank_in_page, 
       revenue, clicks, advertiser_id, Dom_Intl,  Advertiser, advertiser_price, is_publisher,  cartel, winning_price, win_or_tie, loss,
      CASE  
      WHEN torder = 4 THEN 1
      WHEN torder < 4 THEN torder + 1
      ELSE torder END as torder, 4 as meta_rank
      FROM Intent_media_sandbox_production.EA_ppa_competition_stage_2

      UNION

      SELECT request_ID, publisher_id, Publisher, site, "Ad Unit", ad_unit_id,  page_type, requested_at_date_in_et, product_category_type, site_currency, rank_in_page, 
       revenue, clicks, advertiser_id, Dom_Intl, Advertiser, advertiser_price, is_publisher,  cartel, winning_price, win_or_tie, loss,
      CASE  
      WHEN torder = 5 THEN 1
      WHEN torder < 5 THEN torder + 1
      ELSE torder END as torder, 5 as meta_rank
      FROM Intent_media_sandbox_production.EA_ppa_competition_stage_2) b
      LEFT OUTER JOIN 
              (SELECT request_id, min(advertiser_price) as runner_up
              FROM Intent_media_sandbox_production.EA_ppa_competition_stage_2
              Where loss = 1
              GROUP BY request_ID) ru
      on ru.request_ID = b.request_ID
             
      ;

      GRANT ALL on Intent_media_sandbox_production.EA_ppa_competition_stage_3 to PUBLIC

                            """
    cur.execute(sql_string)
    conn.close()

def do_analysis4(stryear, strmonth, strday):

    print "infunction_4"
    print strmonth + strday + stryear

    conn = create_connection()
    cur = conn.cursor()
    sql_string = """ 

      ------------ Transpose each set into a single line

      DROP TABLE IF EXISTS  Intent_media_sandbox_production.EA_ppa_competition_stage_4;
      CREATE TABLE Intent_media_sandbox_production.EA_ppa_competition_stage_4
      AS

      SELECT  a.request_ID, publisher, site, publisher_ID, "Ad Unit", ad_unit_id,  page_type, requested_at_date_in_et, product_category_type, site_currency,  rank_in_page,  
              Dom_Intl, is_publisher_1, Advertiser_1, advertiser_id_1, advertiser_price_1, advertiser_1_revenue, advertiser_1_clicks, is_publisher_2, Advertiser_2, advertiser_id_2, 
              advertiser_price_2, advertiser_2_revenue, advertiser_2_clicks, is_publisher_3, Advertiser_3, advertiser_id_3, advertiser_price_3, advertiser_3_revenue, advertiser_3_clicks, is_publisher_4, Advertiser_4, 
              advertiser_id_4, advertiser_price_4, advertiser_4_revenue, advertiser_4_clicks, is_publisher_5, Advertiser_5, advertiser_id_5, advertiser_price_5, advertiser_5_revenue, advertiser_5_clicks,
              cartel_1, cartel_2, cartel_3, cartel_4, cartel_5 , a.winning_price, a.runner_up, win_or_tie_1, isnull(win_or_tie_2,0) + isnull(win_or_tie_3,0) + isnull(win_or_tie_4,0) + isnull(win_or_tie_5,0) as other_win_or_tie, loss_1
      FROM 
              
              (SELECT request_ID, site, Publisher, publisher_ID, "Ad Unit", ad_unit_id,  page_type, requested_at_date_in_et, product_category_type, site_currency,  rank_in_page, meta_rank, Dom_Intl, cartel as cartel_1,
              is_publisher as is_publisher_1, Advertiser as Advertiser_1, advertiser_id as advertiser_id_1, advertiser_price as advertiser_price_1, revenue as advertiser_1_revenue, clicks as advertiser_1_clicks,
              win_or_tie as win_or_tie_1, loss as loss_1, winning_price, runner_up
              FROM Intent_media_sandbox_production.EA_ppa_competition_stage_3
              WHERE torder = 1 ) a
              
              LEFT OUTER JOIN
              
              (SELECT request_ID, meta_rank, cartel as cartel_2,
              is_publisher as is_publisher_2, Advertiser as Advertiser_2, advertiser_id as advertiser_id_2, advertiser_price as advertiser_price_2, revenue as advertiser_2_revenue, clicks as advertiser_2_clicks,
              win_or_tie as win_or_tie_2, loss as loss_2
              FROM Intent_media_sandbox_production.EA_ppa_competition_stage_3
              WHERE torder = 2 ) b
              
              ON a.request_ID = b.request_ID AND a.meta_rank = b.meta_rank
              
              LEFT OUTER JOIN
              
              (SELECT request_ID, meta_rank, cartel as cartel_3,
              is_publisher as is_publisher_3, Advertiser as Advertiser_3, advertiser_id as advertiser_id_3, advertiser_price as advertiser_price_3, revenue as advertiser_3_revenue, clicks as advertiser_3_clicks,
              win_or_tie as win_or_tie_3, loss as loss_3
              FROM Intent_media_sandbox_production.EA_ppa_competition_stage_3
              WHERE torder = 3 ) c
              
              ON a.request_ID = c.request_ID AND a.meta_rank = c.meta_rank

              LEFT OUTER JOIN
              
              (SELECT request_ID, meta_rank, cartel as cartel_4,
              is_publisher as is_publisher_4, Advertiser as Advertiser_4, advertiser_id as advertiser_id_4, advertiser_price as advertiser_price_4, revenue as advertiser_4_revenue, clicks as advertiser_4_clicks,
              win_or_tie as win_or_tie_4, loss as loss_4
              FROM Intent_media_sandbox_production.EA_ppa_competition_stage_3
              WHERE torder = 4 ) d
              
              ON a.request_ID = d.request_ID AND a.meta_rank = d.meta_rank

              LEFT OUTER JOIN
              
              (SELECT request_ID, meta_rank, cartel as cartel_5,
              is_publisher as is_publisher_5, Advertiser as Advertiser_5, advertiser_id as advertiser_id_5, advertiser_price as advertiser_price_5, revenue as advertiser_5_revenue, clicks as advertiser_5_clicks,
              win_or_tie as win_or_tie_5, loss as loss_5
              FROM Intent_media_sandbox_production.EA_ppa_competition_stage_3
              WHERE torder = 5 ) e
              
              ON a.request_ID = e.request_ID AND a.meta_rank = e.meta_rank;

      GRANT ALL on Intent_media_sandbox_production.EA_ppa_competition_stage_4 to PUBLIC

                            """
    cur.execute(sql_string)
    conn.close()

def do_analysis5(stryear, strmonth, strday):

    print "infunction_5"
    print strmonth + strday + stryear

    conn = create_connection()
    cur = conn.cursor()
    sql_string = """ 

      DROP TABLE IF EXISTS  Intent_media_sandbox_production.EA_ppa_competition_report_"""  + strmonth + strday + stryear + """;
      CREATE TABLE Intent_media_sandbox_production.EA_ppa_competition_report_"""  + strmonth + strday + stryear + """
      AS

      SELECT 
              site, Publisher, publisher_ID, "Ad Unit", ad_unit_id,  page_type, requested_at_date_in_et, product_category_type, site_currency, rank_in_page, 
              Dom_Intl, is_publisher_1, Advertiser_1, cartel_1, advertiser_id_1, is_publisher_2, Advertiser_2, cartel_2, advertiser_id_2, 
              is_publisher_3, Advertiser_3, cartel_3, advertiser_id_3, is_publisher_4, Advertiser_4,  cartel_4, advertiser_id_4,  is_publisher_5, Advertiser_5,  cartel_5, advertiser_id_5,
              
              COUNT(DISTINCT request_ID) as Ad_Calls,
              
              SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie = 0 THEN 1 ELSE 0 END) as WIN,
              SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie > 0 THEN 1 ELSE 0 END) as TIE,                                            
              SUM(Loss_1) as LOSS,
              
              CASE WHEN is_publisher_2 = 1 AND advertiser_price_1 - advertiser_price_2 <= -1 THEN 1
               WHEN is_publisher_3 = 1 AND advertiser_price_1 - advertiser_price_3 <= -1 THEN 1
               WHEN is_publisher_4 = 1 AND advertiser_price_1 - advertiser_price_4 <= -1 THEN 1
               WHEN is_publisher_5 = 1 AND advertiser_price_1 - advertiser_price_5 <= -1 THEN 1
              ELSE 0 END as Beats_Publisher,
                     
              SUM(CASE WHEN advertiser_price_1 - isnull(advertiser_price_2,99999999) >= 1 AND  advertiser_price_1 - isnull(advertiser_price_3,99999999) >= 1 AND  advertiser_price_1 - isnull(advertiser_price_4,99999999) >= 1  AND  advertiser_price_1 - isnull(advertiser_price_5,99999999) >= 1 Then 1 ELSE 0 END) AS LOSE_ALL,

              case WHEN advertiser_price_1 - advertiser_price_2 <= -1 Then 1 ELSE 0 END +
              case WHEN advertiser_price_1 - advertiser_price_3 <= -1 Then 1 ELSE 0 END +
              case WHEN advertiser_price_1 - advertiser_price_4 <= -1 Then 1 ELSE 0 END +
              case WHEN advertiser_price_1 - advertiser_price_5 <= -1 Then 1 ELSE 0 END as num_of_wins,
                      
              case WHEN advertiser_price_1 - advertiser_price_2 < 1 AND advertiser_price_1 - advertiser_price_2 > -1 Then 1 ELSE 0 END +
              case WHEN advertiser_price_1 - advertiser_price_3 < 1 AND advertiser_price_1 - advertiser_price_3 > -1 Then 1 ELSE 0 END +       
              case WHEN advertiser_price_1 - advertiser_price_4 < 1 AND advertiser_price_1 - advertiser_price_4 > -1 Then 1 ELSE 0 END +
              case WHEN advertiser_price_1 - advertiser_price_5 < 1 AND advertiser_price_1 - advertiser_price_5 > -1 Then 1 ELSE 0 END as num_of_ties,

              
              case WHEN advertiser_price_1 - advertiser_price_2 >= 1 Then 1 ELSE 0 END +
              case WHEN advertiser_price_1 - advertiser_price_3 >= 1 Then 1 ELSE 0 END +
              case WHEN advertiser_price_1 - advertiser_price_4 >= 1 Then 1 ELSE 0 END +
              case WHEN advertiser_price_1 - advertiser_price_5 >= 1 Then 1 ELSE 0 END as num_of_losses,

              SUM(advertiser_1_revenue) as advertiser_1_revenue,
              SUM(advertiser_2_revenue) as advertiser_2_revenue, 
              SUM(advertiser_3_revenue) as advertiser_3_revenue,
              SUM(advertiser_4_revenue) as advertiser_4_revenue,
              SUM(advertiser_5_revenue) as advertiser_5_revenue,
                                             
              SUM(advertiser_1_clicks) as advertiser_1_clicks,
              SUM(advertiser_2_clicks) as advertiser_2_clicks,
              SUM(advertiser_3_clicks) as advertiser_3_clicks,
              SUM(advertiser_4_clicks) as advertiser_4_clicks,
              SUM(advertiser_5_clicks) as advertiser_5_clicks, 
      ---Win Clicks
              --SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie = 0 Then advertiser_1_clicks ELSE 0 END) AS WIN_Clicks_1,
              --SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie = 0 Then advertiser_2_clicks ELSE 0 END) AS WIN_Clicks_2,
              --SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie = 0 Then advertiser_3_clicks ELSE 0 END) AS WIN_Clicks_3,
              --SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie = 0 Then advertiser_4_clicks ELSE 0 END) AS WIN_Clicks_4,
              --SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie = 0 Then advertiser_5_clicks ELSE 0 END) AS WIN_Clicks_5,

      --Tie clicks    
              --SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie > 0 THEN advertiser_1_clicks ELSE 0 END) as TIE_Clicks_1,                
              --SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie > 0 THEN advertiser_2_clicks ELSE 0 END) as TIE_Clicks_2,   
              --SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie > 0 THEN advertiser_3_clicks ELSE 0 END) as TIE_Clicks_3,   
              --SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie > 0 THEN advertiser_4_clicks ELSE 0 END) as TIE_Clicks_4,   
              --SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie > 0 THEN advertiser_5_clicks ELSE 0 END) as TIE_Clicks_5,   
                  
      --Lose Clicks    

              --SUM(CASE WHEN Loss_1 = 1 Then advertiser_1_clicks ELSE 0 END) AS LOSS_Clicks_1,    
              --SUM(CASE WHEN Loss_1 = 1 Then advertiser_2_clicks ELSE 0 END) AS LOSS_Clicks_2,
              --SUM(CASE WHEN Loss_1 = 1 Then advertiser_3_clicks ELSE 0 END) AS LOSS_Clicks_3,        
              --SUM(CASE WHEN Loss_1 = 1 Then advertiser_4_clicks ELSE 0 END) AS LOSS_Clicks_4,        
              --SUM(CASE WHEN Loss_1 = 1 Then advertiser_5_clicks ELSE 0 END) AS LOSS_Clicks_5,
                               
      ---Win Revenue
              --SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie = 0 Then advertiser_1_revenue ELSE 0 END) AS WIN_revenue_1,
              --SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie = 0 Then advertiser_2_revenue ELSE 0 END) AS WIN_revenue_2,
              --SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie = 0 Then advertiser_3_revenue ELSE 0 END) AS WIN_revenue_3,
              --SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie = 0 Then advertiser_4_revenue ELSE 0 END) AS WIN_revenue_4,
              --SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie = 0 Then advertiser_5_revenue ELSE 0 END) AS WIN_revenue_5,

      --Tie Revenue    
              --SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie > 0 THEN advertiser_1_revenue ELSE 0 END) as TIE_revenue_1,                
              --SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie > 0 THEN advertiser_2_revenue ELSE 0 END) as TIE_revenue_2,   
              --SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie > 0 THEN advertiser_3_revenue ELSE 0 END) as TIE_revenue_3,               
              --SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie > 0 THEN advertiser_4_revenue ELSE 0 END) as TIE_revenue_4,             
              --SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie > 0 THEN advertiser_5_revenue ELSE 0 END) as TIE_revenue_5,   
                  
      --Lose Revenue    

              --SUM(CASE WHEN Loss_1 = 1 Then advertiser_1_revenue ELSE 0 END) AS LOSS_revenue_1,    
              --SUM(CASE WHEN Loss_1 = 1 Then advertiser_2_revenue ELSE 0 END) AS LOSS_revenue_2,
              --SUM(CASE WHEN Loss_1 = 1 Then advertiser_3_revenue ELSE 0 END) AS LOSS_revenue_3,        
              --SUM(CASE WHEN Loss_1 = 1 Then advertiser_4_revenue ELSE 0 END) AS LOSS_revenue_4,        
              --SUM(CASE WHEN Loss_1 = 1 Then advertiser_5_revenue ELSE 0 END) AS LOSS_revenue_5,
              
              
              CASE WHEN cartel_1 = cartel_2 THEN 1 ELSE 0 END + CASE WHEN cartel_1 = cartel_3 THEN 1 ELSE 0 END + CASE WHEN cartel_1 = cartel_4 THEN 1 ELSE 0 END + CASE WHEN cartel_1 = cartel_5 THEN 1 ELSE 0 END as cartel_count,

      ---- PCT Difference to winning price of loss
              SUM(CASE WHEN Loss_1 = 1 THEN  (advertiser_price_1 - winning_price)/winning_price ELSE 0 END) as SUM_LOSS_PRCT_Differences,
              --SUM(CASE WHEN Loss_1 = 1 THEN ((advertiser_price_1 - winning_price)/winning_price)^2 ELSE 0 END) as SUM_LOSS_PRCT_Differences_sqr,
              
      ---- PCT Difference to next highest price of win -- use Runner_up_price

              SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie = 0 THEN  (advertiser_price_1 - isnull(runner_up, advertiser_price_1))/runner_up ELSE 0 END) as SUM_WIN_PRCT_Differences,
              --SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie = 0 THEN ((advertiser_price_1 - isnull(runner_up, advertiser_price_1))/runner_up)^2 ELSE 0 END) as SUM_WIN_PRCT_Differences_sqr,

                                             
              SUM((advertiser_price_1 - advertiser_price_2)/advertiser_price_2) as SUM_PRCT_Differences_2,
              SUM((advertiser_price_1 - advertiser_price_3)/advertiser_price_3) as SUM_PRCT_Differences_3,
              SUM((advertiser_price_1 - advertiser_price_4)/advertiser_price_4) as SUM_PRCT_Differences_4,
              SUM((advertiser_price_1 - advertiser_price_5)/advertiser_price_5) as SUM_PRCT_Differences_5,

              SUM(CASE WHEN (advertiser_price_1 - advertiser_price_2)/advertiser_price_2 > .5 THEN 1 ELSE 0 END)  as PRCT_Differences_Over_50_2,
              SUM(CASE WHEN  (advertiser_price_1 - advertiser_price_3)/advertiser_price_3 > .5 THEN 1 ELSE 0 END)  as PRCT_Differences_Over_50_3,
              SUM(CASE WHEN  (advertiser_price_1 - advertiser_price_4)/advertiser_price_4 > .5 THEN 1 ELSE 0 END)  as PRCT_Differences_Over_50_4,
              SUM(CASE WHEN  (advertiser_price_1 - advertiser_price_5)/advertiser_price_5 > .5 THEN 1 ELSE 0 END)  as PRCT_Differences_Over_50_5,
                      
              SUM(CASE WHEN  (advertiser_price_1 - advertiser_price_2)/advertiser_price_2 < -.5 THEN 1 ELSE 0 END)  as PRCT_Differences_Under_neg50_2,
              SUM(CASE WHEN  (advertiser_price_1 - advertiser_price_3)/advertiser_price_3 < -.5 THEN 1 ELSE 0 END)  as PRCT_Differences_Under_neg50_3,
              SUM(CASE WHEN  (advertiser_price_1 - advertiser_price_4)/advertiser_price_4 < -.5 THEN 1 ELSE 0 END)  as PRCT_Differences_Under_neg50_4,
              SUM(CASE WHEN  (advertiser_price_1 - advertiser_price_5)/advertiser_price_5 < -.5 THEN 1 ELSE 0 END)  as PRCT_Differences_Under_neg50_5,              
              

              
              SUM(case WHEN advertiser_price_1 - advertiser_price_2 <= -1 Then 1 ELSE 0 END) as WINS_1_2,
              SUM(case WHEN advertiser_price_1 - advertiser_price_3 <= -1 Then 1 ELSE 0 END) as WINS_1_3,
              SUM(case WHEN advertiser_price_1 - advertiser_price_4 <= -1 Then 1 ELSE 0 END) as WINS_1_4 ,       
              SUM(case WHEN advertiser_price_1 - advertiser_price_5 <= -1 Then 1 ELSE 0 END) as WINS_1_5,
                      
              SUM(case WHEN advertiser_price_1 - advertiser_price_2 < 1 AND advertiser_price_1 - advertiser_price_2 > -1 Then 1 ELSE 0 END) as TIES_1_2,
              SUM(case WHEN advertiser_price_1 - advertiser_price_3 < 1 AND advertiser_price_1 - advertiser_price_3 > -1 Then 1 ELSE 0 END) as TIES_1_3,        
              SUM(case WHEN advertiser_price_1 - advertiser_price_4 < 1 AND advertiser_price_1 - advertiser_price_4 > -1 Then 1 ELSE 0 END) as TIES_1_4,
              SUM(case WHEN advertiser_price_1 - advertiser_price_5 < 1 AND advertiser_price_1 - advertiser_price_5 > -1 Then 1 ELSE 0 END) as TIES_1_5,

              
              SUM(case WHEN advertiser_price_1 - advertiser_price_2 >= 1 Then 1 ELSE 0 END) as LOSSES_1_2,
              SUM(case WHEN advertiser_price_1 - advertiser_price_3 >= 1 Then 1 ELSE 0 END) as LOSSES_1_3,        
              SUM(case WHEN advertiser_price_1 - advertiser_price_4 >= 1 Then 1 ELSE 0 END) as LOSSES_1_4,
              SUM(case WHEN advertiser_price_1 - advertiser_price_5 >= 1 Then 1 ELSE 0 END) as LOSSES_1_5,
                               
      --Prct Diff        
              SUM(case WHEN advertiser_price_1 - advertiser_price_2 <= -1 Then (advertiser_price_1 - advertiser_price_2)/advertiser_price_2 ELSE null END) as WINS_SUM_PRCT_Differences_1_2,
              SUM(case WHEN advertiser_price_1 - advertiser_price_3 <= -1 Then (advertiser_price_1 - advertiser_price_3)/advertiser_price_3 ELSE null END) as WINS_SUM_PRCT_Differences_1_3,        
              SUM(case WHEN advertiser_price_1 - advertiser_price_4 <= -1 Then (advertiser_price_1 - advertiser_price_4)/advertiser_price_4 ELSE null END) as WINS_SUM_PRCT_Differences_1_4,
              SUM(case WHEN advertiser_price_1 - advertiser_price_5 <= -1 Then (advertiser_price_1 - advertiser_price_5)/advertiser_price_5 ELSE null END) as WINS_SUM_PRCT_Differences_1_5,                
      --Prct Diff^2
 
      --Prct Diff        
              SUM(case WHEN advertiser_price_1 - advertiser_price_2 >= 1 Then (advertiser_price_1 - advertiser_price_2)/advertiser_price_2 ELSE null END) as LOSES_SUM_PRCT_Differences_1_2,
              SUM(case WHEN advertiser_price_1 - advertiser_price_3 >= 1 Then (advertiser_price_1 - advertiser_price_3)/advertiser_price_3 ELSE null END) as LOSES_SUM_PRCT_Differences_1_3,        
              SUM(case WHEN advertiser_price_1 - advertiser_price_4 >= 1 Then (advertiser_price_1 - advertiser_price_4)/advertiser_price_4 ELSE null END) as LOSES_SUM_PRCT_Differences_1_4,
              SUM(case WHEN advertiser_price_1 - advertiser_price_5 >= 1 Then (advertiser_price_1 - advertiser_price_5)/advertiser_price_5 ELSE null END) as LOSES_SUM_PRCT_Differences_1_5                    

      FROM Intent_media_sandbox_production.EA_ppa_competition_stage_4
      GROUP BY 
              site, Publisher, publisher_ID, "Ad Unit", ad_unit_id,  page_type, requested_at_date_in_et, product_category_type, site_currency, rank_in_page, Dom_Intl,
               is_publisher_1, Advertiser_1, advertiser_id_1, is_publisher_2, Advertiser_2, advertiser_id_2, 
              is_publisher_3, Advertiser_3, advertiser_id_3, is_publisher_4, Advertiser_4, advertiser_id_4,  is_publisher_5, Advertiser_5, advertiser_id_5,
              cartel_1, cartel_2, cartel_3, cartel_4, cartel_5,
              
              case WHEN advertiser_price_1 - advertiser_price_2 <= -1 Then 1 ELSE 0 END +
              case WHEN advertiser_price_1 - advertiser_price_3 <= -1 Then 1 ELSE 0 END +
              case WHEN advertiser_price_1 - advertiser_price_4 <= -1 Then 1 ELSE 0 END +
              case WHEN advertiser_price_1 - advertiser_price_5 <= -1 Then 1 ELSE 0 END ,
                      
              case WHEN advertiser_price_1 - advertiser_price_2 < 1 AND advertiser_price_1 - advertiser_price_2 > -1 Then 1 ELSE 0 END +
              case WHEN advertiser_price_1 - advertiser_price_3 < 1 AND advertiser_price_1 - advertiser_price_3 > -1 Then 1 ELSE 0 END +       
              case WHEN advertiser_price_1 - advertiser_price_4 < 1 AND advertiser_price_1 - advertiser_price_4 > -1 Then 1 ELSE 0 END +
              case WHEN advertiser_price_1 - advertiser_price_5 < 1 AND advertiser_price_1 - advertiser_price_5 > -1 Then 1 ELSE 0 END ,

              
              case WHEN advertiser_price_1 - advertiser_price_2 >= 1 Then 1 ELSE 0 END +
              case WHEN advertiser_price_1 - advertiser_price_3 >= 1 Then 1 ELSE 0 END +
              case WHEN advertiser_price_1 - advertiser_price_4 >= 1 Then 1 ELSE 0 END +
              case WHEN advertiser_price_1 - advertiser_price_5 >= 1 Then 1 ELSE 0 END,
              
              
              CASE WHEN is_publisher_2 = 1 AND advertiser_price_1 - advertiser_price_2 <= -1 THEN 1
               WHEN is_publisher_3 = 1 AND advertiser_price_1 - advertiser_price_3 <= -1 THEN 1
               WHEN is_publisher_4 = 1 AND advertiser_price_1 - advertiser_price_4 <= -1 THEN 1
               WHEN is_publisher_5 = 1 AND advertiser_price_1 - advertiser_price_5 <= -1 THEN 1
              ELSE 0 END ;
                            """
    
    cur.execute(sql_string)
    conn.close()

def runtheloop(year, month, day, step):

    stryear = str(year)

    # Add zeros for single digit days and forward slashes for dates
    if len(str(day)) == 1:
        strday = '0' + str(day) + '/'
    else:
        strday = str(day) + '/'

    if len(str(month)) == 1:
        strmonth = '0' + str(month) + '/'
    else:
        strmonth = str(month) + '/'

    daystorun = (datetime.date.today() - datetime.date(year,month,day)).days - 1
    ongoing_date = datetime.date(year,month,day)
    print daystorun

    while daystorun >= 0:
        
        # Redfine string variables as the loop progresses
        if len(str(ongoing_date.day)) == 1:
            strday = '0' + str(ongoing_date.day) + '/'
        else:
            strday = str(ongoing_date.day) + '/'

        if len(str(ongoing_date.month)) == 1:
            strmonth = '0' + str(ongoing_date.month) + '/'
        else:
            strmonth = str(ongoing_date.month) + '/'

        stryear = str(ongoing_date.year)


        try:
            print "starting"

            # Run each of the functions for each part of the sql query.  I do it in steps so the loop can pick back up in the same place in case of an error
            if step == 1:
                do_analysis1(stryear, strmonth, strday);
                step = 2;

            if step == 2:                
                do_analysis2(stryear, strmonth, strday)
                step = 3

            if step == 3:
                do_analysis3(stryear, strmonth, strday)            
                step = 4

            if step == 4:
                do_analysis4(stryear, strmonth, strday)            
                step = 5

            if step == 5:
                do_analysis5(stryear, strmonth[:-1], strday[:-1])
                step = 1    

            print "ending"
            daystorun = daystorun - 1
            day = day + 1
            ongoing_date = ongoing_date + datetime.timedelta(days=1)

        except Exception:
            print "Error"
            print "Error", sys.exc_info()[0]
            runtheloop(ongoing_date.year, ongoing_date.month, ongoing_date.day, step)

runtheloop(2015, 1, 5, 1)