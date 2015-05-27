CREATE TABLE intent_media_sandbox_production.Two_Days
(
    actual_cpc float,
    ad_call_date_With_Hour varchar(100),
    click_date_with_Hour varchar(100),
    advertiser_id int,
    advertisement_id int,
    site_type varchar(100),
    advertisement_type varchar(100),
    fraudulent int
);



        COPY  intent_media_sandbox_production.Two_Days
        ( actual_cpc ,
        ad_call_date_With_Hour  ,
        click_date_with_Hour ,
        advertiser_id ,
        advertisement_id ,
        site_type ,
        advertisement_type ,
        fraudulent   )
        FROM LOCAL '/Users/eric.abis/Documents/Dumps/Click_Audit_test/Two_Days_With_Click_Date/Two_Days_With_Click_Date.csv'
        WITH DELIMITER E'\t';
        
@export on;
@export set filename = "/Users/eric.abis/Documents/Dumps/Click_Audit_test/Two_Days_With_Click_Date/Two_Days_With_Click_Date_Final.csv";
SELECT actual_cpc, TIMESTAMPADD(HOUR, -5,TO_TIMESTAMP(ad_call_Date_With_Hour, 'yyyy-MM-ddThh')) as ad_call_Date_With_Hour, 
TIMESTAMPADD(HOUR, -5,TO_TIMESTAMP(click_Date_With_Hour, 'yyyy-MM-ddThh')) as click_Date_With_Hour, advertiser_id, Site_Type, advertisement_type, fraudulent
FROM Intent_media_sandbox_production.Two_Days;
@export off;

