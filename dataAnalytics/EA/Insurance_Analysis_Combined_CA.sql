DROP TABLE IF EXISTS Intent_Media_Sandbox_production.Expedia_Insurance_Profit;

Create Table Intent_Media_Sandbox_production.Expedia_Insurance_Profit 
(Flight_Type Varchar(100),  
Insurance_Margin Float,
Percent_Multiple Float,
Flat_Multiple Float);


INSERT INTO Intent_Media_Sandbox_production.Expedia_Insurance_Profit 

SElect Flight_Type, Correct_AVG_OF_INS_MGN, Correct_Avg_Of_PCT_MGN, Correct_Avg_Of_Flat_MGN
 FROM
       
 ----- --------------------------Get Multipliers--------------------------------------------------
 
       
 (Select  CASE main.Flight_Type WHEN 'Special' THEN 'International' ELSE main.Flight_Type END as Flight_Type, 
 
 ROUND((SUM(insurance_Profit_calc)/SUM(Insurance_Value))*100,3)  as Correct_AVG_OF_INS_MGN, 
 ROUND((SUM(insurance_Profit_calc)/SUM(Conversion_Value))*100,3) as Correct_AVG_OF_PCT_MGN, 
 ROUND((SUM(insurance_Profit_calc)/SUM(Travelers)),3)            as Correct_AVG_OF_Flat_MGN
 
       
FRom

(select
	month(requested_at_date_in_et) as mo,
	
	CASE 
	       WHEN apc.is_domestic='true' AND apc2.is_domestic='true'  THEN 'Domestic'
	       WHEN (apc.is_domestic='false' AND apc2.is_domestic='false'  AND (apc.State  in ('HI','AK', 'PR') AND APC2.State  in ('HI','AK', 'PR') )) THEN 'Special'
	       WHEN (apc.is_domestic='false' AND apc2.is_domestic='false'  AND ( (APC.Region in ('Mexico','The Caribbean', 'Canada') or apc.State  in ('HI','AK', 'PR'))  AND (APC2.Region in ('Mexico','The Caribbean', 'Canada') or apc2.State  in ('HI','AK', 'PR')))) THEN 'Special'
	       WHEN (apc.is_domestic='true' AND apc2.is_domestic='false'  AND ( APC2.Region in ('Mexico','The Caribbean', 'Canada') or apc2.State  in ('HI','AK', 'PR') )) THEN 'Special'
	       WHEN (apc.is_domestic='false' AND apc2.is_domestic='true'  AND ( APC.Region in ('Mexico','The Caribbean', 'Canada') or apc.State  in ('HI','AK', 'PR') )) THEN 'Special'
	       WHEN (apc.is_domestic='false' AND apc2.is_domestic='false'  AND ( (APC.Region Not in ('Mexico','The Caribbean', 'Canada') or apc.State NOt in ('HI','AK', 'PR'))  AND (APC2.Region Not in ('Mexico','The Caribbean', 'Canada') or apc.State not in ('HI','AK', 'PR')))) THEN 'International'
	       ELSE 'International'
       
	END as Flight_Type,
	
	insurance_value as insurance_value,  Conversion_Value, Travelers, Insurance_Value/travelers as Insurance_Value_O_travelers,
	
	---Formula applied
	
	CASE 
	       WHEN apc.is_domestic='true' AND apc2.is_domestic='true'  THEN 
	               CASE when conversion_value/travelers < 2700 then Insurance_Value-((10.88 * Travelers) + .033*Insurance_Value)
				when conversion_value/travelers >= 2700 then insurance_Value -((10.57 * Travelers) + .033*Insurance_Value)
			end  
	       WHEN (apc.is_domestic='false' AND apc2.is_domestic='false'  AND (apc.State  in ('HI','AK', 'PR') AND APC2.State  in ('HI','AK', 'PR') )) THEN insurance_Value -((13.47 * Travelers) + .033*Insurance_Value)
	       WHEN (apc.is_domestic='false' AND apc2.is_domestic='false'  AND ( (APC.Region in ('Mexico','The Caribbean', 'Canada') or apc.State  in ('HI','AK', 'PR'))  AND (APC2.Region in ('Mexico','The Caribbean', 'Canada') or apc2.State  in ('HI','AK', 'PR')))) 
	               THEN insurance_Value -((Travelers * 13.47) + .033*Insurance_Value)
	       WHEN (apc.is_domestic='true' AND apc2.is_domestic='false'  AND ( APC2.Region in ('Mexico','The Caribbean', 'Canada') or apc2.State  in ('HI','AK', 'PR') )) THEN insurance_Value -((Travelers *13.47) + .033*Insurance_Value)
	       WHEN (apc.is_domestic='false' AND apc2.is_domestic='true'  AND ( APC.Region in ('Mexico','The Caribbean', 'Canada') or apc.State  in ('HI','AK', 'PR') )) THEN insurance_Value -((13.47 * Travelers) + .033*Insurance_Value)
	       WHEN (apc.is_domestic='false' AND apc2.is_domestic='false'  AND ( (APC.Region Not in ('Mexico','The Caribbean', 'Canada') or apc.State NOt in ('HI','AK', 'PR'))  AND (APC2.Region Not in ('Mexico','The Caribbean', 'Canada') or apc.State not in ('HI','AK', 'PR')))) 
	               THEN insurance_value-((21.23 * Travelers) + (0.033*Insurance_Value))
	       ELSE insurance_value-((21.23*Travelers) + (0.033*Insurance_Value))
       
	END as Insurance_Profit_Calc
	
from
	intent_media_log_data_production.conversions c
join
	intent_media_production.airport_codes apc
on
	c.origination = apc.code
join
	intent_media_production.airport_codes apc2
on
	c.destination = apc2.code
where
	site_type='EXPEDIA_CA'
	and ip_address_blacklisted is false
	and entity_id=45
	and product_category_type='FLIGHTS'
	and insurance_value <>0
	and requested_at_date_in_et >= '2014-04-01' and requested_at_date_in_et < '2014-06-01'
	AND Conversion_Value < 70000) main

GROUP BY     CASE main.Flight_Type WHEN 'Special' THEN 'International' ELSE main.Flight_Type END 
ORder by CASE main.Flight_Type WHEN 'Special' THEN 'International' ELSE main.Flight_Type  END ) GetMult;
  


-----------------------Genereate Summary Table Of Results ------ 

Select  Mo, 
        ---Flight_Type, 
        ROUND(SUM(Insurance_Value),0) as Insurance_Value,
        ROUND(SUM(Old_Insurance_Profit),0) as Old_Insurance_Profit,
        ROUND(SUM(Insurance_Profit_Calc),0) as Expedia_Calc_Profit, 
        
        ------Insurance Margin
        ROUND(SUM(Calc_INS_Margin),0) as Proposed_INS_Profit, 
        ROUND(SUM(Calc_INS_Margin) - SUM(Insurance_Profit_Calc),0) as INS_Profit_Diff, 
        ROUND(((SUM(Calc_INS_Margin) - SUM(Insurance_Profit_Calc))/SUM(Insurance_Profit_Calc)),2) as INS_Profit_PCT_Diff, 
        ROUND(CORR(Calc_INS_Margin, Insurance_Profit_Calc),2) as Corr_INS_Expedia,
        AVG(Calc_INS_Margin) as Calc_INS_Conv_margin, 
        
        ------PCT
        ROUND(SUM(Calc_PCT_CONV_Margin),0) as Proposed_PCT_Profit, 
        ROUND(SUM(Calc_PCT_CONV_Margin) - SUM(Insurance_Profit_Calc),0) as PCT_Profit_Diff, 
        ROUND(((SUM(Calc_PCT_CONV_Margin) - SUM(Insurance_Profit_Calc))/SUM(Insurance_Profit_Calc)),2) as PCT_Profit_PCT_Diff, 
        ROUND(CORR(Calc_PCT_CONV_Margin, Insurance_Profit_Calc),2) as Corr_PCT_Expedia,
        AVG(Calc_PCT_CONV_Margin) as Calc_PCT_Conv_margin,  
        -----Flat
        ROUND(SUM(Calc_Flat_Margin),0) as Proposed_Flat_Profit,
        ROUND(SUM(Calc_Flat_Margin) - SUM(Insurance_Profit_Calc),0) as Flat_Profit_Diff,
        ROUND(((SUM(Calc_Flat_Margin) - SUM(Insurance_Profit_Calc))/SUM(Insurance_Profit_Calc)),2) as Flat_Profit_PCT_Diff, 
        ROUND(CORR(Calc_Flat_Margin, Insurance_Profit_Calc),2) as Corr_Flat_Expedia, 
        AVG(Calc_Flat_Margin) as Calc_Flat_Margin, 
       ------Actual Values
 
        AVG(Insurance_Margin) as Insurance_Margin,   
        AVG(Percent_Multiple) as Percent_Margin, 
        Avg(Flat_Multiple) as Flat_Multiple

From
(SELECT main.*, p.Insurance_Margin, p.Percent_Multiple, p.Flat_Multiple, 
Insurance_Value * Insurance_Margin *(1/100) as Calc_INS_Margin, 
Conversion_Value * Percent_Multiple *(1/100) as Calc_PCT_CONV_Margin, 
Flat_Multiple * Travelers as Calc_Flat_Margin
From 

(select
	month(requested_at_date_in_et) as mo,
	-----------------Note International replaces Special in this Query for Summary
	CASE 
	       WHEN apc.is_domestic='true' AND apc2.is_domestic='true'  THEN 'Domestic'
	       WHEN (apc.is_domestic='false' AND apc2.is_domestic='false'  AND (apc.State  in ('HI','AK', 'PR') AND APC2.State  in ('HI','AK', 'PR') )) THEN 'International'
	       WHEN (apc.is_domestic='false' AND apc2.is_domestic='false'  AND ( (APC.Region in ('Mexico','The Caribbean', 'Canada') or apc.State  in ('HI','AK', 'PR'))  AND (APC2.Region in ('Mexico','The Caribbean', 'Canada') or apc2.State  in ('HI','AK', 'PR')))) THEN 'International'
	       WHEN (apc.is_domestic='true' AND apc2.is_domestic='false'  AND ( APC2.Region in ('Mexico','The Caribbean', 'Canada') or apc2.State  in ('HI','AK', 'PR') )) THEN 'International'
	       WHEN (apc.is_domestic='false' AND apc2.is_domestic='true'  AND ( APC.Region in ('Mexico','The Caribbean', 'Canada') or apc.State  in ('HI','AK', 'PR') )) THEN 'International'
	       WHEN (apc.is_domestic='false' AND apc2.is_domestic='false'  AND ( (APC.Region Not in ('Mexico','The Caribbean', 'Canada') or apc.State NOt in ('HI','AK', 'PR'))  AND (APC2.Region Not in ('Mexico','The Caribbean', 'Canada') or apc.State not in ('HI','AK', 'PR')))) THEN 'International'
	       ELSE 'International'
       
	END as Flight_Type,
	
	insurance_value as insurance_value, Insurance_Value *.6 as OLD_Insurance_Profit, Conversion_Value, Travelers, Insurance_Value/travelers as Insurance_Value_O_travelers,
	
	---Formula applied
	
	CASE 
	       WHEN apc.is_domestic='true' AND apc2.is_domestic='true'  THEN 
	               CASE when conversion_value/travelers < 2700 then Insurance_Value-((10.88*Travelers) + .033*Insurance_Value)
				when conversion_value/travelers >= 2700 then insurance_Value -((10.57*Travelers) + .033*Insurance_Value)
			end  
	       WHEN (apc.is_domestic='false' AND apc2.is_domestic='false'  AND (apc.State  in ('HI','AK', 'PR') AND APC2.State  in ('HI','AK', 'PR') )) THEN insurance_Value -((13.47*Travelers) + .033*Insurance_Value)
	       WHEN (apc.is_domestic='false' AND apc2.is_domestic='false'  AND ( (APC.Region in ('Mexico','The Caribbean', 'Canada') or apc.State  in ('HI','AK', 'PR'))  AND (APC2.Region in ('Mexico','The Caribbean', 'Canada') or apc2.State  in ('HI','AK', 'PR')))) 
	               THEN insurance_Value -((13.47*Travelers) + .033*Insurance_Value)
                                                                                       WHEN (apc.is_domestic='true' AND apc2.is_domestic='false'  AND ( APC2.Region in ('Mexico','The Caribbean', 'Canada') or apc2.State  in ('HI','AK', 'PR') )) THEN insurance_Value -((13.47*Travelers) + .033*Insurance_Value)
                                                                                       WHEN (apc.is_domestic='false' AND apc2.is_domestic='true'  AND ( APC.Region in ('Mexico','The Caribbean', 'Canada') or apc.State  in ('HI','AK', 'PR') )) THEN insurance_Value -((13.47*Travelers) + .033*Insurance_Value)
                                                                                       WHEN (apc.is_domestic='false' AND apc2.is_domestic='false'  AND ( (APC.Region Not in ('Mexico','The Caribbean', 'Canada') or apc.State NOt in ('HI','AK', 'PR'))  AND (APC2.Region Not in ('Mexico','The Caribbean', 'Canada') or apc.State not in ('HI','AK', 'PR')))) 
	               THEN insurance_value-((21.23*Travelers) + (0.033*Insurance_Value))
	       ELSE insurance_value-((21.23*Travelers) + (0.033*Insurance_Value))
       
	END as Insurance_Profit_Calc
	
from
	intent_media_log_data_production.conversions c
join
	intent_media_production.airport_codes apc
on
	c.origination = apc.code
join
	intent_media_production.airport_codes apc2
on
	c.destination = apc2.code
where
	site_type='EXPEDIA_CA'
	and ip_address_blacklisted is false
	and entity_id=45
	and product_category_type='FLIGHTS'
	and insurance_value <>0
	--and requested_at_date_in_et >= '2013-03-01' and requested_at_date_in_et < '2014-07-01'
	AND Conversion_Value < 70000
	                                                                                               ) main
Join 
        Intent_Media_Sandbox_Production.Expedia_Insurance_Profit p
        
On   p.Flight_Type = main.Flight_Type) Comb

GROUP BY Mo 
--,Flight_Type
ORDER BY--- Flight_Type,
 Mo



