/* FLIGHTS Placement Level Breakdown */

Select i.*, p. Placement, p."Placement is Prechecked", p."Clicks from Placement", p."Conversions From Placement", p."Gross Media Revenue from Placement", p."Interactions from Placement", p."Conversion Value Sum"
from intent_media_sandbox_production.sca_dash_air i
LEFT join
  intent_media_sandbox_production.sca_dash_air_placement p
on i.date = p.date_in_et and i.Country = p.Country and i."Ad Unit ID" = p.ad_unit_id
left join intent_media_sandbox_production.ad_units au on i."Ad Unit ID" = au.id
left join intent_media_sandbox_production.sites s on au.site_id = s.id
left join intent_media_sandbox_production.entities e on e.id = s.publisher_id
left join intent_media_sandbox_production.legal_page_types lpt on lpt.id = au.legal_page_type_id

union

/* FLIGHTS Total */
Select i.*, 'Total' as Placement, 'Total Placement' as "Placement is Prechecked", p."Clicks from Placement", p."Conversions From Placement", p."Gross Media Revenue from Placement", p."Interactions from Placement", p."Conversion Value Sum"
from intent_media_sandbox_production.sca_dash_air i
LEFT join
	(
	Select date_in_et, country, ad_unit_id, SUM("Gross Media Revenue from Placement") as "Gross Media Revenue from Placement", SUM("Conversions From Placement") as "Conversions From Placement", SUM("Clicks from Placement") as "Clicks from Placement", 
	       SUM("Conversion Value Sum") as "Conversion Value Sum", SUM("Interactions from Placement") as "Interactions from Placement" 
	FROM intent_media_sandbox_production.sca_dash_air_placement
	GROUP BY date_in_et, country, ad_unit_id 
	) p
on i.Date = p.date_in_et and i.Country = p.Country and i."Ad Unit ID" = p.ad_unit_id
left join intent_media_sandbox_production.ad_units au on i."Ad Unit ID" = au.id
left join intent_media_sandbox_production.sites s on au.site_id = s.id
left join intent_media_sandbox_production.entities e on e.id = s.publisher_id
left join intent_media_sandbox_production.legal_page_types lpt on lpt.id = au.legal_page_type_id

union

/* FLIGHTS Placment Total */
Select i.*,  p.Placement, 'Total Placement' as "Placement is Prechecked", p."Clicks from Placement", p."Conversions From Placement", p."Gross Media Revenue from Placement", p."Interactions from Placement", p."Conversion Value Sum"
from intent_media_sandbox_production.sca_dash_air i
LEFT join
	(
	Select date_in_et, country, ad_unit_id, placement, SUM("Gross Media Revenue from Placement") as "Gross Media Revenue from Placement", SUM("Conversions From Placement") as "Conversions From Placement", SUM("Clicks from Placement") as "Clicks from Placement", 
	       SUM("Conversion Value Sum") as "Conversion Value Sum", SUM("Interactions from Placement") as "Interactions from Placement" 
	FROM intent_media_sandbox_production.sca_dash_air_placement
	GROUP BY date_in_et, country, ad_unit_id, placement
	) p
on i.Date = p.date_in_et and i.Country = p.Country and i."Ad Unit ID" = p.ad_unit_id
left join intent_media_sandbox_production.ad_units au on i."Ad Unit ID" = au.id
left join intent_media_sandbox_production.sites s on au.site_id = s.id
left join intent_media_sandbox_production.entities e on e.id = s.publisher_id
left join intent_media_sandbox_production.legal_page_types lpt on lpt.id = au.legal_page_type_id

union

----------------------------------------HOTELS-----------------------------------------------------------------------------------------

/* HOTELS Placement Level Breakdown */
Select i.*, p. Placement, p."Placement is Prechecked", p."Clicks from Placement", p."Conversions From Placement", p."Gross Media Revenue from Placement", p."Interactions from Placement", p."Conversion Value Sum"
from intent_media_sandbox_production.sca_dash_hotel i
LEFT join
	Intent_media_sandbox_production.sca_dash_hotel_placement p

on i.Date = p.date_in_et and i.Country = p.Country and i."Ad Unit ID" = p.ad_unit_id
left join intent_media_sandbox_production.ad_units au on i."Ad Unit ID" = au.id
left join intent_media_sandbox_production.sites s on au.site_id = s.id
left join intent_media_sandbox_production.entities e on e.id = s.publisher_id
left join intent_media_sandbox_production.legal_page_types lpt on lpt.id = au.legal_page_type_id

union

/* HOTELS Total */
Select i.*, 'Total' as Placement, 'Total Placement' as "Placement is Prechecked", p."Clicks from Placement", p."Conversions From Placement", p."Gross Media Revenue from Placement", p."Interactions from Placement", p."Conversion Value Sum"
from intent_media_sandbox_production.sca_dash_hotel i
LEFT join
	(
	Select date_in_et, country, ad_unit_id, SUM("Gross Media Revenue from Placement") as "Gross Media Revenue from Placement", SUM("Conversions From Placement") as "Conversions From Placement", SUM("Clicks from Placement") as "Clicks from Placement", 
	       SUM("Conversion Value Sum") as "Conversion Value Sum", SUM("Interactions from Placement") as "Interactions from Placement" 
	FROM intent_media_sandbox_production.sca_dash_hotel_placement
	GROUP BY date_in_et, country, ad_unit_id 
	) p
on i.Date = p.date_in_et and i.Country = p.Country and i."Ad Unit ID" = p.ad_unit_id
left join intent_media_sandbox_production.ad_units au on i."Ad Unit ID" = au.id
left join intent_media_sandbox_production.sites s on au.site_id = s.id
left join intent_media_sandbox_production.entities e on e.id = s.publisher_id
left join intent_media_sandbox_production.legal_page_types lpt on lpt.id = au.legal_page_type_id

union

/* Hotels Placment Total */
Select i.*,  p.Placement, 'Total Placement' as "Placement is Prechecked", p."Clicks from Placement", p."Conversions From Placement", p."Gross Media Revenue from Placement", p."Interactions from Placement", p."Conversion Value Sum"
from intent_media_sandbox_production.sca_dash_hotel i
LEFT join
	(
	Select date_in_et, country, ad_unit_id, placement, SUM("Gross Media Revenue from Placement") as "Gross Media Revenue from Placement", SUM("Conversions From Placement") as "Conversions From Placement", SUM("Clicks from Placement") as "Clicks from Placement", 
	       SUM("Conversion Value Sum") as "Conversion Value Sum", SUM("Interactions from Placement") as "Interactions from Placement" 
	FROM intent_media_sandbox_production.sca_dash_hotel_placement
	GROUP BY date_in_et, country, ad_unit_id, placement
	) p
on i.Date = p.date_in_et and i.Country = p.Country and i."Ad Unit ID" = p.ad_unit_id
left join intent_media_sandbox_production.ad_units au on i."Ad Unit ID" = au.id
left join intent_media_sandbox_production.sites s on au.site_id = s.id
left join intent_media_sandbox_production.entities e on e.id = s.publisher_id
left join intent_media_sandbox_production.legal_page_types lpt on lpt.id = au.legal_page_type_id