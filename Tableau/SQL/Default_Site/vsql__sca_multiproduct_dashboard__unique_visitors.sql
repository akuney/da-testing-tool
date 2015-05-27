select
  cuva.aggregation_level_date_in_et as Date,
  s.display_name as Site,
  (case cuva.product_category_type when 'FLIGHTS' then 'Flights' when 'HOTELS' then 'Hotels' else cuva.product_category_type end) as "Product Category Type",
  au.name as "Ad Unit",
  cuva.unique_visitors as "Unique Visitors Count",
  (case when cuva.product_category_type is null and cuva.ad_unit_id is null then 'Keep' else 'Remove' end) as "Site Indicator",
  (case when cuva.product_category_type is not null and cuva.ad_unit_id is null then 'Keep' else 'Remove' end) as "Product Category Indicator",
  (case when cuva.ad_unit_id is not null then 'Keep' else 'Remove' end) as "Ad Unit Indicator"
from intent_media_production.ct_unique_visitors_aggregations cuva
left join intent_media_production.ad_units au on cuva.ad_unit_id = au.id
left join intent_media_production.sites s on cuva.site_id = s.id
left join intent_media_production.legal_page_types lpt on lpt.id = au.legal_page_type_id
left join intent_media_production.entities e on e.id = s.publisher_id