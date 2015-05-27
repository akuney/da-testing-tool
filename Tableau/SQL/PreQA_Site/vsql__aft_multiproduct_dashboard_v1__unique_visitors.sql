select
    ac.product_category_type,
    ac.requested_at_date_in_et as Date,
    (case 
        when e.name = 'Orbitz' then 'OWW' 
        when e.name = 'Expedia' then 'Expedia Inc.'
        when e.name = 'Kayak Software Corporation' then 'Kayak' 
        when e.name = 'Travelocity on Expedia' then 'Expedia Inc.'
        when e.name = 'Hotwire' then 'Expedia Inc.'
        else e.name
    end) as Publisher,
    s.display_name as Site,
    (case
        when lower(au.name) like '%exit%' then 'Total Exit Units'
        when lower(au.name) like '%list%' or lower(au.name) like '%rail%' then 'Total List Page'
        when lower(au.name) like '%media fill in%' then 'Total Media Fill In'
        when lower(au.name) like '%trip.com%' then 'Total Trip.com'
        when lower(au.name) like '%ifc%' then 'Total Integrated Form Compare'
        else au.name 
    end) as "Type of Ad Unit",
    (case au.name
        when 'Orbitz Exit Unit - Firefox' then 'Orbitz Exit Unit - Flights - From Homepage' 
        when 'CheapTickets Exit Unit - Firefox' then 'Cheaptickets Exit Unit - Flights - From Homepage'
        else au.name 
    end) as "Ad Unit",
    au.id as "Ad Unit ID",
    e.publisher_tier as "Publisher Tier",
    (case
        when au.name like '%Firefox%' then 'Exit Unit FF' 
        when au.name = 'Hotwire Media Fill In' then 'MFI'
        when au.name like '%Trip.com%' then 'Search Form'
        else 
            (case c.placement_type 
                when 'INTER_CARD' THEN 'Inter Card'
                when 'MINI_CARD' THEN 'Mini Card'
                when 'RIGHT_RAIL' THEN 'Rail'
                when 'EXIT_UNIT' THEN 'Exit Unit'
                when 'FOOTER' THEN 'Footer'
                when 'TOP_CARD' THEN 'Top Card'
                when 'FORM_COMPARE' then 'Integrated Form Compare'
                else c.placement_type
            end)
    end) as "Placement",
    count(ac.publisher_user_id) as "Unique Visitors Count"
from intent_media_log_data_production.ad_calls ac
left join intent_media_log_data_production.clicks c on ac.request_id = c.request_id
left join intent_media_production.ad_units au on au.id = ac.ad_unit_id
left join intent_media_production.legal_page_types lpt on lpt.id = au.legal_page_type_id
left join intent_media_production.sites s on au.site_id = s.id
left join intent_media_production.entities e on s.publisher_id = e.id
where ac.ip_address_blacklisted = 0
    and ac.ad_unit_type = 'CT'
group by 1,2,3,4,5,6,7,8,9