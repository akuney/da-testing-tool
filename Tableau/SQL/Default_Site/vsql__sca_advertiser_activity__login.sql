select
	u.id as "User ID",
	email as Email,
	first_name as "First Name",
	last_name as "Last Name",
	login_count as "Total Login Count",
	last_request_at as "Last Requested At",
	current_login_at as "Current Login At",
	last_login_at as "Last Login At",
	e.name as "Advertiser Name",
	(case when u.email like '%intentmedia.com%' then 'Internal' else 'External' end) as "User Type"
from intent_media_production.users u
left join intent_media_production.memberships m on m.user_id = u.id
left join intent_media_production.entities e on e.id = m.entity_id
where e.entity_type = 'AftAdvertiser'
and e.active = 1
and m.active = 1