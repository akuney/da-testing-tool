select
	a.entity_id as "Advertiser ID",
	entities.name,
	a.user_id as "User ID",
	users.email,
	(case when users.email like '%intentmedia.com%' then 'Internal' else 'External' end) as "User_Type",
	a.Date,
	a.auditable_type as "Auditable Type",
	a.audited_changes as "Audited Changes"
from
	(select
		entity_id,
		user_id,
		date(convert_tz(audits.created_at,'UTC','America/New_York')) as Date,
		auditable_type,
		audited_changes
	from audits
	where date(convert_tz(created_at,'UTC','America/New_York')) >= (date(convert_tz(CURRENT_DATE(),'UTC','America/New_York')) - interval 30 day)) as a
left join users on a.user_id = users.id
left join entities on a.entity_id = entities.id