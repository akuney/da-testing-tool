select 
	left(u.email, locate('@', u.email, 1) - 1) as user,
	s.name as site_type,
	case mv.ad_type when 'CT' then 'AfT' when 'SSR' then 'SSN' when 'META' then 'Meta' when 'RETARGETING' then 'Retargeting' else 'Other' end as ad_unit_type,
	mv.id as multivariate_version_id,
	mv.description as multivariate_version_description,
	date(mv.created_at) as change_date
from multivariate_versions mv
inner join users u on mv.user_id = u.id
inner join sites s on mv.site_id = s.id