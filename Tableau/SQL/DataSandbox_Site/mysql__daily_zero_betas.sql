select 
	r.updated_at as `Updated At`,
	segmentation_model_id as `Segmentation Model ID`,
	feature_type as `Feature Type`,
	weight as `Weight`,
	avg_weight as `Average Weight`,
	site_id as `Site ID`,
	product_category_type as `Product Category`,
	model_type as `Model Type`,
	s.name as Site
from
	(select * from 
		(select 
			updated_at,
			segmentation_model_id,
			feature_type,
			weight
		from 
			segmentation_model_parameters
		where weight = 0 
			and date(updated_at) = current_date()) c
	inner join
		(select 
				segmentation_model_id as sid,
				feature_type as ftype,
				sum(weight)/count(weight) as avg_weight
		from segmentation_model_parameters
		where segmentation_model_id is not null 
		group by 1,2) p
	on
		c.segmentation_model_id = p.sid
		and c.feature_type = p.ftype
		and abs(p.avg_weight) > 0.0001) r
left join
	(select 
		id,
		site_id,
		product_category_type,
		model_type
	from segmentation_models) d
on r.segmentation_model_id = d.id
left join sites s on s.id = d.site_id