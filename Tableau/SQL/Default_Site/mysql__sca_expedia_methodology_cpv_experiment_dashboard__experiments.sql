select
    id as `Experiment ID`,
    name as `Experiment Name`,
	ifnull(product_category_type, 'Total') as `Experiment Product Category Type`,
    experiment_start as `Start Date`,
    experiment_end as `End Date`
from model_slices_experiments
    