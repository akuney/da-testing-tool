select 
-- transaction_id,
-- transaction_start,
transaction_description,
statement_start,
-- statement_id,
current_statement,
last_statement,
last_statement_duration_us
from sessions
where statement_id is not null
;