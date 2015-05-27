SELECT
	DATE(convert_tz(start_date,'UTC','America/New_York')) AS DATE,
	NAME AS job_name,
	round(sum(total_cost),2) AS total_cost,
	round(AVG(total_cost),2) AS average_cost,
	count(1) AS job_runs
FROM emr_job_statistics
WHERE start_date IS NOT NULL
GROUP BY
	DATE(convert_tz(start_date,'UTC','America/New_York')),
	NAME