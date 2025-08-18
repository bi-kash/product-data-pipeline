# SQL Queries for Data Analysis

## Merchant Status Analysis

-- Get counts of merchants by approval status
SELECT approval_status, COUNT(\*) AS count
FROM sellers
GROUP BY approval_status;

-- Get merchants found in the last 24 hours
SELECT COUNT(\*)
FROM sellers
WHERE first_seen_at >= NOW() - INTERVAL '24 hours';

## Job Performance Analysis

-- Get average counts for harvest jobs
SELECT
AVG(found_count) AS avg_found,
AVG(new_count) AS avg_new,
AVG(duration_seconds) AS avg_duration_seconds
FROM job_runs
WHERE job_type = 'harvest';

-- Get job success rate
SELECT
COUNT(_) AS total_jobs,
SUM(CASE WHEN error_count = 0 THEN 1 ELSE 0 END) AS successful_jobs,
ROUND(SUM(CASE WHEN error_count = 0 THEN 1 ELSE 0 END)::numeric / COUNT(_)::numeric \* 100, 2) AS success_rate
FROM job_runs;
