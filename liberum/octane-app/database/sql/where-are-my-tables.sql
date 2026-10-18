-- Read-only: which schema holds what, before any migration runs.
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
  AND table_name IN ('users','migrations','cache','cache_locks','jobs','job_batches',
                     'failed_jobs','sessions','password_reset_tokens','demo_records')
ORDER BY table_schema, table_name;

-- Everything currently in the octane schema.
SELECT table_name, (SELECT count(*) FROM information_schema.columns c
                    WHERE c.table_schema = t.table_schema AND c.table_name = t.table_name) AS columns
FROM information_schema.tables t
WHERE table_schema = 'octane'
ORDER BY table_name;

-- Row counts for the octane tables, to see whether they hold real data.
SELECT relname AS table_name, n_live_tup AS approx_rows
FROM pg_stat_user_tables
WHERE schemaname = 'octane'
ORDER BY relname;
