-- STAGE 1: PROFILE THE RAW DATA
-- Run these in Adminer (http://localhost:8080) or psql before writing any cleaning code.
-- Each query answers "what is actually wrong with this data?". The answers
-- become the cleaning rules in Stage 2.

-- 1. What has been loaded? (the manifest)
SELECT file_name, target_table, row_count, ingested_at
FROM raw.ingestion_log
ORDER BY file_name;

-- 2. Duplicates: the same receipt appearing more than once, in the same file or across files.
SELECT receipt_no,
       COUNT(*)                                       AS times_seen,
       STRING_AGG(DISTINCT _source_file, ', ')        AS files
FROM raw.mpesa_transactions
GROUP BY receipt_no
HAVING COUNT(*) > 1
ORDER BY times_seen DESC
LIMIT 20;

-- 3. Date formats in use. Each regex is one "shape" of timestamp.
SELECT CASE
         WHEN completion_time ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$' THEN 'YYYY-MM-DD HH:MI:SS'
         WHEN completion_time ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$' THEN 'ISO with T'
         WHEN completion_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'       THEN 'DD/MM/YYYY HH:MI'
         ELSE 'OTHER'
       END AS date_shape,
       COUNT(*) AS n
FROM raw.mpesa_transactions
GROUP BY 1
ORDER BY n DESC;

-- 4. Amount shapes: plain numbers, "KES 1,200.00", "1,200", blanks, zeros.
SELECT CASE
         WHEN amount IS NULL                    THEN 'missing'
         WHEN amount ~ '^-?\d+\.\d{2}$'         THEN 'plain decimal'
         WHEN amount ~ '^KES '                  THEN 'KES prefix'
         WHEN amount ~ ','                      THEN 'thousands comma'
         WHEN amount ~ '^-?\d+$'                THEN 'plain integer'
         ELSE 'OTHER'
       END AS amount_shape,
       COUNT(*) AS n,
       MIN(amount) AS example
FROM raw.mpesa_transactions
GROUP BY 1
ORDER BY n DESC;

-- 5. Phone number formats (all should become 2547XXXXXXXX / 25411XXXXXXX).
SELECT CASE
         WHEN customer_msisdn ~ '^254\d{9}$'  THEN '254XXXXXXXXX (canonical)'
         WHEN customer_msisdn ~ '^\+254\d{9}$' THEN '+254XXXXXXXXX'
         WHEN customer_msisdn ~ '^0\d{9}$'     THEN '0XXXXXXXXX'
         WHEN customer_msisdn ~ '^\d{9}$'      THEN 'XXXXXXXXX (no prefix)'
         ELSE 'OTHER'
       END AS phone_shape,
       COUNT(*) AS n
FROM raw.mpesa_transactions
GROUP BY 1
ORDER BY n DESC;

-- 6. Orphans: transactions whose till number is not in the shop master.
SELECT t.till_number, COUNT(*) AS n_tx
FROM raw.mpesa_transactions t
LEFT JOIN raw.shops s ON s.till_number = t.till_number
WHERE s.till_number IS NULL
GROUP BY t.till_number
ORDER BY n_tx DESC
LIMIT 10;

-- 7. Late arrivals: the transaction date is earlier than the date in the file name.
SELECT _source_file,
       COUNT(*) FILTER (
         WHERE LEFT(completion_time, 10) < SUBSTRING(_source_file FROM '\d{4}-\d{2}-\d{2}')
           AND completion_time ~ '^\d{4}-'
       ) AS late_rows_iso_only,
       COUNT(*) AS total_rows
FROM raw.mpesa_transactions
GROUP BY _source_file
ORDER BY _source_file
LIMIT 10;

-- 8. Inconsistent labels: one till, several spellings of its shop name.
SELECT till_number, COUNT(DISTINCT shop_name) AS spellings, STRING_AGG(DISTINCT shop_name, ' | ') AS variants
FROM raw.mpesa_transactions
GROUP BY till_number
HAVING COUNT(DISTINCT shop_name) > 1
ORDER BY spellings DESC
LIMIT 10;

-- 9. Lineage in action: trace any raw row back to its exact line in the source file.
SELECT receipt_no, _source_file, _source_row, _ingested_at
FROM raw.mpesa_transactions
WHERE transaction_type ILIKE 'reversal'
LIMIT 5;
