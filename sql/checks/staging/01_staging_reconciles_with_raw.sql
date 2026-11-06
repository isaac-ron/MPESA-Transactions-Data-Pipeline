-- Every unique raw receipt ends up in exactly one place: staging or quarantine.
-- If this fails, the transform silently dropped (or invented) rows.
WITH counts AS (
    SELECT (SELECT COUNT(DISTINCT COALESCE(NULLIF(btrim(receipt_no), ''), _source_file || ':' || _source_row))
              FROM raw.mpesa_transactions)                                AS raw_unique,
           (SELECT COUNT(*) FROM staging.mpesa_transactions)             AS clean,
           (SELECT COUNT(*) FROM staging.mpesa_transactions_quarantine)  AS quarantined
)
SELECT raw_unique, clean, quarantined
FROM counts
WHERE raw_unique <> clean + quarantined
