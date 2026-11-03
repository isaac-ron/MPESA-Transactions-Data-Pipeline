-- Some quarantine is normal (the source sends a few zero amounts). A sudden jump
-- in any one reason usually means the source changed something, such as a new
-- phone format, so the run fails instead of quietly quarantining a big slice of the data.
-- 2% is a tolerance knob: tune it to what is normal for your source.
WITH per_reason AS (
    SELECT reason, COUNT(*) AS n
    FROM staging.mpesa_transactions_quarantine, unnest(reasons) AS reason
    GROUP BY reason
),
total AS (
    SELECT (SELECT COUNT(*) FROM staging.mpesa_transactions)
         + (SELECT COUNT(*) FROM staging.mpesa_transactions_quarantine) AS n
)
SELECT p.reason, p.n, total.n AS total_rows, round(100.0 * p.n / total.n, 2) AS pct
FROM per_reason p, total
WHERE p.n > 0.02 * total.n
