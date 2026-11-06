-- Every clean staging transaction is in the fact table exactly once, and the
-- money adds up. Matching row counts alone could hide a wrong amount; reconcile
-- the totals you report on, not just the row count.
WITH s AS (SELECT COUNT(*) AS n, COALESCE(SUM(amount), 0) AS total FROM staging.mpesa_transactions),
     f AS (SELECT COUNT(*) AS n, COALESCE(SUM(amount), 0) AS total FROM warehouse.fact_transactions)
SELECT s.n AS staging_rows, f.n AS fact_rows, s.total AS staging_amount, f.total AS fact_amount
FROM s, f
WHERE s.n <> f.n OR s.total <> f.total
