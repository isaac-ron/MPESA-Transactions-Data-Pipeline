-- dim_customer: SCD Type 1 upsert. One row per phone number, latest name wins.
--
-- "Upsert" = INSERT, or UPDATE if the row already exists. Existing customers keep
-- their customer_key (the fact table depends on it); only their attributes are
-- overwritten. New phone numbers get a new key.

INSERT INTO warehouse.dim_customer (customer_msisdn, customer_name, first_seen_date)
SELECT customer_msisdn, customer_name, first_seen_date
FROM (
    -- One row per customer: their latest transaction that has a name.
    --   ROW_NUMBER() ... = 1  picks one row per customer_msisdn (the Stage 2 dedup pattern).
    --   ORDER BY (customer_name IS NULL) sorts named rows first (FALSE sorts before TRUE),
    --     then newest first, then receipt_no as a tie-break so the choice is deterministic.
    --   MIN(...) OVER (PARTITION BY ...) is an aggregate that doesn't collapse rows:
    --     every row gets its customer's earliest date alongside it.
    SELECT customer_msisdn,
           customer_name,
           MIN(event_date) OVER (PARTITION BY customer_msisdn) AS first_seen_date,
           ROW_NUMBER() OVER (
               PARTITION BY customer_msisdn
               ORDER BY (customer_name IS NULL), completion_time DESC, receipt_no
           ) AS rn
    FROM staging.mpesa_transactions
) latest
WHERE rn = 1
-- EXCLUDED is the row we tried to insert. Type 1 = overwrite with it, except
-- don't overwrite a known name with NULL.
ON CONFLICT (customer_msisdn) DO UPDATE
SET customer_name   = COALESCE(EXCLUDED.customer_name, warehouse.dim_customer.customer_name),
    first_seen_date = LEAST(EXCLUDED.first_seen_date, warehouse.dim_customer.first_seen_date);
