-- fact_transactions: full rebuild from staging, looking up each dimension's key.
--
-- Facts ARE rebuilt from scratch: staging holds every transaction, so nothing is
-- lost. A rebuild also re-runs every key lookup, which means that if a missing
-- shop is later added to the shop master, its orphan transactions move from
-- "Unknown shop" to the real shop automatically (a "late-arriving dimension").
--
-- Every join is a LEFT JOIN, so no transaction can silently disappear:
--   * shop: no match -> COALESCE to key 0, the Unknown shop row.
--   * customer: no match -> NULL key -> the NOT NULL constraint fails the build.
--     It should be impossible (dim_customer is built from these same rows),
--     and if it ever happens we want an error, not a quietly smaller fact table.
--     An INNER JOIN would have dropped the row without a word.

TRUNCATE warehouse.fact_transactions;

INSERT INTO warehouse.fact_transactions
    (receipt_no, date_key, shop_key, customer_key, completion_time, transaction_type, amount, reversed_receipt_no)
SELECT t.receipt_no,
       to_char(t.event_date, 'YYYYMMDD')::INTEGER,   -- smart key: computed, no join needed
       COALESCE(s.shop_key, 0),
       c.customer_key,
       t.completion_time,
       t.transaction_type,
       t.amount,
       t.linked_receipt_no
FROM staging.mpesa_transactions t
-- The SCD Type 2 lookup: same till AND the version that was valid when the sale
-- happened. The join condition is a range, not just an equality.
LEFT JOIN warehouse.dim_shop s
       ON s.till_number = t.till_number
      AND t.completion_time >= s.valid_from
      AND t.completion_time <  s.valid_to
LEFT JOIN warehouse.dim_customer c
       ON c.customer_msisdn = t.customer_msisdn;
