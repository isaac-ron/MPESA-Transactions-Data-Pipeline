-- Transactions: raw.mpesa_transactions -> staging.mpesa_transactions (+ quarantine).
--
-- Order matters:
--   1. DE-DUPLICATE first. Duplicates are exact copies made after the source's
--      mess was applied, so if we validated first, a duplicated bad row would be
--      counted twice.
--   2. NORMALISE every column into a candidate typed value.
--   3. VALIDATE: collect every failed rule into a reasons array.
--   4. SPLIT: no reasons -> staging; any reason -> quarantine.
--
-- Full rebuild (TRUNCATE + INSERT ... SELECT) inside one transaction: re-running
-- it is safe by construction, and readers never see a half-built table.

TRUNCATE staging.mpesa_transactions, staging.mpesa_transactions_quarantine;

-- 1. De-duplicate on the business key. Keep the earliest delivered copy:
--    file names sort by date, and _source_row orders within a file.
--    A missing receipt_no falls back to its lineage so those rows aren't merged together.
CREATE TEMP TABLE _deduped ON COMMIT DROP AS
SELECT *
FROM (
    SELECT r.*,
           ROW_NUMBER() OVER (
               PARTITION BY COALESCE(NULLIF(btrim(r.receipt_no), ''), r._source_file || ':' || r._source_row)
               ORDER BY r._source_file, r._source_row
           ) AS copy_no
    FROM raw.mpesa_transactions r
) ranked
WHERE copy_no = 1;

-- 2 + 3. Normalise, then validate.
CREATE TEMP TABLE _parsed ON COMMIT DROP AS
WITH normalised AS (
    SELECT d.*,
           NULLIF(btrim(d.receipt_no), '') AS n_receipt_no,

           -- Three timestamp shapes arrive. Each is matched explicitly and rewritten
           -- to ISO; anything else becomes NULL and is quarantined.
           -- DD/MM/YYYY is day-first by contract (01/10/2026 = 1 October). That shape
           -- has no seconds, so those rows get :00. That's a real loss of precision
           -- in the source, not a bug here.
           -- The result is a plain TIMESTAMP (Nairobi local time). to_timestamp() is
           -- avoided because it returns TIMESTAMPTZ in the session time zone.
           staging.try_timestamp(CASE
               WHEN btrim(d.completion_time) ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}$'
                   THEN replace(btrim(d.completion_time), 'T', ' ')
               WHEN btrim(d.completion_time) ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
                   THEN regexp_replace(btrim(d.completion_time),
                                       '^(\d{2})/(\d{2})/(\d{4}) (\d{2}:\d{2})$', '\3-\2-\1 \4:00')
           END) AS n_completion_time,

           NULLIF(btrim(d.till_number), '') AS n_till_number,

           -- "KES 1,200.00", "1,200", "-230.00" -> numeric. Blank -> NULL.
           staging.try_numeric(regexp_replace(btrim(d.amount), '^KES\s*|,', '', 'g')) AS n_amount,

           -- Normalise types BEFORE anything looks for reversals: REVERSAL / reversal arrive too.
           initcap(btrim(d.transaction_type)) AS n_transaction_type,

           NULLIF(btrim(d.linked_receipt_no), '') AS n_linked_receipt_no,

           -- Trim, collapse repeated spaces, Title Case. Blank -> NULL (not quarantined:
           -- a payment without a name is still a valid payment).
           NULLIF(initcap(regexp_replace(btrim(d.customer_name), '\s+', ' ', 'g')), '') AS n_customer_name,

           -- Strip spaces, "+" and "-" only (not every non-digit, which would hide garbage).
           regexp_replace(d.customer_msisdn, '[\s+-]', '', 'g') AS msisdn_digits
    FROM _deduped d
),
phones AS (
    -- Four phone formats map onto one canonical form, 254XXXXXXXXX:
    --   254712345678 (already canonical)   +254712345678 (+ already stripped above)
    --   0712345678 / 0112345678            712345678 / 112345678 (no prefix)
    -- The two real prefixes are 2547... and 25411...
    SELECT n.*,
           CASE
               WHEN msisdn_digits ~ '^254'                  THEN msisdn_digits
               WHEN msisdn_digits ~ '^0(7\d{8}|11\d{7})$'   THEN '254' || substr(msisdn_digits, 2)
               WHEN msisdn_digits ~ '^(7\d{8}|11\d{7})$'    THEN '254' || msisdn_digits
               ELSE msisdn_digits
           END AS n_customer_msisdn
    FROM normalised n
)
SELECT p.*,
       s.till_number IS NULL AS is_orphan_till,
       -- Take the shop name from the master (one spelling per till). Orphans have no
       -- master row, so their cleaned source name is the best we have.
       COALESCE(s.shop_name,
                NULLIF(initcap(regexp_replace(btrim(p.shop_name), '\s+', ' ', 'g')), ''),
                '(unknown)') AS n_shop_name,
       -- Late = event date earlier than the file it arrived in. Informational only:
       -- a reversal can also sit in an EARLIER file than its date (pushed past the
       -- last day), so there is deliberately no "event date <= file date" rule.
       p.n_completion_time::DATE
           < substring(p._source_file FROM '\d{4}-\d{2}-\d{2}')::DATE AS is_late_arrival,
       -- Validation. Validate the phone AFTER normalising, against the strict
       -- pattern, so a new bad format is rejected rather than slipped through.
       array_remove(ARRAY[
           CASE WHEN p.n_receipt_no IS NULL THEN 'missing_receipt_no' END,
           CASE WHEN p.n_completion_time IS NULL THEN 'unparseable_completion_time' END,
           CASE WHEN p.n_till_number IS NULL THEN 'missing_till_number' END,
           CASE WHEN p.n_amount IS NULL OR p.n_amount = 0 THEN 'invalid_amount' END,
           CASE WHEN (p.n_transaction_type = 'Buy Goods' AND p.n_amount < 0)
                  OR (p.n_transaction_type = 'Reversal' AND p.n_amount > 0) THEN 'amount_sign_mismatch' END,
           CASE WHEN p.n_transaction_type IS NULL
                  OR p.n_transaction_type NOT IN ('Buy Goods', 'Reversal') THEN 'unknown_transaction_type' END,
           CASE WHEN p.n_customer_msisdn IS NULL
                  OR p.n_customer_msisdn !~ '^254(7\d{8}|11\d{7})$' THEN 'invalid_msisdn' END
       ], NULL) AS reasons
FROM phones p
LEFT JOIN staging.shops s ON s.till_number = p.n_till_number;

-- 4. Split.
INSERT INTO staging.mpesa_transactions
    (receipt_no, completion_time, event_date, till_number, shop_name, customer_msisdn, customer_name,
     amount, transaction_type, linked_receipt_no, is_orphan_till, is_late_arrival, _source_file, _source_row)
SELECT n_receipt_no, n_completion_time, n_completion_time::DATE, n_till_number, n_shop_name,
       n_customer_msisdn, n_customer_name, n_amount, n_transaction_type, n_linked_receipt_no,
       is_orphan_till, is_late_arrival, _source_file, _source_row
FROM _parsed
WHERE cardinality(reasons) = 0;

INSERT INTO staging.mpesa_transactions_quarantine
    (receipt_no, completion_time, till_number, shop_name, customer_msisdn, customer_name, amount,
     transaction_type, linked_receipt_no, reasons, is_orphan_till, is_late_arrival, _source_file, _source_row)
SELECT receipt_no, completion_time, till_number, shop_name, customer_msisdn, customer_name, amount,
       transaction_type, linked_receipt_no, reasons, is_orphan_till, is_late_arrival, _source_file, _source_row
FROM _parsed
WHERE cardinality(reasons) > 0;
