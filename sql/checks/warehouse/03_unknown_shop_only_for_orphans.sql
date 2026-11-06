-- A fact points at the Unknown shop (key 0) if, and only if, staging flagged it
-- as an orphan till. Anything else means the SCD lookup missed a real shop
-- (e.g. a sale dated before a version's valid_from).
SELECT f.receipt_no, f.shop_key, t.is_orphan_till
FROM warehouse.fact_transactions f
JOIN staging.mpesa_transactions t USING (receipt_no)
WHERE (f.shop_key = 0) <> t.is_orphan_till
