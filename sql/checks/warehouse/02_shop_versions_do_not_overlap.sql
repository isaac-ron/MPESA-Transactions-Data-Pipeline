-- No till may have two versions valid at the same moment, or a sale would match
-- both and be counted twice. Two half-open ranges [a1, a2) and [b1, b2) overlap
-- exactly when a1 < b2 AND b1 < a2.
SELECT a.till_number, a.shop_key AS version_a, b.shop_key AS version_b
FROM warehouse.dim_shop a
JOIN warehouse.dim_shop b
  ON a.till_number = b.till_number
 AND a.shop_key < b.shop_key            -- each pair once, never a row with itself
 AND a.valid_from < b.valid_to
 AND b.valid_from < a.valid_to
