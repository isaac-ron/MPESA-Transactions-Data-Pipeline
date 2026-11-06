-- dim_shop: SCD Type 2 merge of the latest shop snapshot into the dimension.
--
-- This table is NOT rebuilt from scratch, unlike staging and the fact table. The
-- source only ever sends today's snapshot (raw.shops is a full refresh), so the
-- history of past versions exists nowhere else. Truncating this table would
-- destroy it.
--
-- The merge has two steps:
--   A. CLOSE: for each till whose details changed, end its current version.
--   B. OPEN:  for each till with no current version (new till, or just closed
--             in step A), insert one.
-- With an unchanged snapshot both steps do nothing, so re-running is safe.

-- When did we see this snapshot? We can't know when a shop really moved town,
-- only when a snapshot first showed it. So the snapshot's ingestion time is
-- the change time. It's converted from TIMESTAMPTZ to Nairobi local time so it
-- can be compared with completion_time (also Nairobi local, see Stage 2).
CREATE TEMP TABLE _snapshot ON COMMIT DROP AS
SELECT s.*,
       (SELECT MAX(_ingested_at) FROM raw.shops) AT TIME ZONE 'Africa/Nairobi' AS seen_at
FROM staging.shops s;

-- A. CLOSE changed versions.
-- UPDATE ... FROM joins the table being updated with another table.
-- (a, b, c) IS DISTINCT FROM (x, y, z) compares whole rows and, unlike <>,
-- treats NULL as a value: NULL <> 'x' is NULL (not true), so a plain <>
-- would miss a change from NULL to a value.
UPDATE warehouse.dim_shop d
SET valid_to   = s.seen_at,
    is_current = FALSE
FROM _snapshot s
WHERE d.till_number = s.till_number
  AND d.is_current
  AND (d.shop_name, d.business_category, d.town, d.county, d.opened_on)
      IS DISTINCT FROM
      (s.shop_name, s.business_category, s.town, s.county, s.opened_on);

-- B. OPEN a version for every till that has no current one.
-- valid_from: a till we've never seen gets '-infinity' ("assume it always looked
-- like this"), so sales from before the first snapshot still find it. A till
-- with older versions starts its new version at the snapshot time, exactly where
-- step A ended the old one.
INSERT INTO warehouse.dim_shop
    (till_number, shop_name, business_category, town, county, opened_on, valid_from, valid_to, is_current)
SELECT s.till_number, s.shop_name, s.business_category, s.town, s.county, s.opened_on,
       CASE WHEN EXISTS (SELECT 1 FROM warehouse.dim_shop old WHERE old.till_number = s.till_number)
            THEN s.seen_at
            ELSE '-infinity'
       END,
       'infinity',
       TRUE
FROM _snapshot s
WHERE NOT EXISTS (
    SELECT 1 FROM warehouse.dim_shop cur
    WHERE cur.till_number = s.till_number AND cur.is_current
);

-- Tills that disappear from the snapshot are left as they are (still current).
-- A shop closing is a business event the source doesn't send. Guessing it from a
-- missing row would turn one bad snapshot into "every shop closed".
