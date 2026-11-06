-- dim_date: one row per calendar day, generated (not loaded from any source).
--
-- It covers whole months from the first to the last transaction month, so a day
-- with zero sales still exists as a row. Reports can then show "0 on 12 Oct"
-- instead of silently skipping that day.
--
-- ON CONFLICT DO NOTHING: days that already exist are left alone, new ones are
-- added. Re-running is safe, and the table only ever grows.

INSERT INTO warehouse.dim_date
    (date_key, full_date, day_of_month, day_name, iso_day_of_week, is_weekend,
     is_payday_window, month, month_name, year)
SELECT to_char(d, 'YYYYMMDD')::INTEGER,
       d,
       EXTRACT(DAY FROM d)::INTEGER,
       to_char(d, 'FMDay'),                          -- FM = no padding spaces: 'Monday', not 'Monday   '
       EXTRACT(ISODOW FROM d)::INTEGER,
       EXTRACT(ISODOW FROM d) IN (6, 7),
       EXTRACT(DAY FROM d) >= 28 OR EXTRACT(DAY FROM d) <= 3,
       EXTRACT(MONTH FROM d)::INTEGER,
       to_char(d, 'FMMonth'),
       EXTRACT(YEAR FROM d)::INTEGER
FROM (
    -- generate_series(start, stop, step) produces one row per value in the range.
    -- With no transactions, MIN/MAX are NULL and this yields zero rows.
    SELECT generate_series(
               date_trunc('month', MIN(event_date)),
               date_trunc('month', MAX(event_date)) + INTERVAL '1 month - 1 day',
               INTERVAL '1 day'
           )::DATE AS d
    FROM staging.mpesa_transactions
) days
ON CONFLICT (date_key) DO NOTHING;
