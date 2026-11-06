-- STAGE 3: ANALYTICS ON THE STAR SCHEMA
-- Run these in Adminer (http://localhost:8080), one at a time.
--
-- Almost every star-schema query has the same shape:
--   FROM the fact table, JOIN the dimensions you need,
--   WHERE / GROUP BY dimension attributes, SUM / COUNT the fact's measures.
-- Learn that shape once and most business questions become easy to write.

-- 1. Daily net revenue, gross sales and reversals, for EVERY day of the month.
--    Starts FROM dim_date and LEFT JOINs the facts, so a day with no sales
--    (31 Oct here) still appears, with 0. Start from the fact table and that day
--    silently vanishes: there are no fact rows to group.
--    FILTER (WHERE ...) restricts one aggregate to some rows, without a separate query.
--    COUNT(f.receipt_no) counts only matched facts; COUNT(*) would count 1 for an empty day.
SELECT d.full_date,
       d.day_name,
       COALESCE(SUM(f.amount), 0)                                               AS net_revenue,
       COALESCE(SUM(f.amount) FILTER (WHERE f.transaction_type = 'Buy Goods'), 0) AS gross_sales,
       COALESCE(-SUM(f.amount) FILTER (WHERE f.transaction_type = 'Reversal'), 0) AS reversed,
       COUNT(f.receipt_no)                                                      AS transactions
FROM warehouse.dim_date d
LEFT JOIN warehouse.fact_transactions f ON f.date_key = d.date_key
GROUP BY d.full_date, d.day_name
ORDER BY d.full_date;

-- 2. Revenue by county and business category. The Unknown shop appears as its
--    own line: orphan revenue is counted, and visibly unattributed.
SELECT s.county, s.business_category, SUM(f.amount) AS net_revenue, COUNT(*) AS transactions
FROM warehouse.fact_transactions f
JOIN warehouse.dim_shop s ON s.shop_key = f.shop_key
GROUP BY s.county, s.business_category
ORDER BY net_revenue DESC;

-- 3. Does payday week matter? Average ticket and daily volume inside vs outside
--    the payday window. Dividing by COUNT(DISTINCT date) gives a per-day figure,
--    since the two windows contain different numbers of days.
SELECT d.is_payday_window,
       ROUND(AVG(f.amount) FILTER (WHERE f.transaction_type = 'Buy Goods'), 2) AS avg_ticket,
       ROUND(COUNT(*)::NUMERIC / COUNT(DISTINCT d.date_key), 1)               AS transactions_per_day
FROM warehouse.fact_transactions f
JOIN warehouse.dim_date d ON d.date_key = f.date_key
GROUP BY d.is_payday_window;

-- 4. Weekday pattern for restaurants (busier on Sundays, when other shops are quieter).
--    Compare per-day AVERAGES, not raw counts: 1-30 Oct has five Thursdays and
--    Fridays but four of every other weekday. Pick the right denominator.
SELECT s.business_category, d.iso_day_of_week, d.day_name,
       ROUND(COUNT(*)::NUMERIC / COUNT(DISTINCT d.date_key), 1) AS transactions_per_day
FROM warehouse.fact_transactions f
JOIN warehouse.dim_date d ON d.date_key = f.date_key
JOIN warehouse.dim_shop s ON s.shop_key = f.shop_key
WHERE s.business_category = 'Restaurant'
GROUP BY s.business_category, d.iso_day_of_week, d.day_name
ORDER BY d.iso_day_of_week;

-- 5. Rank shops within their county by revenue.
--    A window function (RANK() OVER ...) adds a column computed across a group of
--    rows WITHOUT collapsing them, unlike GROUP BY. PARTITION BY restarts the ranking
--    for each county. The inner query aggregates first; the outer one ranks the results.
SELECT county, shop_name, net_revenue,
       RANK() OVER (PARTITION BY county ORDER BY net_revenue DESC) AS rank_in_county
FROM (
    SELECT s.county, s.shop_name, SUM(f.amount) AS net_revenue
    FROM warehouse.fact_transactions f
    JOIN warehouse.dim_shop s ON s.shop_key = f.shop_key
    WHERE s.shop_key <> 0
    GROUP BY s.county, s.shop_name
) per_shop
ORDER BY county, rank_in_county;

-- 6. Running (cumulative) revenue per shop through the month.
--    SUM(...) OVER (PARTITION BY shop ORDER BY date) adds up all rows so far,
--    in date order, within each shop.
SELECT shop_name, full_date, daily_revenue,
       SUM(daily_revenue) OVER (PARTITION BY shop_name ORDER BY full_date) AS month_to_date
FROM (
    SELECT s.shop_name, d.full_date, SUM(f.amount) AS daily_revenue
    FROM warehouse.fact_transactions f
    JOIN warehouse.dim_shop s ON s.shop_key = f.shop_key
    JOIN warehouse.dim_date d ON d.date_key = f.date_key
    WHERE s.shop_key <> 0
    GROUP BY s.shop_name, d.full_date
) daily
ORDER BY shop_name, full_date;

-- 7. Customer loyalty: how many shops does each customer use, and how often?
--    Aggregate twice: first per customer, then count customers per bucket.
SELECT shops_used, COUNT(*) AS customers, ROUND(AVG(visits), 1) AS avg_visits
FROM (
    SELECT f.customer_key, COUNT(DISTINCT f.shop_key) AS shops_used, COUNT(*) AS visits
    FROM warehouse.fact_transactions f
    WHERE f.transaction_type = 'Buy Goods'
    GROUP BY f.customer_key
) per_customer
GROUP BY shops_used
ORDER BY shops_used;

-- 8. Reversal rate per shop, as a share of sales count.
--    NULLIF(x, 0) avoids division by zero (dividing by NULL gives NULL, not an error).
SELECT s.shop_name,
       COUNT(*) FILTER (WHERE f.transaction_type = 'Reversal')  AS reversals,
       COUNT(*) FILTER (WHERE f.transaction_type = 'Buy Goods') AS sales,
       ROUND(100.0 * COUNT(*) FILTER (WHERE f.transaction_type = 'Reversal')
             / NULLIF(COUNT(*) FILTER (WHERE f.transaction_type = 'Buy Goods'), 0), 2) AS reversal_pct
FROM warehouse.fact_transactions f
JOIN warehouse.dim_shop s ON s.shop_key = f.shop_key
GROUP BY s.shop_name
ORDER BY reversal_pct DESC NULLS LAST;

-- 9. SCD Type 2 in action: every version of every shop (after the experiment
--    in the README there will be two rows for the till you changed).
SELECT shop_key, till_number, shop_name, town, county, valid_from, valid_to, is_current
FROM warehouse.dim_shop
ORDER BY till_number, valid_from;
