-- STAGING LAYER (silver)
-- Typed, normalised, de-duplicated data, rebuilt in full from raw by
-- `python -m mpesa_pipeline build-staging` (see sql/staging/).
--
-- Rows that fail validation are not deleted. They go to a quarantine table with
-- the reasons attached, so every raw receipt ends up in exactly one place:
--
--     unique raw receipts = staging.mpesa_transactions + staging.mpesa_transactions_quarantine
--
-- The CHECK constraints below repeat the cleaning rules. They are a second line
-- of defence: if a bug in the transform lets a bad value through, the build
-- fails instead of quietly writing it.

-- Safe casts: return NULL instead of raising, so one bad value sends one row
-- to quarantine rather than aborting the whole build.
CREATE OR REPLACE FUNCTION staging.try_numeric(v TEXT) RETURNS NUMERIC
LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    RETURN v::NUMERIC;
EXCEPTION WHEN others THEN
    RETURN NULL;
END $$;

-- Only call this on ISO-shaped strings (YYYY-MM-DD HH:MI:SS). Other shapes,
-- e.g. 01/10/2026, would be read according to the server's DateStyle setting.
CREATE OR REPLACE FUNCTION staging.try_timestamp(v TEXT) RETURNS TIMESTAMP
LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    RETURN v::TIMESTAMP;
EXCEPTION WHEN others THEN
    RETURN NULL;
END $$;

CREATE TABLE IF NOT EXISTS staging.shops (
    till_number        TEXT PRIMARY KEY,   -- an identifier, not a number: never do arithmetic on it
    shop_name          TEXT NOT NULL,
    business_category  TEXT NOT NULL,
    town               TEXT NOT NULL,
    county             TEXT NOT NULL,
    opened_on          DATE,
    _source_file       TEXT NOT NULL,
    _source_row        INTEGER NOT NULL
);

-- One row per receipt (the PRIMARY KEY enforces the de-duplication).
CREATE TABLE IF NOT EXISTS staging.mpesa_transactions (
    receipt_no         TEXT PRIMARY KEY,
    -- Nairobi local wall-clock time, stored WITHOUT a time zone on purpose.
    -- TIMESTAMPTZ would be shifted by whatever the session time zone is.
    completion_time    TIMESTAMP NOT NULL,
    event_date         DATE NOT NULL,      -- the business date: use this, never the file date
    till_number        TEXT NOT NULL,
    shop_name          TEXT NOT NULL,
    customer_msisdn    TEXT NOT NULL CHECK (customer_msisdn ~ '^254(7\d{8}|11\d{7})$'),
    customer_name      TEXT,               -- NULL when the source left it blank
    amount             NUMERIC(12, 2) NOT NULL CHECK (amount <> 0),
    transaction_type   TEXT NOT NULL CHECK (transaction_type IN ('Buy Goods', 'Reversal')),
    linked_receipt_no  TEXT,               -- reversals only: the receipt being reversed
    is_orphan_till     BOOLEAN NOT NULL,   -- till missing from shop master. Kept, not quarantined: the money is real
    is_late_arrival    BOOLEAN NOT NULL,   -- arrived in a later file than its event date
    _source_file       TEXT NOT NULL,      -- lineage of the copy that was kept
    _source_row        INTEGER NOT NULL,
    _built_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Rejected rows, exactly as they arrived in raw (TEXT), plus why they were rejected.
-- reasons is an array so a row with two problems shows both.
CREATE TABLE IF NOT EXISTS staging.mpesa_transactions_quarantine (
    receipt_no         TEXT,
    completion_time    TEXT,
    till_number        TEXT,
    shop_name          TEXT,
    customer_msisdn    TEXT,
    customer_name      TEXT,
    amount             TEXT,
    transaction_type   TEXT,
    linked_receipt_no  TEXT,
    reasons            TEXT[] NOT NULL CHECK (cardinality(reasons) > 0),
    is_orphan_till     BOOLEAN NOT NULL,
    is_late_arrival    BOOLEAN,            -- NULL when the timestamp could not be parsed
    _source_file       TEXT NOT NULL,
    _source_row        INTEGER NOT NULL,
    _built_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
