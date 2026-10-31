-- RAW LAYER
-- Every business column is TEXT on purpose. The source sends values like
-- "KES 1,200.00" and three different date formats. If these columns were
-- NUMERIC or TIMESTAMP, the load would crash on the first bad row and you
-- would lose the evidence. Raw accepts everything; staging decides what is valid.
--
-- Columns starting with "_" are lineage metadata added by the pipeline.
-- They record which file and which line each row came from, and when it was loaded.

-- One row per file processed. This "manifest" is what makes re-runs safe.
CREATE TABLE IF NOT EXISTS raw.ingestion_log (
    file_name     TEXT PRIMARY KEY,
    target_table  TEXT        NOT NULL,
    file_sha256   TEXT        NOT NULL,
    row_count     INTEGER     NOT NULL,
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Daily M-Pesa till exports. Loaded incrementally: each new file is appended.
CREATE TABLE IF NOT EXISTS raw.mpesa_transactions (
    receipt_no           TEXT,
    completion_time      TEXT,
    till_number          TEXT,
    shop_name            TEXT,
    customer_msisdn      TEXT,
    customer_name        TEXT,
    amount               TEXT,
    transaction_type     TEXT,
    linked_receipt_no    TEXT,
    _source_file         TEXT        NOT NULL,
    _source_row          INTEGER     NOT NULL,
    _ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_raw_tx_source_file ON raw.mpesa_transactions (_source_file);

-- Shop master data. Sent as a full snapshot each time, so it is loaded as a
-- full refresh: the old contents are replaced.
CREATE TABLE IF NOT EXISTS raw.shops (
    till_number        TEXT,
    shop_name          TEXT,
    business_category  TEXT,
    town               TEXT,
    county             TEXT,
    opened_on          TEXT,
    _source_file       TEXT        NOT NULL,
    _source_row        INTEGER     NOT NULL,
    _ingested_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
