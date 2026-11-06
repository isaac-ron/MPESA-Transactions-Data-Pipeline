-- Every clean reversal points at a receipt we have seen. The lookup covers
-- quarantined receipts too: a valid reversal is not an error just because its
-- original was quarantined (e.g. for an invalid amount).
SELECT r.receipt_no, r.linked_receipt_no
FROM staging.mpesa_transactions r
WHERE r.transaction_type = 'Reversal'
  AND (r.linked_receipt_no IS NULL
       OR r.linked_receipt_no NOT IN (
           SELECT receipt_no FROM staging.mpesa_transactions
           UNION
           SELECT btrim(receipt_no) FROM staging.mpesa_transactions_quarantine
           WHERE receipt_no IS NOT NULL))
