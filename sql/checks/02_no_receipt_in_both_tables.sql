-- A receipt is either clean or quarantined, never both.
SELECT t.receipt_no
FROM staging.mpesa_transactions t
JOIN staging.mpesa_transactions_quarantine q ON btrim(q.receipt_no) = t.receipt_no
