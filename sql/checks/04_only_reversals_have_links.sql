-- linked_receipt_no only makes sense on a reversal.
SELECT receipt_no, transaction_type, linked_receipt_no
FROM staging.mpesa_transactions
WHERE transaction_type <> 'Reversal' AND linked_receipt_no IS NOT NULL
