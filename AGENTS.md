# AGENTS.md

This file provides guidance to coding agents working in the `importer/` submodule.

## QuickBooks Order Number Caveat

QuickBooks `order_number` values are not globally unique in this dataset.

We confirmed that the raw QuickBooks imports contain real duplicate printed document numbers across different transactions:

- Duplicate invoice numbers exist in `raw.xlsx_invoice`.
- Duplicate sales receipt numbers exist in `raw.xlsx_sales_receipt`.
- These are true source-data collisions, not only downstream modeling bugs.

Examples discovered during investigation:

- Invoice `A4453` maps to different customers and different QuickBooks transaction identifiers.
- Sales receipt `3D-3218` maps to different customers and different QuickBooks internal IDs.

## Practical Guidance

- Do not assume `order_number` alone is a stable unique key for QuickBooks data.
- Prefer `quick_books_internal_id` as the strongest transaction identifier when available.
- For invoices, `transaction_id` (`transxx`) is also useful for distinguishing collisions.
- If a model must be one row per business transaction, do not group only by `order_number`.
- If a model remains keyed by `order_number` for reporting convenience, document that this may merge a small number of bad source records.

## Accepted Current State

For now, these duplicate source records are an accepted data-quality issue.

- The bookkeeping team will address the source data in QuickBooks.
- We should avoid spending more time re-debugging these same duplicates unless the user explicitly asks.
- Minor reporting distortion from this limited set of records is currently acceptable.

## Separate Issue: Load Deduping

Some records may also appear multiple times across seed and incremental loads with the same QuickBooks identifiers.

- This is a different issue from true reused printed order numbers.
- Example previously observed: invoice `A7218`.
- Treat load deduping problems separately from true source-number collisions.
