# Import Executor

`internal/library/import.ImportExecutor` is the write-side companion to the
Import Pipeline v2 planner.

Production Librarr still defaults to the legacy importer, but the normalized
executor is now reachable behind `LIBRARR_IMPORT_ENGINE=v2`. The executor exists
so completed downloads, manual import, and library-scan imports can use the same
planner/executor path without bypassing `LibraryService`.

## Execution Flow

```text
ImportPlan
  ↓
ImportExecutor
  ↓
LibraryService
  ↓
Normalized repositories
  ↓
books / editions / contributors / files / identifiers / covers
```

For each plan the executor:

1. Re-resolves the target book inside a transaction.
2. Re-resolves the target edition inside the same transaction.
3. Re-checks duplicate path, source ID, content hash, and same-format conflicts.
4. Attaches contributors idempotently.
5. Persists identifiers when present.
6. Attaches the file and saves embedded metadata.

Plans already marked as `ignore_duplicate_file`, `conflict`, or
`needs_manual_review` are not written.

## Transaction Model

- One transaction per `ImportPlan`.
- Batch execution is a sequence of per-plan transactions.
- A failure in one plan does not roll back a previously committed plan.
- A failure inside one plan rolls back every write from that plan.

Transaction ownership remains in `LibraryService` / repository wiring.
`ImportExecutor` only calls `LibraryService.WithinTransaction(...)`.

## Idempotency Strategy

The executor does not trust the planner’s earlier snapshot alone. Before
creating anything it re-checks the normalized model by:

- trusted identifiers
- exact title plus author matching
- exact edition title under the selected book
- file path
- file source ID
- file content hash
- existing formats already attached to the book

This means the same `ImportPlan` can be executed twice and converge on the same
final state without creating duplicate books, editions, contributors, or files.

## Rollback Behavior

If any write fails:

- the transaction is rolled back
- the result status becomes `rolled_back`
- later plans in the batch may still continue

Typical rollback causes include invalid file payloads, conflicting identifiers,
or repository validation failures.

## Production Wiring

Current production wiring is feature-flagged:

```text
LIBRARR_IMPORT_ENGINE=legacy  # default rollback-safe path
LIBRARR_IMPORT_ENGINE=v2      # planner + executor + normalized repository
```

The v2 engine is used by completed torrent imports, completed direct downloads,
manual import, and explicit imports from library scan review results when the
flag is enabled.

The executor deliberately has no direct dependency on `sql.DB` or repository
implementations. It writes only through `LibraryService`.

## Remaining Cutover Work

- Continue dogfooding real-world scan/review/import flows.
- Keep improving manual-review edge cases.
- Make the v2 engine the default only after operational validation.
