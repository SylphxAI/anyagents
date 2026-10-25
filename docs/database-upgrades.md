# Database upgrade histories

The published main and desk builds assigned different meanings to migration
version 14. SQL filenames are not version numbers: the migrator's integer ledger
is authoritative.

| History | v14 | v15 | v16 |
| --- | --- | --- | --- |
| main admin release | admin authority | — | — |
| desk release | RAG search | turn recovery | admin authority |

The original SQL files and ledger rows are retained. Fresh databases follow the
desk sequence, then record v17. Existing desk databases only append v17.

For a main database at v14 (or a previously interrupted upgrade at v15), the
migrator detects the existing `agents.is_admin` column and atomically applies the
idempotent RAG and turn recovery SQL, then appends v17. It does not replay the
admin `ALTER TABLE`, renumber the original v14 row, or fabricate v15/v16 ledger
rows. Admin grants, audit entries, mailbox capabilities, messages and channel
credentials are untouched. A failed bridge transaction rolls back, so the next
open can retry. Turn recovery initialization uses `INSERT OR IGNORE`, preserving
existing open turns.

All later migrations must append versions greater than 17. Never interpret gaps
in this legacy version ledger as unexecuted migrations: main upgrades deliberately
retain their original history.

On-disk upgrade tests cover main, desk, fresh databases and the interrupted main
upgrade. Reopening through `Store::open` twice must preserve the ledger timestamps,
existing records and schema. No production ledger editing is needed.

## Build identity

`anyagents --version` includes the full source commit captured at build time.
The running server's `/v1/webhooks/healthz` response includes `version` and `commit`,
so deployment verification reads the identity of the running process, not merely
the file currently installed on disk. Build deployment binaries from a clean main
checkout; a development working tree is not proof of a reproducible deployment.
