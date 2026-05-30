# Differences from `sqlite3`

`dqlite-dbapi` is shaped like the stdlib `sqlite3` module, but it talks to a
replicated cluster over the network rather than a local file. This page
covers the differences you are most likely to notice when porting code.

## Things you'll likely notice

- **Autocommit by default.** No implicit `BEGIN` before DML — the opposite
  of stdlib's implicit-transaction model. See
  [Transactions](transactions.md).
- **One statement per `execute()`.** `cursor.execute("SELECT 1; SELECT 2;")`
  raises `ProgrammingError` (client-side, before any network round-trip).
  Split into separate calls. There is no `executescript()` — it raises
  `NotSupportedError`.
- **`qmark` parameters only.** Use `?` placeholders. Stdlib also accepts
  `:name`; this driver advertises only `qmark` and rejects mappings with
  `ProgrammingError`.
- **Foreign keys are enforced by default.** dqlite defaults every fresh
  connection to `PRAGMA foreign_keys = ON` — the opposite of stdlib/pysqlite,
  which default `OFF`. Issue `PRAGMA foreign_keys = OFF` per connection for
  SQLite's legacy unenforced behavior.
- **SERIALIZABLE isolation only.** Every statement is ordered by Raft;
  weaker isolation levels are not exposed.
- **Result sets are fully materialized at `execute()` time.** Stdlib streams
  rows lazily from the C engine; dqlite drains every continuation frame into
  the cursor's in-memory buffer before `execute()` returns, and `fetchone()`
  / `fetchmany()` / `fetchall()` iterate that buffer. For large queries, cap
  memory with `max_total_rows` (forwarded from `connect()`) or shape queries
  with an explicit `LIMIT`.
- **No SQLite extension APIs.** `create_function`, `create_aggregate`,
  `create_window_function`, `iterdump`, `backup`, `set_authorizer`,
  `serialize`, `blobopen`, `executescript`, and `register_converter` have no
  server-side counterpart in dqlite; their stubs raise `NotSupportedError`.
- **`rowcount` after SELECT is `len(rows)`.** Stdlib returns `-1` for queries.
  dqlite knows the full result at execute time, so it reports the count.
  `rowcount` stays `-1` for true non-result paths (PRAGMA write, a
  never-executed cursor).

## NULL in BOOLEAN/DATETIME columns depends on the server version

A 2026 dqlite server change (upstream commit `f30fc99`) changed how a NULL
cell is encoded in columns declared `BOOLEAN`, `DATE`, `DATETIME`, or
`TIMESTAMP`:

- **Before**: a NULL came back as `False` (for `BOOLEAN`) or `""` (for
  datetime types) — indistinguishable from a real value.
- **After**: a NULL comes back as `None`.

There is no handshake that distinguishes the two server versions, and the
driver decodes faithfully whatever the server sends. Code like
`if row[0] is None:` will silently miss rows on an old cluster and start
matching after an upgrade. Check your dqlite cluster version before relying
on `is None` for these columns.

## Minor / edge-case differences

You can usually ignore these, but they are here so nothing surprises you:

- **`lastrowid` is `None` (not `0`) before the first INSERT.** After the
  first INSERT both drivers agree and keep the value sticky across
  UPDATE/DELETE/DDL.
- **`fetchmany(0)` returns `[]`** (PEP 249 read literally). Pass `None` or
  omit the argument to default to `arraysize`. `fetchmany(negative)` raises
  `ProgrammingError`.
- **`execute("")` raises `ProgrammingError`** (empty/whitespace/comment-only
  SQL). Stdlib treats it as a no-op.
- **Empty result sets report the `UNKNOWN` type code** in
  `description[i][1]` (importable from `dqlitedbapi`). Test it with
  `type_code == UNKNOWN`; for column types on an empty result, query
  `PRAGMA table_info(...)`.
- **CTE-prefixed pure DML** (`WITH ... INSERT/UPDATE/DELETE`) is
  misclassified as row-returning, so `rowcount` comes back `-1` and
  `lastrowid` is stale. Use plain DML, or `INSERT ... RETURNING id`.

### Type-code sentinels: compare with `==`, not `in {}`

The PEP 249 type sentinels (`STRING`, `BINARY`, `NUMBER`, `DATETIME`,
`ROWID`) match a `description` type code through `__eq__`, and each wraps
several wire types (`NUMBER` covers INTEGER+FLOAT+BOOLEAN; `DATETIME` covers
DATE+TIMESTAMP+ISO8601). A bare wire-int type code does not *hash* equal to a
sentinel, so set/dict membership silently returns `False`:

```python
type_code = cur.description[i][1]
if type_code == STRING or type_code == NUMBER:   # correct
    ...
if type_code in {STRING, NUMBER}:                # WRONG: silently False
    ...
```

Stdlib `sqlite3` doesn't export these sentinels at all, so the
chained-equality form is also the cross-driver-portable idiom.
