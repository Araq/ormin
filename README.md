# Ormin

Ormin is a compile-time SQL query DSL and prepared statement generator for Nim. It allows you to describe queries in Nim syntax and compiles them into type-safe, parameterized SQL. The system can generate iterators and procedures for repeated queries and has optional JSON support.

## Schema and Database Setup

1. **Generate a model from SQL** – Place your schema in an `.sql` file and import it using `importModel`. The macro runs the `ormin_importer` tool and includes the generated Nim code for you【F:ormin.nim†L8-L31】
2. **Create a database connection** – Ormin expects a global connection named `db` when issuing queries. Open the connection using the appropriate backend (SQLite example below)【F:tests/tsqlite.nim†L6-L11】

```nim
import ../ormin
importModel(DbBackend.sqlite, "model_sqlite")
let db {.global.} = open(":memory:", "", "", "")
```

## Query DSL

`query:` blocks are turned into prepared statements at compile time. Placeholders use `?` for Nim values, `%` for JSON values and `!!` to splice raw SQL fragments【F:ormin/queries.nim†L300-L319】. Typical clauses such as `where`, `join`, `orderby`, `groupby`, `limit` and `offset` are supported, and `returning` captures generated values (e.g. inserted IDs)【F:ormin/queries.nim†L736-L767】.

Example showing inserts, selects and joins:

```nim
# Insert with parameters and JSON
query:
  insert tb_timestamp(dt1 = ?dt1, dt2 = %payload["dt2"])

# Select and join tables
let res = query:
  select post(author)
  join person(name) on author == id
  where id == ?postId
```

`tryQuery` executes a query but ignores database errors; `createProc` and `createIter` generate reusable procedures or iterators for a query【F:ormin/queries.nim†L951-L986】.

## Return Types

- Selecting multiple columns returns a sequence of tuples of the inferred Nim types【F:tests/tsqlite.nim†L49-L52】.
- `produce json` emits `JsonNode` objects instead of Nim tuples【F:tests/tsqlite.nim†L85-L89】; `produce nim` forces standard Nim results【F:tests/tfeature.nim†L450-L465】.
- `returning` or `limit 1` make the query return a single value or tuple instead of a sequence【F:ormin/queries.nim†L736-L759】.
- Generated procedures/iterators return the same types as the underlying query (see `createProc`/`createIter` tests)【F:tests/tsqlite.nim†L70-L83】.

## JSON and Raw SQL

JSON values can be spliced directly using `%jsonNode["field"]`, while `!!"RAW"` injects a literal SQL fragment. The tests include examples of both JSON parameters and raw SQL expressions【F:tests/tfeature.nim†L526-L577】.

## Transactions and Batching

A search of the codebase shows no built-in transaction or batch APIs, so these features must be handled via the underlying `db_connector` modules (e.g. issuing `BEGIN`/`COMMIT` manually)【b75dc1†L1-L2】【a593c9†L1-L2】.

## Additional Facilities

- **Protocol DSL** – The `protocol` macro lets you describe paired server/client handlers that communicate via JSON messages【F:ormin/queries.nim†L1110-L1155】. This is used by the examples to generate matching APIs.
- **JSON Dispatcher** – `createDispatcher` constructs a dispatcher for textual commands mapped to Nim procedures【F:ormin/dispatcher.nim†L31-L56】.
- **WebSocket Server** – `serverws` provides a small WebSocket server that can broadcast messages to selected receivers via the `serve` proc【F:ormin/serverws.nim†L8-L11】【F:ormin/serverws.nim†L131-L139】.

## Tooling

The repository ships with `tools/ormin_importer`, invoked automatically by `importModel`, to parse SQL schema files into Nim type information【F:ormin.nim†L8-L16】.

## Examples

The `examples/` directory contains small applications (chat, forum, tweeter) demonstrating schema import, query blocks and the protocol/WebSocket features.

## Testing

Run the full test suite via Nimble:

```bash
nimble test
```

## License

Ormin is released under the MIT license.
