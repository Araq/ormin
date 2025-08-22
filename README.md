# ormin

Prepared SQL statement generator for Nim. A lightweight ORM.

Features:

- Compile time query checking: Types as well as table
  and column names are checked, no surprises at runtime!
- Automatic join generation: Ormin knows the table
  relations and can compute the "natural" join for you!
- Nim based DSL for queries: No syntax errors at runtime,
  no SQL injections possible.
- Generated prepared statements: As fast as low level
  hand written API calls!
- First class JSON support: No explicit conversions
  from rows to JSON objects required.

TODO:

- Add support for UNION, INTERSECT and EXCEPT.
- Transactions.
- Better support for complex nested queries.
- Write mysql backend.


## Schema and Database Setup

1. **Generate a model from SQL** – Place your schema in an `.sql` file and import it using `importModel`. The macro runs the `ormin_importer` tool and includes the generated Nim code for you
2. **Create a database connection** – Ormin expects a global connection named `db` when issuing queries. The library ships drivers for SQLite and PostgreSQL; pick the matching backend in `importModel` and open a connection with Nim's database modules.

### SQLite

```nim
import ../ormin
importModel(DbBackend.sqlite, "model_sqlite")
let db {.global.} = open(":memory:", "", "", "")
```

### PostgreSQL

```nim
import postgres
import ../ormin
importModel(DbBackend.postgre, "model_postgre")
let db {.global.} = open("localhost", "user", "password", "dbname")
```

## Query DSL

`query:` blocks are turned into prepared statements at compile time. Placeholders use `?` for Nim values and `%` for JSON values; Ormin chooses JSON instead of an ad-hoc variant type so your data can flow straight from/into `JsonNode` trees. `!!` splices vendor-specific SQL fragments. Typical clauses such as `where`, `join`, `orderby`, `groupby`, `limit` and `offset` are supported, and `returning` captures generated values (e.g. inserted IDs). Referring to columns from related tables can trigger **automatic join generation** based on foreign keys, reducing boilerplate joins.

Example snippets:

```nim
# Select recent rows from a Messages table with a Nim parameter
let recentMessages = query:
  select Messages(content, creation, author)
  orderby desc(creation)
  limit ?maxMessages

# Insert using Nim and JSON parameters
let payload = %*{"dt2": %*"2023-10-01T00:00:00Z"}
query:
  insert tb_timestamp(dt1 = ?dt1, dt2 = %payload["dt2"])

# Explicit join with filter
let rows = query:
  select Post(author)
  join Person(name) on author == id
  where id == ?postId

# Automatic join generated from foreign keys
let postsWithAuthors = query:
  select Post(title)
  join Author(name)
  where author.name == ?userName

# Multiple joins with pagination
let page = query:
  select Post(title)
  join Person(name) on author == id
  join Category(title) on category == id
  orderby desc(post.creation)
  limit 5 offset 10

# Vendor-specific function via raw SQL splice
query:
  update Users(lastOnline = !!"DATETIME('now')")
  where id == ?userId
```

Compile with `-d:debugOrminSql` to see the produced SQL at build time, which helps when experimenting with the DSL.

`tryQuery` executes a query but ignores database errors. `createProc` and `createIter` wrap a `query` block into a callable method on `db` for reuse.

### Select and Joins

Selecting columns for the primary table is done using the syntax `select Post(title, author, ...)` where `Post` is the table and `title`, `author`, etc are columns of that table. This will return a tuple containing `(title, author, ...)`. Only one table can be selected and columns must be from that table. Unlike in SQL, columns for joined tables are selected directly in the `join` syntax.

Joins use the syntax `join Person(name, city) on author == id` where `Person` is the table and the columns `name`, and `city` are columns of that table. Often the join condition can be inferred from foreign keys and can be left out: `join Author(name, city)`. The columns listed in the joined tabled will be appended to the results tuple, i.e. `(title, author, name, city)`. Supported joins are: `join`, `innerjoin`, `outerjoin`.

The join syntax differs from SQL but simplifies selecting fields from multiple tables by making them more explicit while still maintaing SQL's full query capabilities.

### Return Types

The core return type for queries is a sequence of tuples where the tuples fields are the types of the columns. Some queries with `returning` or `limit` clauses will return singular values or raise a DbError.

- Selecting multiple columns returns a sequence of tuples of the inferred Nim types.
- Selecting a single column produces a sequence of that Nim type, e.g. `let names: seq[string] = query: select person(name)`.
- `produce json` emits `JsonNode` objects instead of Nim tuples; `produce nim` forces standard Nim results.
- `returning` or `limit 1` make the query return a single value or tuple instead of a sequence.
- Generated procedures/iterators return the same types as the underlying query (see `createProc`/`createIter` tests).

Examples:

```nim
# Sequence of tuples
let threads = query:
  select thread(id, name)

# Sequence of simple Nim types
let ids = query:
  select thread(id)

# JSON result
let threadJson = query:
  select thread(id, name)
  produce json

# Force Nim tuple even if `produce json` was used earlier
let threadNim = query:
  select thread(id, name)
  produce nim
```

Single tuples or values can be returned in some cases:

```nim
# Single value returning
let newId = query:
  insert thread(name = ?"topic")
  returning id

# Single row value returning when limit is a const `1`
let newId = query:
  select thread(name = ?"topic")
  orderby desc(trhead.id)
  limit 1
```

**Note**: use an integer arg to limit to return a sequence instead!

```nim
let n = 1
let newId = query:
  select thread(name = ?"topic")
  orderby desc(trhead.id)
  limit ?n

```

### JSON and Raw SQL

JSON values can be spliced directly using `%` expressions. The `%` prefix tells Ormin to treat the following Nim expression as a `JsonNode` without conversion:

```nim
import json
let payload = %*{"id": %*1, "meta": %*{"tags": %*["nim", "orm"]}}

# Use JSON in WHERE clause
let rows = query:
  select post(id, title)
  where id == %payload["id"]
  produce json

# Insert a row using JSON fields
query:
  insert post(id = %payload["id"], title = %payload["title"], info = %payload["meta"])
```

`!!"RAW"` injects a literal SQL fragment for vendor-specific functions or clauses that Ormin does not know about:

```nim
query:
  update users(lastOnline = !!"DATETIME('now')")
  where id == ?userId
```

The tests include additional samples of JSON parameters and raw SQL expressions.

## Transactions and Batching

TODO!

## Reusable Procedures and Iterators

`createProc` turns a query into a procedure that returns all rows at once:

```nim
createProc postsByAuthor:
  select post(id, title)
  where author == ?userId

let posts = db.postsByAuthor(userId)
```

`createIter` emits an iterator that yields rows lazily:

```nim
createIter postsIter:
  select post(id, title)
  where author == ?userId

for row in db.postsIter(userId):
  echo row.title
```

Both forms accept parameters matching the `?`/`%` placeholders and produce the same return types as an inline `query` block.

## Additional Facilities

- **Protocol DSL** – The `protocol` macro lets you describe paired server/client handlers that communicate via JSON messages. Sections use keywords like `recv`, `broadcast` and `send`, and every server block must be mirrored by a client block. The chat example demonstrates this code generation.
- **JSON Dispatcher** – `createDispatcher` constructs a dispatcher for textual commands mapped to Nim procedures.
- **WebSocket Server** – `serverws` provides a small WebSocket server that can broadcast messages to selected receivers via the `serve` proc.

## Tooling

The repository ships with `tools/ormin_importer`, invoked automatically by `importModel`, to parse SQL schema files into Nim type information.

## Examples

The `examples/` directory contains small applications (chat, forum, tweeter) demonstrating schema import, query blocks and the protocol/WebSocket features.

## Testing

Run the full test suite via Nimble:

```bash
nimble test
```

## License

Ormin is released under the MIT license.
