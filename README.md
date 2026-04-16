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

- Better support for complex nested queries.
- Write mysql backend.


## Schema and Database Setup

1. **Generate a model from SQL** – Place your schema in an `.sql` file and import it using `importModel`. By default this runs `ormin_importer` and includes the generated Nim code. Pass `includeStatic = true` to generate the model directly at compile time from the SQL file instead.
2. **Create a database connection** – Ormin expects a global connection named `db` when issuing queries. The library ships drivers for SQLite and PostgreSQL; pick the matching backend in `importModel` and open a connection with Nim's database modules.

### Static Schema

The SQL file can be easily embedded at compile time. Import `ormin/db_utils` and use `const mySql = staticLoad("schema.sql")`, which returns a distinct string type `DbSql` after running sanity-checks the SQL. Pass that const `DbSql` to `createTable` / `dropTable` overloads:

```nim
import ormin/db_utils

const schema = staticLoad("model.sql")

db.createTable(schema)
db.createTable(schema, "quoted table")
db.dropTable(schema)
db.dropTable(schema, "quoted table")
```

If you already use `importModel`, you can opt into the same static path directly there. `includeStatic = true` skips the generated `.nim` file, builds the model metadata from the `.sql` file at compile time, and exposes `sqlSchema` automatically:

```nim
import ../ormin
importModel(DbBackend.sqlite, "model_sqlite", includeStatic = true)

db.createTable(sqlSchema)
db.dropTable(sqlSchema, "tb_timestamp")
```

### SQLite

```nim
import ../ormin
importModel(DbBackend.sqlite, "model_sqlite")
let db {.global.} = open(":memory:", "", "", "")
```

Note: Ormin now properly handles quoted table names in `dropTable`. The compile flag `-d:orminLegacySqliteDropNames` restores that older drop-table behavior by using the normalized lookup name instead of the preserved SQL identifier. The old behavior only worked in SQlite, not Postgres.

### PostgreSQL

```nim
import postgres
import ../ormin
importModel(DbBackend.postgre, "model_postgre")
let db {.global.} = open("localhost", "user", "password", "dbname")
```

## Query DSL

`query:` blocks are turned into prepared statements at compile time. Placeholders use `?` for Nim values and `%` for JSON values; Ormin chooses JSON instead of an ad-hoc variant type so your data can flow straight from/into `JsonNode` trees. `!!` splices vendor-specific SQL fragments. Typical clauses such as `with`, `where`, joins, `orderby`, `groupby`, `limit`, `offset`, `exists`, `distinct`, window expressions, `union`/`intersect`/`except` and `returning` are supported. Referring to columns from related tables can trigger **automatic join generation** based on foreign keys, reducing boilerplate joins.

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
  leftjoin Person(name) on author == id
  where id == ?postId

# Automatic join generated from foreign keys
let postsWithAuthors = query:
  select Post(title)
  join Author(name)
  where author.name == ?userName

# DISTINCT queries and COUNT(DISTINCT ...)
let authorIds = query:
  select `distinct` Post(author)
let authorCount = query:
  select Post(count(distinct author))

# NULL predicates use `nil` or `null`
let unassigned = query:
  select Ticket(id)
  where assignee == nil

# Pattern matching uses backticked infix operators
let matchingPeople = query:
  select Person(id, name)
  where name `like` ?"john%"

# EXISTS / NOT EXISTS subqueries
let peopleWithPosts = query:
  select Person(id)
  where exists(select Post(id) where author == ?personId)

# CTEs use with cteName(select ...)
let recentAuthors = query:
  with recent(select Post(id, author) where id <= 3)
  select recent(author)

# Window functions use over(expr, ...)
let rankedPosts = query:
  select Post(author, id, over(row_number(), partitionby(author), orderby(id)) as rn)

# Set operations can be written inline between select queries
let mergedIds = query:
  select Person(id) where id <= 2
  union
  select Person(id) where id >= 4

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

Joins use the syntax `join Person(name, city) on author == id` where `Person` is the table and the columns `name`, and `city` are columns of that table. Often the join condition can be inferred from foreign keys and can be left out: `join Author(name, city)`. The columns listed in the joined tabled will be appended to the results tuple, i.e. `(title, author, name, city)`. Supported joins are: `join`, `innerjoin`, `leftjoin`, `leftouterjoin`, `rightjoin`, `rightouterjoin`, `fulljoin`, `fullouterjoin`, `crossjoin`, and the legacy `outerjoin`. Runtime support for `rightjoin` / `fulljoin` still depends on the SQL backend.

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

### Custom SQL Functions

Use the `{.importSql.}` pragma to tell Ormin about additional SQL functions that your database provides. Declare a Nim proc or func that mirrors the SQL signature and mark it with the pragma; the declaration does not need an implementation because Ormin only uses it to register the function for the query DSL.

```nim
proc substr(s: string; start, length: int): string {.importSql.}

let name = "foo"
let rows = query:
  select tb_string(substr(typstring, 1, 5))
  where substr(typstring, 1, 5) == ?name
```

Imported functions participate in compile-time checking for arity and return type so they can be composed with regular Ormin expressions.

**Limitation:** argument types are currently not validated, so using mismatched parameter types still compiles—ensure the arguments you pass match what the underlying SQL function expects.

## Transactions and Batching

Use `transaction:` to run multiple queries atomically. The block commits on success and rolls back on any exception. Nesting is supported via savepoints. `tryTransaction:` behaves the same but returns `bool` (false on database errors) without raising.

Examples:

```nim
# Commit on success
transaction:
  query:
    insert person(id = ?(1), name = ?"alice", password = ?"p", email = ?"a@x", salt = ?"s", status = ?"ok")
  query:
    update thread(views = views + 1)
    where id == ?(42)

# Rollback on error
let ok = tryTransaction:
  query:
    insert person(id = ?(2), name = ?"bob", password = ?"p", email = ?"b@x", salt = ?"s", status = ?"ok")
  # Primary key violation => entire block is rolled back, ok = false
  query:
    insert person(id = ?(2), name = ?"duplicate", password = ?"p", email = ?"d@x", salt = ?"s", status = ?"x")

# Nested transactions via savepoints
transaction:
  query:
    insert person(id = ?(3), name = ?"carol", password = ?"p", email = ?"c@x", salt = ?"s", status = ?"ok")
  let innerOk = tryTransaction:
    # This will fail and roll back to the savepoint
    query:
      insert person(id = ?(3), name = ?"duplicate", password = ?"p", email = ?"d@x", salt = ?"s", status = ?"x")
  doAssert innerOk == false
  # Continue outer transaction normally
```

PostgreSQL and SQLite are supported. The macros use `BEGIN/COMMIT/ROLLBACK` for the outermost transaction and `SAVEPOINT/RELEASE/ROLLBACK TO` for nested scopes. 

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

Inline `query` blocks resolve `db` from the current lexical scope, so a proc parameter or local `db` binding can override the global connection when needed. This is useful for making procs which need to do complex handling:

```nim
proc loadUser(db: DbConn; userId: int): User =
  let row = query:
    select user(id, name, email)
    where id == ?userId
    limit 1

  User(id: row.id, name: row.name, email: row.email)
```

## Running Arbitrary SQL

The standard `db_connector` APIs can be imported and used. For example:

```nim
discard db.getValue(sql"select setval('antibot_id_seq', 10, false)")
```

## Additional Facilities

- **Protocol DSL** – The `protocol` macro lets you describe paired server/client handlers that communicate via JSON messages. Sections use keywords like `recv`, `broadcast` and `send`, and every server block must be mirrored by a client block. The chat example demonstrates this code generation.
- **JSON Dispatcher** – `createDispatcher` constructs a dispatcher for textual commands mapped to Nim procedures.
- **WebSocket Server** – `serverws` provides a small WebSocket server that can broadcast messages to selected receivers via the `serve` proc.

## Tooling

The repository ships with `tools/ormin_importer`, used by the default `importModel` path, to parse SQL schema files into Nim type information and write the generated `.nim` model file.

## Examples

The `examples/` directory contains small applications (chat, forum, tweeter) demonstrating schema import, query blocks and the protocol/WebSocket features.

## Testing

Run the full test suite via Nimble:

```bash
nimble test
```

## License

Ormin is released under the MIT license.
