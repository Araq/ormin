import std/paths
import db_connector/db_common, strutils, strformat
import db_connector/db_postgres as db_postgres
import db_connector/db_sqlite as db_sqlite

import parsesql_tmp # import std/parsesql

export paths

type
  DbConn = db_postgres.DbConn | db_sqlite.DbConn
  DbId* = distinct string

proc `$`*(dbId: DbId): string {.borrow.}

proc parseQualifiedIdentifier(name: string): seq[(string, bool)] =
  var current = ""
  var quoteChar = '\0'
  var partWasQuoted = false
  var i = 0
  while i < name.len:
    let c = name[i]
    if quoteChar == '\0':
      case c
      of '.':
        result.add((current, partWasQuoted))
        current = ""
        partWasQuoted = false
      of '"', '\'', '`':
        if current.len == 0:
          quoteChar = c
          partWasQuoted = true
        else:
          current.add(c)
      else:
        current.add(c)
    else:
      if c == quoteChar:
        if i + 1 < name.len and name[i + 1] == quoteChar:
          current.add(c)
          inc(i)
        else:
          quoteChar = '\0'
      else:
        current.add(c)
    inc(i)

  result.add((current, partWasQuoted))

proc quoteDbIdentifier*(name: string): DbId =
  ## Quote a database identifier for direct interpolation into SQL text.
  ## Dot-separated names are treated as qualified identifiers.
  var quotedParts: seq[string] = @[]
  for (part, wasQuoted) in parseQualifiedIdentifier(name):
    let normalizedPart =
      if wasQuoted:
        part
      else:
        part.toLowerAscii()
    quotedParts.add("\"" & normalizedPart.replace("\"", "\"\"") & "\"")
  DbId(quotedParts.join("."))

proc dropTableName(db: DbConn; tableName: string) =
  # SQL parameters bind values, not identifiers, so DROP TABLE needs the
  # identifier rendered into the statement text with correct quoting.
  let tableIdentSql = quoteDbIdentifier(tableName)
  when defined(postgre):
    db.exec(sql("drop table if exists " & $tableIdentSql & " cascade"))
  else:
    db.exec(sql("drop table if exists " & $tableIdentSql))

iterator tableDefs(sql: string): tuple[name, tableName, model: string] =
  # Parse the entire SQL file and iterate statements via the SQL parser
  var ast: SqlNode
  try:
    ast = parseSql(sql)
  except SqlParseError as e:
    echo "SQL Parse Error:\n", sql
    raise e

  if ast.len > 0:
    # ast is a statement list; iterate each statement node
    for i in 0 ..< ast.len:
      let node = ast[i]
      if node.kind in {nkCreateTable, nkCreateTableIfNotExists}:
        yield (node[0].strVal.toLowerAscii(), $node[0], $node)
  else:
    # Fallback: ast might be a single statement (not a list)
    let node = ast
    if node.kind in {nkCreateTable, nkCreateTableIfNotExists}:
      yield (node[0].strVal.toLowerAscii(), $node[0], $node)

iterator tablePairs*(sql: string): tuple[name, model: string] =
  for name, _, model in tableDefs(sql):
    yield (name, model)

iterator tablePairs*(sql: Path): tuple[name, model: string] =
  let f = readFile($sql)
  for n, m in tablePairs(f):
    yield (n, m)

proc createTable*(db: DbConn; sqlFile: Path) =
  for _, m in tablePairs(sqlFile):
    db.exec(sql(m))

proc createTable*(db: DbConn; sqlFile: Path, name: string) =
  for n, m in tablePairs(sqlFile):
    if n == name:
      db.exec(sql(m))
      return
  raiseAssert &"table: {name} not found in: {sqlFile}"

proc createTableStatic*(db: DbConn; schemaSql: static[string]) =
  for _, m in tablePairs(schemaSql):
    db.exec(sql(m))

proc createTableStatic*(db: DbConn; schemaSql: static[string], name: string) =
  for n, m in tablePairs(schemaSql):
    if n == name:
      db.exec(sql(m))
      return
  raiseAssert &"table: {name} not found in static schema"

proc dropTable*(db: DbConn; sqlFile: Path) =
  for _, tableName, _ in tableDefs(readFile($sqlFile)):
    db.dropTableName(tableName)

proc dropTable*(db: DbConn; sqlFile: Path, name: string) =
  for n, tableName, _ in tableDefs(readFile($sqlFile)):
    if n == name:
      db.dropTableName(tableName)
      return
  raiseAssert &"table: {name} not found in: {sqlFile}"

proc dropTableStatic*(db: DbConn; schemaSql: static[string]) =
  for _, tableName, _ in tableDefs(schemaSql):
    db.dropTableName(tableName)

proc dropTableStatic*(db: DbConn; schemaSql: static[string], name: string) =
  for n, tableName, _ in tableDefs(schemaSql):
    if n == name:
      db.dropTableName(tableName)
      return
  raiseAssert &"table: {name} not found in static schema"
