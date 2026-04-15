import std/[os, paths]
import db_connector/db_common, strutils, strformat
import db_connector/db_postgres as db_postgres
import db_connector/db_sqlite as db_sqlite

import parsesql_tmp # import std/parsesql

export paths

type
  DbConn = db_postgres.DbConn | db_sqlite.DbConn
  DbId* = distinct string
  DbSql* = distinct string

proc `$`*(dbId: DbId): string {.borrow.}
proc `$`*(dbSql: DbSql): string {.borrow.}

template staticLoad*(filename: string): DbSql =
  bind isAbsolute, parentDir, `/`, instantiationInfo
  block:
    const schemaPath {.gensym.} = static:
      if isAbsolute(filename):
        filename
      else:
        parentDir(instantiationInfo(-1, true)[0]) / filename
    const schemaSql {.gensym.} = staticRead(schemaPath)
    static:
      try:
        discard parseSql(schemaSql)
      except SqlParseError as e:
        doAssert false, "Invalid SQL in " & schemaPath & ": " & e.msg
    DbSql(schemaSql)

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

proc canUseBareIdentifier(part: string): bool =
  if part.len == 0:
    return false
  if part[0] notin {'a'..'z', 'A'..'Z', '_'}:
    return false
  for c in part[1..^1]:
    if c notin {'a'..'z', 'A'..'Z', '0'..'9', '_'}:
      return false
  true

proc quoteDbIdentifier*(name: string): DbId =
  ## Quote a database identifier for direct interpolation into SQL text.
  ## Dot-separated names are treated as qualified identifiers.
  var partsSql: seq[string] = @[]
  for (part, wasQuoted) in parseQualifiedIdentifier(name):
    let normalizedPart = if wasQuoted: part else: part.toLowerAscii()
    if wasQuoted or not canUseBareIdentifier(normalizedPart):
      partsSql.add("\"" & normalizedPart.replace("\"", "\"\"") & "\"")
    else:
      partsSql.add(normalizedPart)
  DbId(partsSql.join("."))

proc dropTableName(db: DbConn; tableName, lookupName: string) =
  # SQL parameters bind values, not identifiers, so DROP TABLE needs the
  # identifier rendered into the statement text with correct quoting.
  when defined(sqlite) and defined(orminLegacySqliteDropNames):
    db.exec(sql("drop table if exists " & lookupName))
  else:
    let tableIdentSql = quoteDbIdentifier(tableName)
    when defined(postgre):
      db.exec(sql("drop table if exists " & $tableIdentSql & " cascade"))
    else:
      db.exec(sql("drop table if exists " & $tableIdentSql))

iterator tableDefs(sql: DbSql): tuple[name, tableName, model: string] =
  # Parse the entire SQL file and iterate statements via the SQL parser
  var ast: SqlNode
  try:
    ast = parseSql($sql)
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
  for name, _, model in tableDefs(DbSql(sql)):
    yield (name, model)

iterator tablePairs*(sql: static[DbSql]): tuple[name, model: string] =
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

proc createTable*(db: DbConn; schemaSql: static[DbSql]) =
  for _, m in tablePairs(schemaSql):
    db.exec(sql(m))

proc createTable*(db: DbConn; schemaSql: static[DbSql], name: string) =
  for n, m in tablePairs(schemaSql):
    if n == name:
      db.exec(sql(m))
      return
  raiseAssert &"table: {name} not found in static schema"

proc dropTable*(db: DbConn; sqlFile: Path) =
  for lookupName, tableName, _ in tableDefs(DbSql(readFile($sqlFile))):
    db.dropTableName(tableName, lookupName)

proc dropTable*(db: DbConn; sqlFile: Path, name: string) =
  for lookupName, tableName, _ in tableDefs(DbSql(readFile($sqlFile))):
    if lookupName == name:
      db.dropTableName(tableName, lookupName)
      return
  raiseAssert &"table: {name} not found in: {sqlFile}"

proc dropTable*(db: DbConn; schemaSql: static[DbSql]) =
  for lookupName, tableName, _ in tableDefs(schemaSql):
    db.dropTableName(tableName, lookupName)

proc dropTable*(db: DbConn; schemaSql: static[DbSql], name: string) =
  for lookupName, tableName, _ in tableDefs(schemaSql):
    if lookupName == name:
      db.dropTableName(tableName, lookupName)
      return
  raiseAssert &"table: {name} not found in static schema"
