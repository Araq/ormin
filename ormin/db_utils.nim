import db_connector/db_common, strutils, strformat
import std/parsesql, std/paths
import db_connector/db_postgres as db_postgres
import db_connector/db_sqlite as db_sqlite

export paths

type DbConn = db_postgres.DbConn | db_sqlite.DbConn

iterator tablePairs*(sql: string): tuple[name, model: string] =
  # Parse the entire SQL file and iterate statements via the SQL parser
  let ast = parseSql(sql)
  if ast.len > 0:
    # ast is a statement list; iterate each statement node
    for i in 0 ..< ast.len:
      let node = ast[i]
      if node.kind in {nkCreateTable, nkCreateTableIfNotExists}:
        let tableName = node[0].strVal.toLowerAscii()
        yield (tableName, $node)
  else:
    # Fallback: ast might be a single statement (not a list)
    let node = ast
    if node.kind in {nkCreateTable, nkCreateTableIfNotExists}:
      let tableName = node[0].strVal.toLowerAscii()
      yield (tableName, $node)

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

proc dropTable*(db: DbConn; sqlFile: Path) =
  for n, _ in tablePairs(sqlFile):
    when defined(postgre):
      db.exec(sql("drop table if exists " & n & " cascade"))
    else:
      db.exec(sql("drop table if exists " & n))

proc dropTable*(db: DbConn; sqlFile: Path, name: string) =
  for n, _ in tablePairs(sqlFile):
    if n == name:
      when defined(postgre):
        db.exec(sql("drop table if exists " & n & " cascade"))
      else:
        db.exec(sql("drop table if exists " & n))
