import db_connector/db_common, strutils, strformat
import std/parsesql
import db_connector/db_postgres as db_postgres
import db_connector/db_sqlite as db_sqlite

type DbConn = db_postgres.DbConn | db_sqlite.DbConn

iterator tablePairs(sqlFile: string): tuple[name, model: string] =
  let f = readFile(sqlFile)
  for m in f.split(';'):
    let stmt = m.strip()
    if stmt.len == 0: continue
    try:
      let ast = parseSql(stmt)
      if ast.len > 0:
        let node = ast[0]
        if node.kind in {nkCreateTable, nkCreateTableIfNotExists}:
          let tableName = node[0].strVal.toLowerAscii()
          yield (tableName, stmt)
    except SqlParseError:
      discard

proc createTable*(db: DbConn; sqlFile: string) =
  for _, m in tablePairs(sqlFile):
    db.exec(sql(m))

proc createTable*(db: DbConn; sqlFile, name: string) =
  for n, m in tablePairs(sqlFile):
    if n == name:
      db.exec(sql(m))
      return
  raiseAssert &"table: {name} not found in: {sqlFile}"

proc dropTable*(db: DbConn; sqlFile: string) =
  for n, _ in tablePairs(sqlFile):
    when defined(postgre):
      db.exec(sql("drop table if exists " & n & " cascade"))
    else:
      db.exec(sql("drop table if exists " & n))

proc dropTable*(db: DbConn; sqlFile, name: string) =
  for n, _ in tablePairs(sqlFile):
    if n == name:
      when defined(postgre):
        db.exec(sql("drop table if exists " & n & " cascade"))
      else:
        db.exec(sql("drop table if exists " & n))
