import db_connector/db_common, strutils, strformat, pegs
import db_connector/db_postgres as db_postgres
import db_connector/db_sqlite as db_sqlite

type DbConn = db_postgres.DbConn | db_sqlite.DbConn

iterator tablePairs(sqlFile: string): tuple[name, model: string] =
  let f = readFile(sqlFile)
  let pat = peg"start <- \s* 'create' \s+ 'table' \s+ ('if' \s+ 'not' \s+ 'exists' \s+)? { [A-Za-z_][A-Za-z0-9_]* }"
  for m in f.split(';'):
    let s = m.toLowerAscii()
    if m.strip() != "":
      var caps = newSeq[string](1)
      if s.match(pat, caps):
        yield (caps[0], m)

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
