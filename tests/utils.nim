import db_connector/db_common, strutils, strformat, re
from db_connector/db_postgres import nil
from db_connector/db_sqlite import nil

type DbConn = db_postgres.DbConn | db_sqlite.DbConn

iterator tablePairs(sqlFile: string): tuple[name, model: string] =
  let f = readFile(sqlFile)
  for m in f.split(';'):
    if m.strip() != "" and
       m =~ re"\n*create\s+table(\s+if\s+not\s+exists)?\s+(\w+)":
      yield (matches[1], m)

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