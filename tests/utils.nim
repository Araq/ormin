import db_common, strutils
from db_postgres import nil
from db_sqlite import nil

type DbConn = db_postgres.DbConn | db_sqlite.DbConn

proc createTable*(db: DbConn; sqlFile, name: string) =
  let model = readFile(sqlFile)
  for m in model.split(';'):
    let x = m.strip()
    if x != "" and x.find("create table if not exists " & name) > -1:
      db.exec(sql(m))  

proc dropTable*(db: DbConn, name: string) =
  db.exec(sql("drop table if exists " & name))