import strutils, sequtils, db_postgres
include model_postgres

let db = open("localhost", "test", "test", "test")

# find all tables in public schema
let exsitstables = db.getAllRows(sql"select tablename from pg_tables where schemaname='public'").mapIt(it[0])

# drop table in sql file
for name in tableNames:
  if name in exsitstables:
    echo "drop exsits table: " & name
    db.exec(sql("drop table if exists " & name & " cascade"))

# create table in sql file
echo "create postgres table from sql: " & tableNames.join(", ")
let model = readFile("tests/model_postgres.sql")
for m in model.split(';'):
  if m.strip != "":
    db.exec(sql(m), [])

db.close()