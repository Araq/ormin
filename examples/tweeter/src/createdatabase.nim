import db_sqlite, os, strutils

let db {.global.} = open("tweeter.db", "", "", "")

let sqlFile = readFile(currentSourcePath.parentDir() / "tweeter_model.sql")
for t in sqlFile.split(';'):
  if t.strip() != "":
    db.exec(sql(t))

echo("Database created successfully!")
db.close()