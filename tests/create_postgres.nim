import strutils, db_postgres
include model_postgres

echo "from sql create postgres table: " & tableNames.join(", ")

let db = open("localhost", "test", "test", "test")
let model = readFile("tests/model_postgres.sql")
for m in model.split(';'):
  if m.strip != "":
    db.exec(sql(m), [])
db.close()