 import strutils, db_postgres

## Create user and database 
## 1. createuser ormin
## 2. createdb chatpg -O ormin


# host, user, password, dbname
let db = open("localhost", "ormin", "", "chatpg")
 
let model = readFile("chat_model.sql")
for m in model.split(';'):
  if m.strip != "":
    db.exec(sql(m), [])
db.close()

