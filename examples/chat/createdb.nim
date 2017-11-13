import strutils, db_sqlite

var db = open(connection="chat.db", user="araq", password="",
              database="chat")
let model = readFile("chat_model.sql")
for m in model.split(';'):
  if m.strip != "":
    db.exec(sql(m), [])
db.close()
