import unittest, strutils
from db_postgres import exec
import ../ormin

importModel(DbBackend.postgre, "model_postgres")

suite "test postgres":
  let db {.global.} = open("localhost", "test", "test", "test")

  test "insert data":
    query:
      insert users(id=1, name="john3", password="xxxx", lastOnline = !!"CURRENT_TIMESTAMP")

  test "query data":
    let users = query:
      select users(id, name)
    assert users == [(id: 1, name: "john3")]

  # test "create table":
  #   db.exec(sql("""CREATE TABLE myTable (
  #                id integer,
  #                name varchar(50) not null)"""))

  # test "insert data":
  #   db.exec(sql"INSERT INTO myTable (id, name) VALUES (0, ?)",
  #       "Dominik")

  # test "query data":
  #   let row = db.getRow(sql"select * from myTable")
  #   assert row == ["0", "Dominik"]      

  for name in tableNames:
    db.exec(sql("drop table " & name & " cascade"))
  db.close()
