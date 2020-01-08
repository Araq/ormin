import unittest, strutils
# from db_postgres import exec
import ormin

importModel(DbBackend.postgre, "model_postgres")

suite "test postgres":
  setup:
    let db {.global.} = open("localhost", "test", "test", "test")

  teardown:
    # for name in tableNames:
    #   echo name
    #   db.exec(sql("drop table " & name & " cascade"))
    db.close()

  test "query data":
    query:
      insert users(name="john3", password="xxxx", lastOnline = !!"CURRENT_TIMESTAMP")

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