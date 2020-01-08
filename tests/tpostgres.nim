import unittest
import db_postgres

suite "test postgres":
  setup:
    let db = open("localhost", "test", "test", "test")
  
  test "create table":
    db.exec(sql("""CREATE TABLE myTable (
                 id integer,
                 name varchar(50) not null)"""))

  test "insert data":
    db.exec(sql"INSERT INTO myTable (id, name) VALUES (0, ?)",
        "Dominik")

  test "query data":
    let row = db.getRow(sql"select * from myTable")
    echo row
    assert row == ["0", "Dominik"]

  teardown:
    db.close()