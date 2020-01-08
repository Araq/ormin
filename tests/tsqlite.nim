import unittest
import db_sqlite

suite "connect to sqlite":
  setup:
    var db {.global.} = open("test.db", "", "", "")

  test "create table":
    db.exec(sql"""CREATE TABLE my_table (
                id   INTEGER,
                name VARCHAR(50) NOT NULL
              )""")

  teardown:
    db.close()