import unittest, strutils, db_postgres

suite "create postgres table from sql":
  test "create...":
    let db = open("localhost", "test", "test", "test")
    let model = readFile("tests/model_postgres.sql")
    for m in model.split(';'):
      if m.strip != "":
        db.exec(sql(m), [])