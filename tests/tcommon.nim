import unittest, json, strutils, sequtils, macros, times, os
import ormin
import ./utils

when defined(postgre):
  from db_postgres import exec, getValue

  importModel(DbBackend.postgre, "model_postgre")
  const sqlFileName = "model_postgre.sql"
  let db {.global.} = open("localhost", "test", "test", "test")
else:
  from db_sqlite import exec, getValue

  importModel(DbBackend.sqlite, "model_sqlite")
  const sqlFileName = "model_sqlite.sql"
  let db {.global.} = open("test.db", "", "", "")

let
  testDir = currentSourcePath.parentDir()
  sqlFile = testDir / sqlFileName


let seqs = toSeq(1..3)

suite "serial_insert":
  setup:
    db.dropTable("tb_serial")
    db.createTable(sqlFile, "tb_serial")

  test "insert":
    for i in seqs:
      query:
        insert tb_serial(typinteger = ?i)
    check db.getValue(sql"select count(*) from tb_serial") == $seqs.len

  test "json":
    for i in seqs:
      let v = %*{"typinteger": i}
      query:
        insert tb_serial(typinteger = %v["typinteger"])
    check db.getValue(sql"select count(*) from tb_serial") == $seqs.len


suite "serial":
  db.dropTable("tb_serial")
  db.createTable(sqlFile, "tb_serial")

  let insertSql = sql"insert into tb_serial(typinteger) values (?)"
  for i in seqs:
    db.exec(insertSql, i)  
  doAssert db.getValue(sql"select count(*) from tb_serial") == $seqs.len

  test "query":
    let res = query:
      select tb_serial(typserial, typinteger)
    check res == seqs.mapIt((it, it))

  test "where":
    let
      id = 1
      res = query:
        select tb_serial(typserial, typinteger)
        where typserial == ?id
    check res == seqs.filterIt(it == id).mapIt((it, it))

  test "json":
    let res = query:
      select tb_serial(typserial, typinteger)
      produce json
    check res == %*seqs.mapIt(%*{"typserial": it, "typinteger": it})


suite "boolean_insert":
  setup:
    db.dropTable("tb_boolean")
    db.createTable(sqlFile, "tb_boolean")

  test "insert":
    for i in seqs:
      let b = (i mod 2 == 0)
      query:
        insert tb_boolean(typboolean = ?b)
    check db.getValue(sql"select count(*) from tb_boolean") == $seqs.len

  test "json":
    for i in seqs:
      let b = %*{"typboolean": (i mod 2 == 0)}
      query:
        insert tb_boolean(typboolean = %b["typboolean"])
    check db.getValue(sql"select count(*) from tb_boolean") == $seqs.len


suite "boolean":
  db.dropTable("tb_boolean")
  db.createTable(sqlFile, "tb_boolean")

  proc toBool(x: int): bool =
    if x mod 2 == 0: true else: false

  proc toDbValue(x: int): auto =
    when defined(postgre):
      if x mod 2 == 0: "t" else: "f"
    else:
      if x mod 2 == 0: 1 else: 0

  let insertSql = sql"insert into tb_boolean(typboolean) values (?)"
  for i in seqs:
    db.exec(insertSql, toDbValue(i))
  doAssert db.getValue(sql"select count(*) from tb_boolean") == $seqs.len

  test "query":
    let res = query:
      select tb_boolean(typboolean)
    check res == seqs.mapIt(it mod 2 == 0)

  test "where":
    let
      b = true
      res = query:
        select tb_boolean(typboolean)
        where typboolean == ?b
    check res == seqs.map(toBool).filterIt(it == b)

  test "json":
    let res = query:
      select tb_boolean(typboolean)
      produce json
    check res == %*seqs.mapIt(%*{"typboolean": toBool(it)})


let fs = [3.14, 2.56, 10.45]

suite "float_insert":
  setup:
    db.dropTable("tb_float")
    db.createTable(sqlFile, "tb_float")

  test "insert":
    for f in fs:
      query:
        insert tb_float(typfloat = ?f)
    check db.getValue(sql"select count(*) from tb_float") == $fs.len

  test "json":
    for f in fs:
      let v = %*{"typfloat": f}
      query:
        insert tb_float(typfloat = %v["typfloat"])
    check db.getValue(sql"select count(*) from tb_float") == $fs.len


suite "float":
  db.dropTable("tb_float")
  db.createTable(sqlFile, "tb_float")

  let insertSql = sql"insert into tb_float(typfloat) values (?)"
  for f in fs:
    db.exec(insertSql, f)
  doAssert db.getValue(sql"select count(*) from tb_float") == $fs.len

  test "query":
    let res = query:
      select tb_float(typfloat)
    check res == fs

  test "where":
    let res = query:
      select tb_float(typfloat)
      where typfloat == ?fs[0]
    check res == [fs[0]]

  test "json":
    let res = query:
      select tb_float(typfloat)
      produce json
    check res == %*fs.mapIt(%*{"typfloat": it})