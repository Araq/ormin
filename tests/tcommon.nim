import unittest, json, strutils, strformat, sequtils, macros, times, os, math, unicode
import ormin
import ormin/db_utils

when defined(postgre):
  when defined(macosx):
    {.passL: "-Wl,-rpath,/opt/homebrew/lib/postgresql@14".}
  import db_connector/db_postgres as db_postgres

  const backend = DbBackend.postgre
  importModel(backend, "model_postgre")
  const sqlFileName = "model_postgre.sql"
  let db {.global.} = db_postgres.open("localhost", "test", "test", "test_ormin")
else:
  from db_connector/db_sqlite import exec, getValue

  const backend = DbBackend.sqlite
  importModel(backend, "model_sqlite")
  const sqlFileName = "model_sqlite.sql"
  # let db {.global.} = open(":memory:", "", "", "")
  let db {.global.} = open("test.db", "", "", "")

let
  testDir = currentSourcePath.parentDir()
  sqlFile = Path(testDir / sqlFileName)

proc substr(s: string; start, length: int): string {.importSql.}

let
  min = -3
  max = 3
  serial_int_seqs = [(1, min), (2, 1), (3, 2), (4, max)]

suite &"Test common database types and functions of {backend}":
  discard

suite "serial_insert":
  setup:
    db.dropTable(sqlFile, "tb_serial")
    db.createTable(sqlFile, "tb_serial")

  test "insert":
    for i in serial_int_seqs:
      query:
        insert tb_serial(typinteger = ?i[1])
    check db.getValue(sql"select count(*) from tb_serial") == $serial_int_seqs.len

  test "json":
    for i in serial_int_seqs:
      let v = %*{"typinteger": i[1]}
      query:
        insert tb_serial(typinteger = %v["typinteger"])
    check db.getValue(sql"select count(*) from tb_serial") == $serial_int_seqs.len


suite "serial":
  db.dropTable(sqlFile, "tb_serial")
  db.createTable(sqlFile, "tb_serial")

  let insertSql = sql"insert into tb_serial(typinteger) values (?)"
  for (_, v) in serial_int_seqs:
    db.exec(insertSql, v) 
  doAssert db.getValue(sql"select count(*) from tb_serial") == $serial_int_seqs.len

  test "query":
    let res = query:
      select tb_serial(typserial, typinteger)
    check res == serial_int_seqs

  test "where":
    let
      id = 1
      res = query:
        select tb_serial(typserial, typinteger)
        where typserial == ?id
    check res == serial_int_seqs.filterIt(it[0] == id)

  test "json":
    let res = query:
      select tb_serial(typserial, typinteger)
      produce json
    check res == %*serial_int_seqs.mapIt(%*{"typserial": it[0], "typinteger": it[1]})

  test "count":
    let res = query:
      select tb_serial(count(_))
    check res[0] == serial_int_seqs.len

  test "sum":
    let res = query:
      select tb_serial(sum(typinteger))
    check res[0] == serial_int_seqs.mapIt(it[1]).sum()

  test "avg":
    let res = query:
      select tb_serial(avg(typinteger))
    check res[0] == serial_int_seqs.mapIt(it[1]).sum() / serial_int_seqs.len()

  test "min":
    let res = query:
      select tb_serial(min(typinteger))
    check res[0] == min

  test "max":
    let res = query:
      select tb_serial(max(typinteger))
    check res[0] == max

  test "abs":
    let res = query:
      select tb_serial(abs(typinteger))
    check res == serial_int_seqs.mapIt(abs(it[1]))


let seqs = toSeq(1..3)

suite "boolean_insert":
  setup:
    db.dropTable(sqlFile, "tb_boolean")
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
  db.dropTable(sqlFile, "tb_boolean")
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

  when defined(sqlite):
    test "importSql substr wrong type":
      ## TODO: should this be allowed?
      let res = query:
        select tb_boolean(substr(typboolean, 1, 2))
      echo "res: ", res

let fs = [-3.14, 2.56, 10.45]

suite "float_insert":
  setup:
    db.dropTable(sqlFile, "tb_float")
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
  db.dropTable(sqlFile, "tb_float")
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

  test "abs":
    let res = query:
      select tb_float(abs(typfloat))
    check res == fs.mapIt(abs(it))

let ss = ["one", "Two", "three", "第四", "four'th"]

suite "string_insert":
  setup:
    db.dropTable(sqlFile, "tb_string")
    db.createTable(sqlFile, "tb_string")

  test "insert":
    for v in ss:
      query:
        insert tb_string(typstring = ?v)
    check db.getValue(sql"select count(*) from tb_string") == $ss.len

  test "insert":
    for v in ss:
      query:
        insert tb_string(typstring = "can't touch this")
    check db.getValue(sql"select count(*) from tb_string") == $ss.len

  test "json":
    for v in ss:
      let j = %*{"typstring": v}
      query:
        insert tb_string(typstring = %j["typstring"])
    check db.getValue(sql"select count(*) from tb_string") == $ss.len

suite "string":
  db.dropTable(sqlFile, "tb_string")
  db.createTable(sqlFile, "tb_string")

  let insertsql = sql"insert into tb_string(typstring) values (?)"
  for v in ss:
    db.exec(insertsql, v)
  doAssert db.getValue(sql"select count(*) from tb_string") == $ss.len

  test "query":
    let res = query:
      select tb_string(typstring)
    check res == ss

  test "where":
    let res = query:
      select tb_string(typstring)
      where typstring == ?ss[0]
    check res[0] == ss[0]

  test "json":
    let res = query:
      select tb_string(typstring)
      produce json
    check res == %*ss.mapIt(%*{"typstring": it})

  test "concat_op":
    let
      s = "typstring: "
    let res = query:
      select tb_string(?s & typstring)
    check res == ss.mapIt(s & it)
    
  test "length":
    let res = query:
      select tb_string(length(typstring))
    check res == ss.mapIt(it.runeLen)

  test "lower":
    let res = query:
      select tb_string(lower(typstring))
    check res == ss.mapIt(it.toLowerAscii)

  test "upper":
    let res = query:
      select tb_string(upper(typstring))
    check res == ss.mapIt(it.toUpperAscii)

  test "replace":
    let res = query:
      select tb_string(replace(typstring, "e", "o"))
    check res == ss.mapIt(it.replace("e", "o"))

  test "importSql substr length 0":
    let res = query:
      select tb_string(substr(typstring, 1, 0))
    let expected = ss.mapIt($(toRunes(it)[0..<0]))
    check res == expected

  test "importSql substr length no arg types checked":
    # TODO: fixme!
    # the `functions` array only contains the types of the return
    # but sqlite doesn't really care...
    let res = query:
      select tb_string(substr(typstring, "1", 2))
    let expected = ss.mapIt($(toRunes(it)[0..<2]))
    check res == expected

let js = [
  %*{"name": "tom", "age": 30},
  %*{"name": "bob", "age": 35},
  %*{"name": "jack", "age": 23} 
]

suite "insert_json":
  setup:
    db.dropTable(sqlFile, "tb_json")
    db.createTable(sqlFile, "tb_json")

  test "insert":
    for j in js:
      query:
        insert tb_json(typjson = ?j)
    check db.getValue(sql"select count(*) from tb_json") == $js.len

suite "json":
  db.dropTable(sqlFile, "tb_json")
  db.createTable(sqlFile, "tb_json")
  
  let insertSql = sql"insert into tb_json(typjson) values (?)"
  for j in js:
    db.exec(insertSql, j)
  doAssert db.getValue(sql"select count(*) from tb_json") == $js.len

  test "query":
    let res = query:
      select tb_json(typjson)
    check res == js

  test "json":
    let res = query:
      select tb_json(typjson)
      produce json
    check res == %*js.mapIt(%*{"typjson": it})


let cps = [
  (1, 1, "one-one"),
  (1, 2, "one-two"),
  (2, 1, "two-one")
]

suite "composite_pk_insert":
  setup:
    db.dropTable(sqlFile, "tb_composite_pk")
    db.createTable(sqlFile, "tb_composite_pk")

  test "insert":
    for r in cps:
      query:
        insert tb_composite_pk(pk1 = ?r[0], pk2 = ?r[1], message = ?r[2])
    check db.getValue(sql"select count(*) from tb_composite_pk") == $cps.len

  test "json":
    for r in cps:
      let v = %*{"pk1": r[0], "pk2": r[1], "message": r[2]}
      query:
        insert tb_composite_pk(pk1 = %v["pk1"], pk2 = %v["pk2"], message = %v["message"])
    check db.getValue(sql"select count(*) from tb_composite_pk") == $cps.len


suite "composite_pk":
  db.dropTable(sqlFile, "tb_composite_pk")
  db.createTable(sqlFile, "tb_composite_pk")

  let insertSql = sql"insert into tb_composite_pk(pk1, pk2, message) values (?, ?, ?)"
  for r in cps:
    db.exec(insertSql, r[0], r[1], r[2])
  doAssert db.getValue(sql"select count(*) from tb_composite_pk") == $cps.len

  test "query":
    let res = query:
      select tb_composite_pk(pk1, pk2, message)
    check res == cps

  test "where":
    let res = query:
      select tb_composite_pk(pk1, pk2, message)
      where pk1 == ?cps[0][0]
    check res == cps.filterIt(it[0] == cps[0][0])

  test "json":
    let res = query:
      select tb_composite_pk(pk1, pk2, message)
      produce json
    check res == %*cps.mapIt(%*{"pk1": it[0], "pk2": it[1], "message": it[2]})
