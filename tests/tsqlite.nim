import unittest, db_connector/sqlite3, json, times, sequtils
from db_connector/db_sqlite import exec, getValue
import ormin
import ormin/db_utils

importModel(DbBackend.sqlite, "model_sqlite")

let
  db {.global.} = open(":memory:", "", "", "")
  testDir = currentSourcePath.parentDir()
  sqlFile = Path(testDir / "model_sqlite.sql")


suite "Test special database types and functions of sqlite":
  discard

jsonTimeFormat = "yyyy-MM-dd HH:mm:ss\'.\'fff"
let
  dtStr1 = "2018-02-20 02:02:02"
  dt1 = parse(dtStr1, "yyyy-MM-dd HH:mm:ss", utc())
  dtStr2 = "2019-03-30 03:03:03.123"
  dt2 = parse(dtStr2, "yyyy-MM-dd HH:mm:ss\'.\'fff", utc())
  dtjson = %*{"dt1": dt1.format(jsonTimeFormat),
              "dt2": dt2.format(jsonTimeFormat)}
let insertSql =  sql"insert into tb_timestamp(dt1, dt2) values (?, ?)"

proc blobFromBytes(bytes: openArray[int]): seq[byte] =
  result = newSeq[byte](bytes.len)
  for i, b in bytes:
    doAssert b >= 0 and b <= 255
    result[i] = byte(b)

let blobFixtures = [
  blobFromBytes(@[0, 1, 2, 3, 4, 5, 6, 7]),
  blobFromBytes(@[255, 128, 64, 32, 16, 8, 4, 2, 1]),
  blobFromBytes(@[ord('O'), ord('R'), ord('M'), ord('I'), ord('N'), 0, 1, 2])
]

suite "timestamp_insert":
  setup:
    db.dropTable(sqlFile, "tb_timestamp")
    db.createTable(sqlFile, "tb_timestamp")

  test "insert":
    query:
      insert tb_timestamp(dt1 = ?dt1, dt2 = ?dt2)
    check db.getValue(sql"select count(*) from tb_timestamp") == "1"

  test "json":
    query:
      insert tb_timestamp(dt1 = %dtjson["dt1"], dt2 = %dtjson["dt2"])
    check db.getValue(sql"select count(*) from tb_timestamp") == "1"

suite "timestamp":
  db.dropTable(sqlFile, "tb_timestamp")
  db.createTable(sqlFile, "tb_timestamp")

  db.exec(insertSql, dtStr1, dtStr2)
  doAssert db.getValue(sql"select count(*) from tb_timestamp") == "1"

  test "query":
    let res = query:
      select tb_timestamp(dt1, dt2)
    check res == [(dt1, dt2)]

  test "where":
    let res = query:
      select tb_timestamp(dt1, dt2)
      where dt1 == ?dt1
    check res == [(dt1, dt2)]

  test "in":
    let
      duration = initDuration(hours = 1)
      dtStart = dt1 - duration
      dtEnd = dt1 + duration
      res2 = query:
        select tb_timestamp(dt1, dt2)
        where dt1 in ?dtStart .. ?dtEnd
    check res2 == [(dt1, dt2)]

  test "iter":
    createIter iter:
      select tb_timestamp(dt1, dt2)
      where dt1 == ?dt1
    var res: seq[tuple[dt1: DateTime, dt2: DateTime]]
    for it in db.iter(dt1):
      res.add(it)
    check res == [(dt1, dt2)]

  test "proc":
    createProc aproc:
      select tb_timestamp(dt1, dt2)
      where dt1 == ?dt1
    check db.aproc(dt1) == [(dt1, dt2)]

  test "json":
    let res = query:
      select tb_timestamp(dt1, dt2)
      produce json
    check res == %*[dtjson]
  
  test "json_where":
    let res = query:
      select tb_timestamp(dt1, dt2)
      where dt1 == %dtjson["dt1"]
      produce json
    check res == %*[dtjson]

suite "blob_insert":
  setup:
    db.dropTable(sqlFile, "tb_blob")
    db.createTable(sqlFile, "tb_blob")

  test "insert parameters":
    for blob in blobFixtures:
      query:
        insert tb_blob(typblob = ?blob)
    check db.getValue(sql"select count(*) from tb_blob") == $blobFixtures.len

suite "blob":
  db.dropTable(sqlFile, "tb_blob")
  db.createTable(sqlFile, "tb_blob")

  for blob in blobFixtures:
    query:
      insert tb_blob(typblob = ?blob)
  doAssert db.getValue(sql"select count(*) from tb_blob") == $blobFixtures.len

  test "query":
    let res = query:
      select tb_blob(id, typblob)
    check res.mapIt(it.typblob) == blobFixtures

  test "where":
    let target = blobFixtures[1]
    let res = query:
      select tb_blob(id, typblob)
      where typblob == ?target
    check res.len == 1
    check res[0].typblob == target
