import unittest, json, options, os
import ormin
import ormin/db_utils
from db_connector/db_sqlite import getValue

const backend = DbBackend.sqlite
importModel(backend, "model_sqlite")

let
  db {.global.} = open(":memory:", "", "", "")
  testDir = currentSourcePath.parentDir()
  sqlFile = Path(testDir / "model_sqlite.sql")

suite "sqlite float nullable behavior":
  setup:
    db.dropTable(sqlFile, "tb_nullable_float")
    db.createTable(sqlFile, "tb_nullable_float")

  test "sqlite stores nan as null and float reads follow compile mode":
    let nanValue = NaN
    query:
      insert tb_nullable_float(id = 1, typfloat = 1.25)
    query:
      insert tb_nullable_float(id = 2, typfloat = null)
    query:
      insert tb_nullable_float(id = 3, typfloat = ?nanValue)

    check db.getValue(sql"select count(*) from tb_nullable_float where typfloat is null") == "2"

    let res = query:
      select tb_nullable_float(typfloat)
      orderby id
    check res.len == 3
    check res[0] == 1.25
    when defined(orminSqliteNullFloatAsNaN) or defined(ormin.sqliteNullFloatAsNaN):
      check res[1] != res[1]
      check res[2] != res[2]
    else:
      check res[1] == 0.0
      check res[2] == 0.0

  test "option_float placeholder maps none to null":
    let someValue = some(4.5)
    let noneValue = none(float)
    query:
      insert tb_nullable_float(id = 10, typfloat = ?someValue)
    query:
      insert tb_nullable_float(id = 11, typfloat = ?noneValue)

    let res = query:
      select tb_nullable_float(typfloat)
      where id in {10, 11}
      orderby id
    check res.len == 2
    check res[0] == 4.5
    when defined(orminSqliteNullFloatAsNaN) or defined(ormin.sqliteNullFloatAsNaN):
      check res[1] != res[1]
    else:
      check res[1] == 0.0

  test "json preserves null for nullable float":
    query:
      insert tb_nullable_float(id = 20, typfloat = null)
    let res = query:
      select tb_nullable_float(id, typfloat)
      where id == 20
      produce json
    check res[0]["typfloat"].kind == JNull
