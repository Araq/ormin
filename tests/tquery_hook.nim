import unittest, strutils, strformat, os
import ormin
import ormin/db_utils

when defined(postgre):
  when defined(macosx):
    {.passL: "-Wl,-rpath,/opt/homebrew/lib/postgresql@14".}
  const backend = DbBackend.postgre
  importModel(backend, "model_postgre")
  const sqlFileName = "model_postgre.sql"
  let db {.global.} = open("localhost", "test", "test", "test_ormin")
else:
  const backend = DbBackend.sqlite
  importModel(backend, "model_sqlite")
  const sqlFileName = "model_sqlite.sql"
  let db {.global.} = open("test.db", "", "", "")

let
  testDir = currentSourcePath.parentDir()
  sqlFile = Path(testDir / sqlFileName)

type
  CompositeRow = object
    id: int
    message: string

  SplitMessageRow = object
    parts: seq[string]

proc fromQueryHook*(to: typedesc[seq[string]], x: string): seq[string] =
  x.split(",")

suite &"query(T) mapping on {backend}":
  setup:
    db.dropTable(sqlFile, "tb_composite_pk")
    db.createTable(sqlFile, "tb_composite_pk")
    db.dropTable(sqlFile, "tb_string")
    db.createTable(sqlFile, "tb_string")

    query:
      insert tb_composite_pk(pk1 = 1, pk2 = 1, message = "hello")
    query:
      insert tb_composite_pk(pk1 = 2, pk2 = 2, message = "world")

    query:
      insert tb_string(typstring = "alice,bob")
    query:
      insert tb_string(typstring = "carol,dave")

  test "maps selected rows to objects":
    let rows = query(CompositeRow):
      select tb_composite_pk(pk1 as id, message)
      orderby pk1

    check rows == @[
      CompositeRow(id: 1, message: "hello"),
      CompositeRow(id: 2, message: "world")
    ]

  test "applies fromQueryHook per destination field type":
    let rows = query(SplitMessageRow):
      select tb_string(typstring as parts)
      orderby typstring

    check rows[0].parts == @["alice", "bob"]
    check rows[1].parts == @["carol", "dave"]

  test "single-row query(T) returns a single object":
    let row = query(CompositeRow):
      select tb_composite_pk(pk1 as id, message)
      where pk1 == 1 and pk2 == 1
      limit 1

    check row == CompositeRow(id: 1, message: "hello")
