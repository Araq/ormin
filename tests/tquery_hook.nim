import unittest, strutils, json, times
import std/options

import ormin/query_hooks

type
  DirectRow = object
    id: int
    message: string
    note: Option[string]

  SplitRow = object
    parts: seq[string]

  RefRow = ref object
    id: int
    ok: bool

proc fromQueryHook*(to: typedesc[seq[string]], x: string): seq[string] =
  x.split(",")

proc toQueryHook*(val: var string, x: seq[string]) =
  val = x.join(",")

suite "query hook helpers":
  test "maps DbItem primitives directly":
    var intItem = DbItem(name: "id", value: "42", isNull: false)
    var boolItem = DbItem(name: "ok", value: "true", isNull: false)
    var jsonItem = DbItem(name: "payload", value: """{"hello":"world"}""", isNull: false)

    check fromQueryHook(int, intItem) == 42
    check fromQueryHook(bool, boolItem) == true
    check fromQueryHook(JsonNode, jsonItem) == %*{"hello": "world"}

  test "maps null DbItem to Option":
    var noneItem = DbItem(name: "note", isNull: true)
    var someItem = DbItem(name: "note", value: "hello", isNull: false)

    check fromQueryHook(Option[string], noneItem).isNone
    check fromQueryHook(Option[string], someItem) == some("hello")

  test "maps DbRow to object using fieldPairs":
    var row: DbRow = @[
      DbItem(name: "id", value: "1", isNull: false),
      DbItem(name: "message", value: "hello", isNull: false),
      DbItem(name: "note", value: "memo", isNull: false)
    ]

    let mapped = fromQueryHook(DirectRow, row)
    check mapped.id == 1
    check mapped.message == "hello"
    check mapped.note == some("memo")

  test "applies custom string hooks during DbRow mapping":
    var row: DbRow = @[
      DbItem(name: "parts", value: "alice,bob", isNull: false)
    ]

    let mapped = fromQueryHook(SplitRow, row)
    check mapped.parts == @["alice", "bob"]

  test "maps DbRow to ref object":
    var row: DbRow = @[
      DbItem(name: "id", value: "9", isNull: false),
      DbItem(name: "ok", value: "1", isNull: false)
    ]

    let mapped = fromQueryHook(RefRow, row)
    check not mapped.isNil
    check mapped.id == 9
    check mapped.ok == true

  test "writes Option.none to a null DbItem":
    var item: DbItem

    toQueryHook(item, none(string))
    check item.isNull
    check item.value.len == 0

  test "round-trips object rows through toQueryHook and fromQueryHook":
    let source = DirectRow(id: 7, message: "roundtrip", note: some("works"))
    var row: DbRow

    toQueryHook(row, source)

    check row.len == 3
    check row[0].name == "id"
    check row[1].name == "message"
    check row[2].name == "note"

    let mapped = fromQueryHook(DirectRow, row)
    check mapped == source

  test "round-trips custom hook types through DbRow":
    let source = SplitRow(parts: @["carol", "dave"])
    var row: DbRow

    toQueryHook(row, source)

    check row.len == 1
    check row[0].name == "parts"
    check row[0].value == "carol,dave"

    let mapped = fromQueryHook(SplitRow, row)
    check mapped == source

  test "round-trips DateTime through DbItem":
    let source = dateTime(2024, mJan, 2, 3, 4, 5, zone = utc())
    var item: DbItem

    toQueryHook(item, source)
    let mapped = fromQueryHook(DateTime, item)
    check mapped == source
