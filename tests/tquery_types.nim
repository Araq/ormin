import unittest, strformat, os, times, std/monotimes
import std/options
import ormin
import ormin/db_utils
import ormin/query_hooks

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

  RefCompositeRow = ref object
    id: int
    message: string

  BenchmarkCompositeRow = object
    pk1: int
    message: string

  NullableNoteOptionRow = object
    id: int
    note: Option[string]

  MessageSize = distinct int

  HookedMessageRow = object
    id: int
    message: MessageSize

  NullFallbackNote = distinct string

  HookedNullableNoteRow = object
    id: int
    note: NullFallbackNote

const
  benchmarkRowCount = 256
  benchmarkWarmupIterations = 75
  benchmarkIterations = 250
  benchmarkRounds = 5
  maxTypedQuerySlowdown = 1.20

proc fromQueryHook*(val: var MessageSize, value: string) =
  val = MessageSize(value.len)

proc fromQueryHook*(val: var NullFallbackNote, value: DbValue[string]) =
  if value.isNull:
    val = NullFallbackNote("<missing>")
  else:
    val = NullFallbackNote("note:" & value.value)

proc loadBenchmarkRows() =
  db.dropTable(sqlFile, "tb_composite_pk")
  db.createTable(sqlFile, "tb_composite_pk")
  for i in 1 .. benchmarkRowCount:
    let message = &"message-{i}"
    query:
      insert tb_composite_pk(pk1 = ?i, pk2 = ?i, message = ?message)

proc benchmarkCurrentQuery(iterations: int): float =
  var checksum = 0
  let started = getMonoTime()
  for _ in 0 ..< iterations:
    let rows = query:
      select tb_composite_pk(pk1, message)
      orderby pk1
    checksum += rows.len + rows[^1][0] + rows[^1][1].len
  doAssert checksum > 0
  result = (getMonoTime() - started).inNanoseconds.float / 1_000_000_000.0

proc benchmarkTypedQuery(iterations: int): float =
  var checksum = 0
  let started = getMonoTime()
  for _ in 0 ..< iterations:
    let rows = query(BenchmarkCompositeRow):
      select tb_composite_pk(pk1, message)
      orderby pk1
    checksum += rows.len + rows[^1].pk1 + rows[^1].message.len
  doAssert checksum > 0
  result = (getMonoTime() - started).inNanoseconds.float / 1_000_000_000.0

suite &"query(T) mapping on {backend}":
  setup:
    db.dropTable(sqlFile, "tb_composite_pk")
    db.createTable(sqlFile, "tb_composite_pk")
    db.dropTable(sqlFile, "tb_nullable")
    db.createTable(sqlFile, "tb_nullable")

    query:
      insert tb_composite_pk(pk1 = 1, pk2 = 1, message = "hello")
    query:
      insert tb_composite_pk(pk1 = 2, pk2 = 2, message = "world")

    query:
      insert tb_nullable(id = 1, note = nil)
    query:
      insert tb_nullable(id = 2, note = "hello")

  test "maps selected rows to objects":
    let rows = query(CompositeRow):
      select tb_composite_pk(pk1 as id, message)
      orderby pk1

    check rows == @[
      CompositeRow(id: 1, message: "hello"),
      CompositeRow(id: 2, message: "world")
    ]

  test "maps selected rows to ref objects":
    let rows = query(RefCompositeRow):
      select tb_composite_pk(pk1 as id, message)
      orderby pk1

    check rows.len == 2
    check rows[0] != nil
    check rows[0].id == 1
    check rows[0].message == "hello"
    check rows[1] != nil
    check rows[1].id == 2
    check rows[1].message == "world"

  test "single-row query(T) returns a single object":
    let row = query(CompositeRow):
      select tb_composite_pk(pk1 as id, message)
      where pk1 == 1 and pk2 == 1
      limit 1

    check row == CompositeRow(id: 1, message: "hello")

  test "maps nullable column to Option":
    let rows = query(NullableNoteOptionRow):
      select tb_nullable(id, note)
      orderby id

    check rows[0].id == 1
    check rows[0].note.isNone
    check rows[1].id == 2
    check rows[1].note.isSome
    check rows[1].note.get == "hello"

  test "maps object fields through user fromQueryHook overloads":
    let rows = query(HookedMessageRow):
      select tb_composite_pk(pk1 as id, message)
      orderby pk1

    check rows.len == 2
    check rows[0].id == 1
    check int(rows[0].message) == "hello".len
    check rows[1].id == 2
    check int(rows[1].message) == "world".len

  test "maps scalar query(T) through user fromQueryHook overloads":
    let values = query(MessageSize):
      select tb_composite_pk(message)
      orderby pk1

    check values.len == 2
    check int(values[0]) == "hello".len
    check int(values[1]) == "world".len

  test "allows user fromQueryHook overloads to handle NULL values":
    let rows = query(HookedNullableNoteRow):
      select tb_nullable(id, note)
      orderby id

    check rows.len == 2
    check rows[0].id == 1
    check string(rows[0].note) == "<missing>"
    check rows[1].id == 2
    check string(rows[1].note) == "note:hello"

  test "sqlite benchmark for query and query(T)":
      loadBenchmarkRows()

      let untypedRows = query:
        select tb_composite_pk(pk1, message)
        orderby pk1
      let typedRows = query(BenchmarkCompositeRow):
        select tb_composite_pk(pk1, message)
        orderby pk1
      check untypedRows.len == typedRows.len
      check typedRows[0] == BenchmarkCompositeRow(pk1: untypedRows[0][0], message: untypedRows[0][1])
      check typedRows[^1] == BenchmarkCompositeRow(pk1: untypedRows[^1][0], message: untypedRows[^1][1])

      discard benchmarkCurrentQuery(benchmarkWarmupIterations)
      discard benchmarkTypedQuery(benchmarkWarmupIterations)

      var currentBest = high(float)
      var typedBest = high(float)
      for _ in 0 ..< benchmarkRounds:
        currentBest = min(currentBest, benchmarkCurrentQuery(benchmarkIterations))
        typedBest = min(typedBest, benchmarkTypedQuery(benchmarkIterations))

      let ratio = typedBest / currentBest
      echo &"sqlite benchmark query={currentBest:.6f}s query(T)={typedBest:.6f}s ratio={ratio:.3f}x; 20% budget={(ratio <= maxTypedQuerySlowdown)}"
      check currentBest > 0.0
      check typedBest > 0.0
      when defined(release):
        check ratio <= maxTypedQuerySlowdown
