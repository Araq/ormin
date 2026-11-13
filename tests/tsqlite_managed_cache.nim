import std/[assertions, json, options, strutils, times]

import db_connector/sqlite3
import db_connector/db_sqlite as sqliteDb except DbConn, open, close, dbError
import ormin
importModel(DbBackend.sqlite, "model_sqlite")

when defined(staticSqlite):
  {.pragma: sqliteApi, cdecl.}
else:
  when defined(windows):
    when defined(nimOldDlls):
      const SqliteLibrary = "sqlite3.dll"
    elif defined(cpu64):
      const SqliteLibrary = "sqlite3_64.dll"
    else:
      const SqliteLibrary = "sqlite3_32.dll"
  elif defined(macosx):
    const SqliteLibrary = "libsqlite3(|.0).dylib"
  else:
    const SqliteLibrary = "libsqlite3.so(|.0)"
  {.pragma: sqliteApi, cdecl, dynlib: SqliteLibrary.}

proc nextStatement(db: PSqlite3; previous: PStmt): PStmt {.
  sqliteApi, importc: "sqlite3_next_stmt".}
proc statementBusy(statement: PStmt): cint {.
  sqliteApi, importc: "sqlite3_stmt_busy".}
proc createFunctionV2(db: PSqlite3; name: cstring; args, encoding: cint;
                      data: pointer; fn, step: Create_function_func_func;
                      finish: Create_function_final_func;
                      destroy: proc(data: pointer) {.cdecl.}): cint {.
  sqliteApi, importc: "sqlite3_create_function_v2".}

proc unusedFunction(context: Pcontext; count: int32; values: PValueArg) {.cdecl.} =
  result_null(context)

proc countClose(data: pointer) {.cdecl.} =
  inc cast[ptr int](data)[]

proc observeClose(db: ManagedDbConn; count: var int) =
  doAssert createFunctionV2(db.borrowRaw, "ormin_observe_close", 0, SQLITE_UTF8,
    addr count, unusedFunction, nil, nil, countClose) == SQLITE_OK

proc liveStatements(db: ManagedDbConn): int =
  var statement = nextStatement(db.borrowRaw, nil)
  while statement != nil:
    inc result
    statement = nextStatement(db.borrowRaw, statement)

proc assertIdle(db: ManagedDbConn) =
  doAssert liveStatements(db) == db.managedPlanCount
  var statement = nextStatement(db.borrowRaw, nil)
  while statement != nil:
    doAssert statementBusy(statement) == 0
    statement = nextStatement(db.borrowRaw, statement)

proc newDatabase(maxPlans = DefaultManagedPlanLimit): ManagedDbConn =
  result = openManaged(":memory:", maxPlans)
  sqliteDb.exec(result.borrowRaw,
    sql"create table tb_serial (typserial integer primary key, typinteger integer unique)")
  sqliteDb.exec(result.borrowRaw,
    sql"insert into tb_serial(typinteger) values (11), (22)")

proc queryInt(db: var ManagedDbConn; sql: string; value: int): int =
  var statement = prepareStmt(db, sql)
  startBindings(statement, 1)
  bindParam(db, statement, 1, value, int)
  startQuery(db, statement)
  doAssert stepQuery(db, statement, true)
  bindResult(db, statement, 0, result, int, "value")
  stopQuery(db, statement)

createProc allValues:
  select tb_serial(typinteger)
  orderby typinteger

createIter values:
  select tb_serial(typinteger)
  orderby typinteger

block reuse_and_cache_limit:
  var db = newDatabase(maxPlans = 1)
  doAssert queryInt(db, "select ?", 1) == 1
  let cached = nextStatement(db.borrowRaw, nil)
  for value in 0..<100:
    doAssert queryInt(db, "select ?", value) == value
    doAssert queryInt(db, "select ? + 1", value) == value + 1
    doAssert nextStatement(db.borrowRaw, nil) == cached
    doAssert db.managedPlanCount == 1
    assertIdle(db)

block disabled_cache_and_invalid_limits:
  doAssertRaises(ValueError):
    discard openManaged(":memory:", maxPlans = -1)
  doAssertRaises(DbError):
    discard openManaged("/ormin-nonexistent-directory/database.sqlite")
  var db = newDatabase(maxPlans = 0)
  for value in 0..<25:
    doAssert queryInt(db, "select ?", value) == value
    doAssert db.managedPlanCount == 0
    assertIdle(db)

block bindings_are_cleared_on_release:
  var db = newDatabase()
  block:
    var statement = prepareStmt(db, "select ?, ?")
    bindParam(db, statement, 1, 1, int)
    bindParam(db, statement, 2, "previous value", string)
    startQuery(db, statement)
    doAssert stepQuery(db, statement, true)
  block:
    var statement = prepareStmt(db, "select ?, ?")
    # Deliberately skip startBindings: release itself must clear old bindings.
    bindParam(db, statement, 1, 2, int)
    startQuery(db, statement)
    doAssert stepQuery(db, statement, true)
    doAssert columnIsNull(db, statement, 1)
    stopQuery(db, statement)
  assertIdle(db)

block generated_queries_and_nested_iterators:
  var db = newDatabase(maxPlans = 1)
  doAssert db.allValues() == @[11, 22]
  var pairs: seq[(int, int)]
  for outer in db.values():
    doAssert db.allValues() == @[11, 22]
    for inner in db.values():
      doAssert liveStatements(db) == 2
      pairs.add (outer, inner)
  doAssert pairs == @[(11, 11), (11, 22), (22, 11), (22, 22)]
  doAssert db.managedPlanCount == 1
  assertIdle(db)
  let raw = db.borrowRaw
  doAssert raw.allValues() == @[11, 22]
  assertIdle(db)

proc firstValue(db: var ManagedDbConn): int =
  for value in db.values():
    return value

block iterator_exits_release_statements:
  for limit in [0, 1]:
    var db = newDatabase(maxPlans = limit)
    for value in db.values():
      doAssert value == 11
      break
    assertIdle(db)
    doAssert firstValue(db) == 11
    assertIdle(db)
    doAssertRaises(ValueError):
      for value in db.values():
        raise newException(ValueError, "iterator consumer failed")
    assertIdle(db)
    doAssert db.allValues() == @[11, 22]

type
  ReadFailure = distinct int
  BindFailure = distinct int

proc fromQueryHook*(dest: var ReadFailure; value: int) =
  raise newException(ValueError, "result conversion failed")

proc toQueryHook*(dest: var int; value: BindFailure) =
  raise newException(ValueError, "parameter conversion failed")

block conversion_and_binding_errors_release_statements:
  for limit in [0, 8]:
    var db = newDatabase(maxPlans = limit)
    doAssertRaises(ValueError):
      discard query(ReadFailure):
        select tb_serial(typinteger)
    assertIdle(db)
    let badValue = BindFailure(11)
    doAssertRaises(ValueError):
      discard query:
        select tb_serial(typinteger)
        where typinteger == ?badValue
    assertIdle(db)
    doAssert db.allValues() == @[11, 22]

block typed_json_nullable_and_single_row_queries:
  var db = newDatabase()
  let value = 33
  let inserted = query:
    insert tb_serial(typinteger = ?value)
    returning typserial
  doAssert inserted == 3
  let untypedRow = query:
    select tb_serial(typinteger)
    where typserial == ?inserted
    limit 1
  doAssert untypedRow == value
  let typedRow = query(int):
    select tb_serial(typinteger)
    where typserial == ?inserted
    limit 1
  doAssert typedRow == value
  doAssertRaises(DbError):
    discard query:
      select tb_serial(typinteger)
      where typserial == 999
      limit 1
  doAssertRaises(DbError):
    discard query(int):
      select tb_serial(typinteger)
      where typserial == 999
      limit 1
  let missing = tryQuery(int):
    select tb_serial(typinteger)
    where typserial == 999
    limit 1
  doAssert missing == 0
  sqliteDb.exec(db.borrowRaw, sql"create table tb_nullable (id integer primary key, note text)")
  let note = some("hello")
  query:
    insert tb_nullable(id = 1, note = ?note)
  let empty = none(string)
  query:
    insert tb_nullable(id = 2, note = ?empty)
  type NoteRow = object
    id: int
    note: Option[string]
  let notes = query(NoteRow):
    select tb_nullable(id, note)
    orderby id
  doAssert notes == @[NoteRow(id: 1, note: note), NoteRow(id: 2, note: empty)]
  let input = %*{"id": 1}
  let rows = query:
    select tb_nullable(id, note)
    where id == %input["id"]
    produce json
  doAssert rows == %*[{"id": 1, "note": "hello"}]
  assertIdle(db)

block sqlite_value_types:
  var db = openManaged(":memory:")
  sqliteDb.exec(db.borrowRaw, sql"create table tb_blob (id integer primary key, typblob blob)")
  let bytes = @[0.byte, 1, 255]
  query:
    insert tb_blob(typblob = ?bytes)
  let blobs = query:
    select tb_blob(typblob)
  doAssert blobs == @[bytes]
  sqliteDb.exec(db.borrowRaw, sql"create table tb_timestamp (dt1 timestamp, dt2 timestamp)")
  let timestamp = parse("2026-11-13 12:00:00", "yyyy-MM-dd HH:mm:ss", utc())
  query:
    insert tb_timestamp(dt1 = ?timestamp, dt2 = ?timestamp)
  let timestamps = query:
    select tb_timestamp(dt1, dt2)
  doAssert timestamps == @[(timestamp, timestamp)]
  assertIdle(db)

block execution_and_prepare_errors_preserve_connection:
  var db = newDatabase()
  for attempt in 0..<3:
    var message = ""
    try:
      query:
        insert tb_serial(typinteger = 11)
    except DbError as exc:
      message = exc.msg
    doAssert "UNIQUE constraint failed" in message
    doAssertRaises(DbError):
      discard prepareStmt(db, "select from")
    doAssertRaises(DbError):
      discard prepareStmt(db, "-- no statement")
    doAssert db.allValues() == @[11, 22]
    assertIdle(db)

block transactions_commit_rollback_and_nested_recovery:
  var db = newDatabase()
  transaction:
    query:
      insert tb_serial(typinteger = 33)
  doAssert db.allValues() == @[11, 22, 33]
  doAssertRaises(DbError):
    transaction:
      query:
        insert tb_serial(typinteger = 44)
      query:
        insert tb_serial(typinteger = 11)
  doAssert db.allValues() == @[11, 22, 33]
  transaction:
    query:
      insert tb_serial(typinteger = 55)
    var recovered = false
    transaction:
      query:
        insert tb_serial(typinteger = 66)
      query:
        insert tb_serial(typinteger = 11)
    else:
      recovered = true
    doAssert recovered
    query:
      insert tb_serial(typinteger = 77)
  doAssert db.allValues() == @[11, 22, 33, 55, 77]
  assertIdle(db)

block reset_preserves_data_and_rejects_active_statements:
  for limit in [0, 4]:
    var db = newDatabase(maxPlans = limit)
    let raw = db.borrowRaw
    block:
      var statement = prepareStmt(db, "select 1")
      doAssertRaises(DbError):
        db.reset()
      startQuery(db, statement)
      doAssert stepQuery(db, statement, true)
      stopQuery(db, statement)
    transaction:
      query:
        insert tb_serial(typinteger = 33)
      db.reset()
      doAssert db.managedPlanCount == 0
      doAssert liveStatements(db) == 0
      doAssert db.borrowRaw == raw
    doAssert db.allValues() == @[11, 22, 33]
    assertIdle(db)

block schema_changes_recompile_cached_statements:
  var db = newDatabase()
  doAssert db.allValues() == @[11, 22]
  sqliteDb.exec(db.borrowRaw, sql"drop table tb_serial")
  doAssertRaises(DbError):
    discard db.allValues()
  sqliteDb.exec(db.borrowRaw, sql"create table tb_serial (typinteger integer)")
  sqliteDb.exec(db.borrowRaw, sql"insert into tb_serial values (99)")
  doAssert db.allValues() == @[99]
  doAssert db.managedPlanCount == 1
  assertIdle(db)

block move_and_scope_destruction_close_once:
  var closed = 0
  block:
    var original = newDatabase()
    observeClose(original, closed)
    discard original.allValues()
    var moved = move(original)
    doAssert original.borrowRaw == nil
    original.close()
    doAssert closed == 0
    doAssert moved.allValues() == @[11, 22]
  doAssert closed == 1

block move_assignment_closes_replaced_connection:
  var originalClosed, replacedClosed = 0
  block:
    var original = newDatabase()
    var replaced = newDatabase()
    observeClose(original, originalClosed)
    observeClose(replaced, replacedClosed)
    discard original.allValues()
    discard replaced.allValues()
    replaced = move(original)
    doAssert replacedClosed == 1
    doAssert originalClosed == 0
    doAssert original.borrowRaw == nil
    doAssert replaced.allValues() == @[11, 22]
    replaced.close()
    replaced.close()
  doAssert originalClosed == 1
  doAssert replacedClosed == 1

block explicit_release_and_wrong_connection:
  var db = newDatabase()
  var other = newDatabase()
  var statement = prepareStmt(db, "select 1")
  doAssertRaises(DbError):
    startQuery(other, statement)
  finalizeStmt(statement)
  finalizeStmt(statement)
  doAssertRaises(DbError):
    startQuery(db, statement)
  assertIdle(db)

block statement_move_transfers_the_active_use:
  var db = newDatabase()
  var original = prepareStmt(db, "select 1")
  var moved = move(original)
  doAssertRaises(DbError):
    startQuery(db, original)
  startQuery(db, moved)
  doAssert stepQuery(db, moved, true)
  stopQuery(db, moved)
  finalizeStmt(moved)
  db.reset()
  doAssert liveStatements(db) == 0

block close_with_outstanding_statements_is_safe:
  for limit in [0, 1]:
    var closed = 0
    block:
      var db = newDatabase(maxPlans = limit)
      observeClose(db, closed)
      var statement = prepareStmt(db, "select 1")
      startQuery(db, statement)
      doAssert stepQuery(db, statement, true)
      db.close()
      doAssert db.borrowRaw == nil
      doAssertRaises(DbError):
        discard stepQuery(db, statement, true)
      doAssertRaises(DbError):
        db.reset()
      doAssertRaises(DbError):
        discard db.allValues()
      doAssertRaises(DbError):
        dbError(db)
    doAssert closed == 1

block borrowed_statements_can_finish_cleanup_after_close:
  var closed = 0
  var db = newDatabase()
  observeClose(db, closed)
  var borrowed: PStmt
  doAssert prepare_v2(db.borrowRaw, "select 1", -1, borrowed, nil) == SQLITE_OK
  db.close()
  doAssert closed == 0
  doAssert sqlite3.finalize(borrowed) == SQLITE_OK
  doAssert closed == 1

echo "SQLite managed connection tests passed"
