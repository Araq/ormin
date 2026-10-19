import std/assertions

import db_connector/db_sqlite as sqliteDb except DbConn, open, close, dbError
import ormin
importModel(DbBackend.sqlite, "model_sqlite", includeStatic = true)

proc newDatabase(): DbConn =
  result = open(":memory:", "", "", "")
  sqliteDb.exec(result, sql"CREATE TABLE tb_string(typstring TEXT NOT NULL UNIQUE)")
  sqliteDb.exec(result, sql"INSERT INTO tb_string VALUES ('first'), ('second')")

block repeated_queries_release_statements_before_connection_close:
  let db = newDatabase()
  for index in 0 ..< 100:
    let rows = query:
      select tb_string(typstring)
    doAssert rows.len == 2
    let typed = query(string):
      select tb_string(typstring)
    doAssert typed.len == 2
    let missing = "missing"
    let absent = tryQuery:
      select tb_string(typstring)
      where typstring == ?missing
      limit 1
    discard absent
  close(db)

block unused_routine_declarations_do_not_prepare_statements:
  let db = newDatabase()
  createProc unusedProc:
    select tb_string(typstring)
  createIter unusedIterator:
    select tb_string(typstring)
  close(db)

createIter strings:
  select tb_string(typstring)

createProc allStrings:
  select tb_string(typstring)

block generated_proc_and_complete_iteration_release_statements:
  let db = newDatabase()
  for index in 0 ..< 100:
    doAssert allStrings(db).len == 2
    var count = 0
    for value in strings(db):
      inc count
    doAssert count == 2
  close(db)

block iterator_break_releases_its_statement:
  let db = newDatabase()
  for value in strings(db):
    doAssert value.len > 0
    break
  close(db)

proc returnFromIterator(db: DbConn): string =
  for value in strings(db):
    return value

block caller_return_releases_iterator_statement:
  let db = newDatabase()
  doAssert returnFromIterator(db).len > 0
  close(db)

block caller_exception_releases_iterator_statement:
  let db = newDatabase()
  doAssertRaises ValueError:
    for value in strings(db):
      raise newException(ValueError, value)
  close(db)

type
  ReadFailure = distinct string
  BindFailure = distinct string

proc fromQueryHook*(dest: var ReadFailure, value: string) =
  raise newException(ValueError, "result conversion failed")

proc toQueryHook*(dest: var string, value: BindFailure) =
  raise newException(ValueError, "parameter conversion failed")

block result_hook_exception_releases_statement:
  let db = newDatabase()
  doAssertRaises ValueError:
    discard query(ReadFailure):
      select tb_string(typstring)
  close(db)

block binding_hook_exception_releases_statement:
  let db = newDatabase()
  let value = BindFailure("first")
  doAssertRaises ValueError:
    discard query:
      select tb_string(typstring)
      where typstring == ?value
  close(db)

block execution_error_preserves_error_and_releases_statement:
  let db = newDatabase()
  doAssertRaises DbError:
    query:
      insert tb_string(typstring = "first")
  # The failed query must not poison a subsequent query or connection close.
  let rows = query:
    select tb_string(typstring)
  doAssert rows.len == 2
  close(db)
