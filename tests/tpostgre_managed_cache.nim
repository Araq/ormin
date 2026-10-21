import std/[os, strutils, unittest]

import db_connector/db_postgres as pg
from db_connector/postgres import pqgetvalue, pqnfields
import ormin
import ormin/ormin_postgre as orminPostgre

importModel(DbBackend.postgre, "model_postgre")

let target = getEnv("TEST_DATABASE_URL", "postgresql://test@localhost/test_ormin")

proc queryInt(db: var ManagedDbConn; sql: string; value: int): string =
  let statement = orminPostgre.prepareStmt(db, sql)
  orminPostgre.startBindings(statement, 1)
  orminPostgre.bindParam(db, statement, 1, value, int)
  orminPostgre.startQuery(db, statement)
  doAssert orminPostgre.stepQuery(db, statement, true)
  result = $pqgetvalue(queryResult, queryI, 0)
  orminPostgre.stopQuery(db, statement)

proc querySerial(db: var ManagedDbConn; value: int): seq[int] =
  result = query:
    select tb_serial(typinteger)
    where typinteger == ?value

proc failAfterStart(db: var ManagedDbConn) =
  let statement = orminPostgre.prepareStmt(db, "select 1")
  orminPostgre.startQuery(db, statement)
  raise newException(ValueError, "conversion failed")

proc scopedBackendPid(target: string): int =
  var db = openManaged(target)
  discard queryInt(db, "select $1::integer", 7)
  result = pg.getValue(db.borrowRaw, sql"select pg_backend_pid()").parseInt()

suite "owned PostgreSQL plan cache":
  test "same SQL uses one plan, while excess queries execute directly":
    var db = openManaged(target, maxPlans = 1)
    for value in 0 ..< 100:
      check queryInt(db, "select $1::integer", value) == $value
      check queryInt(db, "select $1::integer + 1", value) == $(value + 1)
    check db.managedPlanCount == 1
    check pg.getValue(db.borrowRaw, sql"select count(*) from pg_prepared_statements") == "1"
    db.close()

  test "zero plan limit uses direct execution":
    var db = openManaged(target, maxPlans = 0)
    for value in 0 ..< 25:
      check queryInt(db, "select $1::integer", value) == $value
    check db.managedPlanCount == 0
    check pg.getValue(db.borrowRaw, sql"select count(*) from pg_prepared_statements") == "0"

  test "move and explicit close transfer and release ownership":
    var first = openManaged(target)
    discard queryInt(first, "select $1::integer", 7)
    pg.exec(first.borrowRaw,
      sql"create temporary table tb_serial (typserial serial primary key, typinteger integer)")
    pg.exec(first.borrowRaw, sql"insert into tb_serial(typinteger) values (17)")
    var second = move(first)
    check first.borrowRaw == nil
    check queryInt(second, "select $1::integer", 8) == "8"
    check querySerial(second, 17) == @[17]
    second.close()
    check second.borrowRaw == nil
    second.close()

  test "move assignment releases the replaced session":
    var first = openManaged(target)
    var second = openManaged(target)
    let replacedPid = pg.getValue(second.borrowRaw, sql"select pg_backend_pid()").parseInt()
    second = move(first)
    check first.borrowRaw == nil
    check queryInt(second, "select $1::integer", 11) == "11"
    let observer = pg.open("", "", "", target)
    check pg.getValue(observer, sql("select count(*) from pg_stat_activity where pid = " &
      $replacedPid)) == "0"
    pg.close(observer)
    second.close()

  test "closed connections reject queries and reset":
    var db = openManaged(target)
    db.close()
    expect DbError:
      discard orminPostgre.prepareStmt(db, "select 1")
    expect DbError:
      db.reset()

  test "generated query blocks use the owned connection cache":
    var db = openManaged(target)
    pg.exec(db.borrowRaw,
      sql"create temporary table tb_serial (typserial serial primary key, typinteger integer not null)")
    let value = 31
    query:
      insert tb_serial(typinteger = ?value)
    let rows = query:
      select tb_serial(typinteger)
      where typinteger == ?value
    check rows == @[value]
    for _ in 0 ..< 20:
      let repeated = query:
        select tb_serial(typinteger)
        where typinteger == ?value
      check repeated == @[value]
    check db.managedPlanCount == 2
    check pg.getValue(db.borrowRaw, sql"select count(*) from pg_prepared_statements") == "2"

    createProc lookupSerial:
      select tb_serial(typinteger)
      where typinteger == ?value
    createIter lookupSerialIter:
      select tb_serial(typinteger)
      where typinteger == ?value
    for _ in 0 ..< 20:
      check db.lookupSerial(value) == @[value]
    var iterRows: seq[int]
    for row in db.lookupSerialIter(value):
      iterRows.add row
    check iterRows == @[value]
    check db.managedPlanCount == 2
    db.close()

  test "scope destruction closes the owned session":
    let pid = scopedBackendPid(target)
    let observer = pg.open("", "", "", target)
    check pg.getValue(observer, sql("select count(*) from pg_stat_activity where pid = " &
      $pid)) == "0"
    pg.close(observer)

  test "reset discards plans and prepares them again on the new session":
    var db = openManaged(target)
    check queryInt(db, "select $1::integer", 2) == "2"
    check db.managedPlanCount == 1
    db.reset()
    check db.managedPlanCount == 0
    check pg.getValue(db.borrowRaw, sql"select count(*) from pg_prepared_statements") == "0"
    check queryInt(db, "select $1::integer", 3) == "3"
    check db.managedPlanCount == 1

  test "generated queries prepare again after reset":
    var db = openManaged(target)
    pg.exec(db.borrowRaw,
      sql"create temporary table tb_serial (typserial serial primary key, typinteger integer)")
    pg.exec(db.borrowRaw, sql"insert into tb_serial(typinteger) values (18)")
    check querySerial(db, 18) == @[18]
    check db.managedPlanCount == 1
    db.reset()
    check db.managedPlanCount == 0
    pg.exec(db.borrowRaw,
      sql"create temporary table tb_serial (typserial serial primary key, typinteger integer)")
    pg.exec(db.borrowRaw, sql"insert into tb_serial(typinteger) values (19)")
    check querySerial(db, 19) == @[19]
    check db.managedPlanCount == 1

  test "reset recovers after a server-terminated connection":
    var db = openManaged(target)
    check queryInt(db, "select $1::integer", 2) == "2"
    let pid = pg.getValue(db.borrowRaw, sql"select pg_backend_pid()").parseInt()
    let observer = pg.open("", "", "", target)
    check pg.getValue(observer, sql("select pg_terminate_backend(" & $pid & ")")) == "t"
    pg.close(observer)
    db.reset()
    check db.managedPlanCount == 0
    check queryInt(db, "select $1::integer", 3) == "3"

  test "a failed bound value retains the plan and connection":
    var db = openManaged(target)
    let statement = orminPostgre.prepareStmt(db, "select $1::integer")
    orminPostgre.startBindings(statement, 1)
    orminPostgre.bindParam(db, statement, 1, "invalid", string)
    var errorMessage = ""
    try:
      orminPostgre.startQuery(db, statement)
      orminPostgre.stopQuery(db, statement)
    except DbError as exc:
      errorMessage = exc.msg
    check "invalid input syntax" in errorMessage
    check db.managedPlanCount == 1
    check queryInt(db, "select $1::integer", 4) == "4"

  test "a failed prepare preserves the server error and leaves the connection reusable":
    var db = openManaged(target)
    for _ in 0 ..< 25:
      var errorMessage = ""
      try:
        discard orminPostgre.prepareStmt(db, "select from")
      except DbError as exc:
        errorMessage = exc.msg
      check "syntax error" in errorMessage
      check db.managedPlanCount == 0
    check queryInt(db, "select $1::integer", 9) == "9"

  test "unwinding after execution releases the result":
    var db = openManaged(target)
    for _ in 0 ..< 100:
      expect ValueError:
        failAfterStart(db)
    check queryInt(db, "select $1::integer", 10) == "10"

  test "missing server plan disables the cache without replaying the query":
    var db = openManaged(target)
    check queryInt(db, "select $1::integer", 4) == "4"
    pg.exec(db.borrowRaw, sql"deallocate all")
    var errorMessage = ""
    try:
      discard queryInt(db, "select $1::integer", 5)
    except DbError as exc:
      errorMessage = exc.msg
    check "does not exist" in errorMessage
    check db.managedPlanCount == 0
    check queryInt(db, "select $1::integer", 6) == "6"
    check pg.getValue(db.borrowRaw, sql"select count(*) from pg_prepared_statements") == "0"

  test "a schema change invalidates plans without replaying the query":
    var db = openManaged(target)
    pg.exec(db.borrowRaw, sql"create temporary table ormin_schema_test (a integer)")
    let statement = orminPostgre.prepareStmt(db, "select * from ormin_schema_test")
    block:
      orminPostgre.startQuery(db, statement)
      check pqnfields(queryResult) == 1
      orminPostgre.stopQuery(db, statement)
    pg.exec(db.borrowRaw, sql"alter table ormin_schema_test add column b integer")
    var errorMessage = ""
    try:
      block:
        orminPostgre.startQuery(db, statement)
        orminPostgre.stopQuery(db, statement)
    except DbError as exc:
      errorMessage = exc.msg
    check "cached plan must not change result type" in errorMessage
    check db.managedPlanCount == 0
    block:
      orminPostgre.startQuery(db, statement)
      check pqnfields(queryResult) == 2
      orminPostgre.stopQuery(db, statement)
