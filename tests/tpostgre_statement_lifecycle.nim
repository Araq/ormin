import std/[os, strutils, unittest]

import db_connector/db_postgres as pg
from db_connector/postgres import pqgetvalue
import ormin/ormin_postgre as orminPostgre

let target = getEnv("TEST_DATABASE_URL")
let db =
  if target.len > 0:
    pg.open("", "", "", target)
  else:
    pg.open("localhost", "test", "test", "test_ormin")

suite "PostgreSQL statement lifecycle":
  test "repeated parameterized queries leave no named plans behind":
    let before = pg.getValue(db, sql"select count(*) from pg_prepared_statements").parseInt()
    for value in 0 ..< 1000:
      let statement = orminPostgre.prepareStmt(db, "select $1::integer")
      orminPostgre.startBindings(statement, 1)
      orminPostgre.bindParam(db, statement, 1, value, int)
      orminPostgre.startQuery(db, statement)
      check orminPostgre.stepQuery(db, statement, true)
      check $pqgetvalue(queryResult, queryI, 0) == $value
      orminPostgre.stopQuery(db, statement)
    let after = pg.getValue(db, sql"select count(*) from pg_prepared_statements").parseInt()
    check after == before

  test "queries without parameters and commands do not retain named plans":
    let before = pg.getValue(db, sql"select count(*) from pg_prepared_statements").parseInt()
    let selectStatement = orminPostgre.prepareStmt(db, "select 42")
    orminPostgre.startQuery(db, selectStatement)
    check orminPostgre.stepQuery(db, selectStatement, true)
    check $pqgetvalue(queryResult, queryI, 0) == "42"
    orminPostgre.stopQuery(db, selectStatement)

    pg.exec(db, sql"create temporary table ormin_lifecycle_test (value integer)")
    for value in 0 ..< 100:
      let insertStatement = orminPostgre.prepareStmt(
        db, "insert into ormin_lifecycle_test (value) values ($1)")
      orminPostgre.startBindings(insertStatement, 1)
      orminPostgre.bindParam(db, insertStatement, 1, value, int)
      orminPostgre.startQuery(db, insertStatement)
      check orminPostgre.getAffectedRows(db, insertStatement) == 1
      orminPostgre.stopQuery(db, insertStatement)
    check pg.getValue(db, sql"select count(*) from ormin_lifecycle_test") == "100"
    pg.exec(db, sql"drop table ormin_lifecycle_test")
    let after = pg.getValue(db, sql"select count(*) from pg_prepared_statements").parseInt()
    check after == before

  test "a failed parameterized query keeps its error and leaves the connection usable":
    let statement = orminPostgre.prepareStmt(db, "select $1::integer")
    orminPostgre.startBindings(statement, 1)
    orminPostgre.bindParam(db, statement, 1, "not an integer", string)
    for attempt in 0 ..< 100:
      var errorMessage = ""
      try:
        orminPostgre.startQuery(db, statement)
        orminPostgre.stopQuery(db, statement)
      except DbError as exc:
        errorMessage = exc.msg
      check "invalid input syntax" in errorMessage
    check pg.getValue(db, sql"select 1") == "1"

  test "invalid SQL reports its error and does not poison the connection":
    let statement = orminPostgre.prepareStmt(db, "select from")
    var errorMessage = ""
    try:
      orminPostgre.startQuery(db, statement)
      orminPostgre.stopQuery(db, statement)
    except DbError as exc:
      errorMessage = exc.msg
    check "syntax error" in errorMessage
    check pg.getValue(db, sql"select 1") == "1"

pg.close(db)
