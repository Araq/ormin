# Package

version       = "0.1.0"
author        = "Araq"
description   = "Prepared SQL statement generator. A lightweight ORM."
license       = "MIT"

# Dependencies

requires "nim >= 0.17.2"
requires "websocket >= 0.2.2"

bin = @["tools/ormin_importer"]

skipDirs = @["examples"]
installExt = @["nim"]

task initcommonsqlite, "Init sqlite for common test":
  if existsFile("tests/test.db"): exec "rm tests/test.db"
  exec "tests/forum_model_sqlite.sh"

task initcommonpostgre, "init postgresql for common test":
  exec """psql -U test -wq -h localhost -c "drop table if exists person, thread, post, session, antibot cascade"
  """
  exec "psql -U test -wq -h localhost -f tests/forum_model_postgres.sql"

task tcommonsqlite, "Test common of sqlite":
  --r
  setCommand "c", "tests/tforum"
before tcommonsqlite:
  initcommonsqliteTask()

task tdcommonsqlite, "Test common of sqlite with debugOrminSql":
  --r
  --define: debugOrminSql
  setCommand "c", "tests/tforum"
before tdcommonsqlite:
  initcommonsqliteTask()

task tcommonpostgre, "Test common of postgresql":
  --r
  --define: postgre
  setCommand "c", "tests/tforum"
before tcommonpostgre:
  initcommonpostgreTask()

task tdcommonpostgre, "Test common of postgresql with debugOrminSql":
  --r
  --define: debugOrminSql
  --define: postgre
  setCommand "c", "tests/tforum"
before tdcommonpostgre:
  initcommonpostgreTask()

task tcommonfuncsqlite, "Test common sql function of sqlite":
  --r
  setCommand "c", "tests/tfunction"
before tcommonfuncsqlite:
  initcommonsqliteTask()

task tdcommonfuncsqlite, "Test common sql function of sqlite with debugOrminSql":
  --r
  --define: debugOrminSql
  setCommand "c", "tests/tfunction"
before tdcommonfuncsqlite:
  initcommonsqliteTask()

task tcommonfuncpostgre, "Test common sql function of postgresql":
  --r
  --define: postgre
  setCommand "c", "tests/tfunction"
before tcommonfuncpostgre:
  initcommonpostgreTask()

task tdcommonfuncpostgre, "Test common sql function of postgresql with debugOrminSql":
  --r
  --define: debugOrminSql
  --define: postgre
  setCommand "c", "tests/tfunction"
before tdcommonfuncpostgre:
  initcommonpostgreTask()

task test, "Run all test suite of common feature":
  exec "nimble tcommonsqlite"
  exec "nimble tcommonpostgre"
  exec "nimble tcommonfuncsqlite"
  exec "nimble tcommonfuncpostgre"

