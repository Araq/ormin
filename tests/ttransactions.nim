import unittest, os, strformat
import ormin
import ormin/db_utils
when NimVersion < "1.2.0": import ./compat

let testDir = currentSourcePath.parentDir()

when defined postgre:
  when defined(macosx):
    {.passL: "-Wl,-rpath,/opt/homebrew/lib/postgresql@14".}
  from db_connector/db_postgres import exec, getValue
  const backend = DbBackend.postgre
  importModel(backend, "forum_model_postgres")
  const sqlFileName = "forum_model_postgres.sql"
  let db {.global.} = open("localhost", "test", "test", "test_ormin")
else:
  from db_connector/db_sqlite import exec, getValue
  const backend = DbBackend.sqlite
  importModel(backend, "forum_model_sqlite")
  const sqlFileName = "forum_model_sqlite.sql"
  var memoryPath = testDir & "/" & ":memory:"
  let db {.global.} = open(memoryPath, "", "", "")

var sqlFilePath = Path(testDir & "/" & sqlFileName)

# Fresh schema
db.dropTable(sqlFilePath)
db.createTable(sqlFilePath)

suite &"Transactions ({backend})":

  test "commit on success":
    transaction:
      query:
        insert person(id = ?(101), name = ?"john101", password = ?"p101", email = ?"john101@mail.com", salt = ?"s101", status = ?"ok")
    check db.getValue(sql"select count(*) from person where id = 101") == "1"

  test "rollback on error":
    # prepare one row
    query:
      insert person(id = ?(201), name = ?"john201", password = ?"p201", email = ?"john201@mail.com", salt = ?"s201", status = ?"ok")
    # in transaction insert a new row and then violate PK
    try:
      transaction:
        query:
          insert person(id = ?(202), name = ?"john202", password = ?"p202", email = ?"john202@mail.com", salt = ?"s202", status = ?"ok")
        # duplicate key error
        query:
          insert person(id = ?(201), name = ?"dup", password = ?"p", email = ?"e", salt = ?"s", status = ?"x")
      check false # should not reach
    except DbError as e:
      discard
    # both inserts inside the transaction should be rolled back
    check db.getValue(sql"select count(*) from person where id = 202") == "0"
    check db.getValue(sql"select count(*) from person where id = 201 and name = 'dup'") == "0"

  test "rollback on error with else":
    # prepare one row
    var failed = false
    query:
      insert person(id = ?(501), name = ?"john501", password = ?"p501", email = ?"john501@mail.com", salt = ?"s501", status = ?"ok")
    # in transaction insert a new row and then violate PK
    transaction:
      query:
        insert person(id = ?(502), name = ?"john502", password = ?"p502", email = ?"john502@mail.com", salt = ?"s502", status = ?"ok")
      # duplicate key error
      query:
        insert person(id = ?(501), name = ?"dup", password = ?"p", email = ?"e", salt = ?"s", status = ?"x")
      check false # should not reach
    else:
      failed = true

    check failed
    # both inserts inside the transaction should be rolled back
    check db.getValue(sql"select count(*) from person where id = 502") == "0"
    check db.getValue(sql"select count(*) from person where id = 501 and name = 'dup'") == "0"

  test "commit normally with else":
    # prepare one row
    var failed = false
    query:
      insert person(id = ?(601), name = ?"john601", password = ?"p601", email = ?"john601@mail.com", salt = ?"s601", status = ?"ok")
    # in transaction insert a new row and then violate PK
    transaction:
      query:
        insert person(id = ?(602), name = ?"john602", password = ?"p602", email = ?"john602@mail.com", salt = ?"s602", status = ?"ok")
      query:
        insert person(id = ?(603), name = ?"dup", password = ?"p", email = ?"e", salt = ?"s", status = ?"x")
    else:
      failed = true

    check not failed
    # both inserts inside the transaction should be rolled back
    check db.getValue(sql"select count(*) from person where id = 603") == "1"
    check db.getValue(sql"select count(*) from person where id = 602") == "1"
    check db.getValue(sql"select count(*) from person where id = 601") == "1"

  test "transaction set false on DbError":
    var failed = false
    transaction:
      query:
        insert person(id = ?(301), name = ?"john301", password = ?"p301", email = ?"john301@mail.com", salt = ?"s301", status = ?"ok")
      query:
        insert person(id = ?(301), name = ?"dup", password = ?"p", email = ?"e", salt = ?"s", status = ?"x")
    else:
      failed = true
    check failed
    check db.getValue(sql"select count(*) from person where id = 301") == "0"

  test "nested savepoints":
    var failed = false
    transaction:
      query:
        insert person(id = ?(401), name = ?"john401", password = ?"p401", email = ?"john401@mail.com", salt = ?"s401", status = ?"ok")
      var innerOk = true
      transaction:
        query:
          insert person(id = ?(402), name = ?"john402", password = ?"p402", email = ?"john402@mail.com", salt = ?"s402", status = ?"ok")
        query:
          insert person(id = ?(401), name = ?"dup401", password = ?"p", email = ?"e", salt = ?"s", status = ?"x")
      else:
        innerOk = false

      check innerOk == false

      # after inner rollback, we can still insert another row and commit outer
      query:
        insert person(id = ?(403), name = ?"john403", password = ?"p403", email = ?"john403@mail.com", salt = ?"s403", status = ?"ok")
    else:
      failed = true
    check db.getValue(sql"select count(*) from person where id in (401,402,403)") == "2"
