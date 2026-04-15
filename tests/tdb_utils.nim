import unittest, os, sequtils
import db_connector/db_common
from db_connector/db_sqlite import open, exec, getValue
import ormin/db_utils

const usesLegacySqliteDropNames = defined(sqlite) and defined(orminLegacySqliteDropNames)

let
  db {.global.} = open(":memory:", "", "", "")
  testDir = currentSourcePath.parentDir()
  sqlFile = Path(testDir / "db_utils_case_quoted.sql")

let sqlContent = """
-- lower case, upper case, and quoted table names
  create table lower_table (
    id integer primary key
  );

  CREATE TABLE UPPER_TABLE (
    id integer primary key
  );

  -- std sql quoted table name
  create table "Quoted Table" (
    id integer primary key
  );

  -- sqlite quoted table name
  create table `Quoted Table2` (
    id integer primary key
  );

  create table "UPPER_QUOTED" (
    id integer primary key
  );

  create table "A""B" (
    id integer primary key
  );
"""

const staticSqlContent = staticLoad("db_utils_case_quoted.sql")

writeFile($sqlFile, sqlContent)

suite "db_utils: case and quoted names":
  test "staticLoad returns typed compile-time SQL":
    let loaded: DbSql = staticSqlContent
    check $loaded == sqlContent

  test "quoteDbIdentifier quotes and escapes identifiers":
    check $quoteDbIdentifier("lower_table") == "lower_table"
    check $quoteDbIdentifier("UPPER_TABLE") == "upper_table"
    check $quoteDbIdentifier("\"Quoted Table\"") == "\"Quoted Table\""
    check $quoteDbIdentifier("`Quoted Table2`") == "\"Quoted Table2\""
    check $quoteDbIdentifier("'Quoted Table3'") == "\"Quoted Table3\""
    check $quoteDbIdentifier("\"UPPER_TABLE\"") == "\"UPPER_TABLE\""
    check $quoteDbIdentifier("\"A\"\"B\"") == "\"A\"\"B\""
    check $quoteDbIdentifier("schema.table") == "schema.table"
    check $quoteDbIdentifier("Schema.\"Mixed Table\"") == "schema.\"Mixed Table\""
    check $quoteDbIdentifier("weird\"name") == "\"weird\"\"name\""

  test "check tables names":
    let pairs = tablePairs(sqlContent).toSeq()
    check pairs.len == 6
    check pairs[0][0] == ("lower_table")
    check pairs[1][0] == ("upper_table")
    check pairs[2][0] == ("quoted table")
    check pairs[3][0] == ("quoted table2")
    check pairs[4][0] == ("upper_quoted")
    check pairs[5][0] == ("a\"b")

    check pairs[0][1] == "create table lower_table(id  integer  primary key );"
    check pairs[1][1] == "create table UPPER_TABLE(id  integer  primary key );"
    check pairs[2][1] == "create table \"Quoted Table\"(id  integer  primary key );"
    check pairs[4][1] == "create table \"UPPER_QUOTED\"(id  integer  primary key );"
    check pairs[5][1] == "create table \"A\"\"B\"(id  integer  primary key );"

  test "tablePairs parses compile-time SQL":
    let pairs = tablePairs(staticSqlContent).toSeq()
    check pairs.len == 6
    check pairs[0][0] == "lower_table"
    check pairs[1][0] == "upper_table"
    check pairs[2][0] == "quoted table"
    check pairs[3][0] == "quoted table2"
    check pairs[4][0] == "upper_quoted"
    check pairs[5][0] == "a\"b"

  test "createTable creates all tables from SQL file":
    db.createTable(sqlFile)
    let countAll = db.getValue(sql"select count(*) from sqlite_master where type='table' and name in ('lower_table','UPPER_TABLE','Quoted Table')")
    check countAll == "3"

  test "createTable with specific lowercased name matches quoted":
    # Use a new in-memory DB for isolation
    let db2 = open(":memory:", "", "", "")
    db2.createTable(sqlFile, "quoted table")
    let countQuoted = db2.getValue(sql"select count(*) from sqlite_master where type='table' and name = 'Quoted Table'")
    check countQuoted == "1"

  test "createTable creates all tables from compile-time SQL":
    let db2 = open(":memory:", "", "", "")
    db2.createTable(staticSqlContent)
    let countAll = db2.getValue(sql("select count(*) from sqlite_master where type='table' and name in ('lower_table','UPPER_TABLE','Quoted Table','Quoted Table2','UPPER_QUOTED','A\"B')"))
    check countAll == "6"

  test "createTable with specific lowercased name matches quoted from compile-time SQL":
    let db2 = open(":memory:", "", "", "")
    db2.createTable(staticSqlContent, "quoted table")
    let countQuoted = db2.getValue(sql"select count(*) from sqlite_master where type='table' and name = 'Quoted Table'")
    check countQuoted == "1"

  test "dropTable handles quoted names from SQL file":
    let db2 = open(":memory:", "", "", "")
    db2.createTable(sqlFile)
    db2.dropTable(sqlFile, "quoted table2")
    let countQuoted = db2.getValue(sql"select count(*) from sqlite_master where type='table' and name = 'Quoted Table2'")
    when usesLegacySqliteDropNames:
      check countQuoted == "1"
    else:
      check countQuoted == "0"

    db2.dropTable(sqlFile, "upper_quoted")
    let countUpperQuoted = db2.getValue(sql"select count(*) from sqlite_master where type='table' and name = 'UPPER_QUOTED'")
    when usesLegacySqliteDropNames:
      check countUpperQuoted == "1"
    else:
      check countUpperQuoted == "0"

    db2.dropTable(sqlFile, "a\"b")
    let countEscapedQuoted = db2.getValue(sql("select count(*) from sqlite_master where type='table' and name = 'A\"B'"))
    when usesLegacySqliteDropNames:
      check countEscapedQuoted == "1"
    else:
      check countEscapedQuoted == "0"

  test "dropTable removes tables from compile-time SQL":
    let db2 = open(":memory:", "", "", "")
    db2.createTable(staticSqlContent)
    db2.dropTable(staticSqlContent, "quoted table2")
    let countQuoted = db2.getValue(sql"select count(*) from sqlite_master where type='table' and name = 'Quoted Table2'")
    when usesLegacySqliteDropNames:
      check countQuoted == "1"
    else:
      check countQuoted == "0"

    db2.dropTable(staticSqlContent, "upper_quoted")
    let countUpperQuoted = db2.getValue(sql"select count(*) from sqlite_master where type='table' and name = 'UPPER_QUOTED'")
    when usesLegacySqliteDropNames:
      check countUpperQuoted == "1"
    else:
      check countUpperQuoted == "0"

    db2.dropTable(staticSqlContent, "a\"b")
    let countEscapedQuoted = db2.getValue(sql("select count(*) from sqlite_master where type='table' and name = 'A\"B'"))
    when usesLegacySqliteDropNames:
      check countEscapedQuoted == "1"
    else:
      check countEscapedQuoted == "0"

    db2.dropTable(staticSqlContent)
    let countAll = db2.getValue(sql("select count(*) from sqlite_master where type='table' and name in ('lower_table','UPPER_TABLE','Quoted Table','Quoted Table2','UPPER_QUOTED','A\"B')"))
    when usesLegacySqliteDropNames:
      check countAll == "3"
    else:
      check countAll == "0"
