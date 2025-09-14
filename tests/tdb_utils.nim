import unittest, os, sequtils
import db_connector/db_common
from db_connector/db_sqlite import open, exec, getValue
import ormin/db_utils

let
  db {.global.} = open(":memory:", "", "", "")
  testDir = currentSourcePath.parentDir()
  sqlFile = testDir / "db_utils_case_quoted.sql"

let sqlContent = """
-- lower case, upper case, and quoted table names
  create table lower_table (
    id integer primary key
  );

  CREATE TABLE UPPER_TABLE (
    id integer primary key
  );

  create table "Quoted_Table" (
    id integer primary key
  );
"""

writeFile(sqlFile, sqlContent)

suite "db_utils: case and quoted names":
  test "check tables names":
    let pairs = tablePairs(sqlFile).toSeq()
    check pairs.len == 3
    check pairs[0][0] == ("lower_table")
    check pairs[1][0] == ("upper_table")
    check pairs[2][0] == ("quoted_table")


    echo pairs[0][1].repr()
    check pairs[0][1] == ("create table lower_table (id  integer  primary  key);")

  test "createTable creates all tables from SQL file":
    db.createTable(sqlFile)
    let countAll = db.getValue(sql"select count(*) from sqlite_master where type='table' and name in ('lower_table','UPPER_TABLE','Quoted_Table')")
    check countAll == "3"

  test "createTable with specific lowercased name matches quoted":
    # Use a new in-memory DB for isolation
    let db2 = open(":memory:", "", "", "")
    db2.createTable(sqlFile, "quoted_table")
    let countQuoted = db2.getValue(sql"select count(*) from sqlite_master where type='table' and name = 'Quoted_Table'")
    check countQuoted == "1"
