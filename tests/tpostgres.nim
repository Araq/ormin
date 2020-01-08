import unittest
import db_postgres

suite "test postgres":
  setup:
    let db = open("localhost", "postgres", "postgress", "postgres")
  
  test "create table":
    db.exec(sql("""CREATE TABLE myTable (
                 id integer,
                 name varchar(50) not null)"""))

  teardown:
    db.close()