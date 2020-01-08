import unittest
import db_postgres

suite "test postgres":
  setup:
    let db = open("localhost", "postgres", "postgress", "postgres")
  
  teardown:
    db.close()