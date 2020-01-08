import unittest
import db_postgres

suite "test postgres":
  setup:
    let db = open("localhost", "postgres", "postgres", "postgres")
  
  teardown:
    db.close()