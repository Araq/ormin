import unittest
import ormin/ormin_sqlite as orm_sqlite
from db_connector/db_sqlite import exec, getValue

import ormin

importModel(DbBackend.sqlite, "static_only_model", includeStatic = true)

let db {.global.} = orm_sqlite.open(":memory:", "", "", "")

suite "importModel includeStatic":
  test "sqlSchema is generated and usable without a generated Nim model":
    let schema: DbSql = sqlSchema
    check ($schema).len > 0

    db.createTable(sqlSchema)
    db.exec(sql"insert into user_static (id, name) values (?, ?)", 1, "Ada")

    let row = query:
      select user_static(id, name)
      where id == 1
      limit 1

    check row.id == 1
    check row.name == "Ada"
    check db.getValue(sql"select count(*) from sqlite_master where type='table' and name = 'user_static'") == "1"

    db.dropTable(sqlSchema, "user_static")
    check db.getValue(sql"select count(*) from sqlite_master where type='table' and name = 'user_static'") == "0"
