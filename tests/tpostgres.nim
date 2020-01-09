import unittest, sequtils, strformat
import ../ormin

importModel(DbBackend.postgre, "model_postgres")

static:
  functions.add([
    Function(name: "upper", arity: 1, typ: dbVarchar),
    Function(name: "lower", arity: 1, typ: dbVarchar),
  ])

suite "test postgres":
  let db {.global.} = open("localhost", "test", "test", "test")

  test "insert data":
    for i in 1..3:
      let name = fmt"john{i}"
      let password = fmt"pass{i}"
      query:
        insert users(id = ?i, name = ?name, password = ?password,
                    lastOnline = !!"CURRENT_TIMESTAMP")

  test "query data":
    let users = query:
      select users(id, name, password)
    assert users == [1, 2, 3].mapIt((id: it, name: fmt"john{it}",
                                    password: fmt"pass{it}"))

  test "query count":
    let count = query:
      select users(count(_))
      limit 1
    assert count == 3

  test "update data":
    let updatedstr = "updated"
    let id = 3
    query:
      update users(name = name || ?updatedstr,
                  lastOnline = !!"CURRENT_TIMESTAMP")
      where id == ?id
    let updatedname = query:
      select users(name)
      where id == ?id
      limit 1
    assert updatedname == fmt"john{id}{updatedstr}"

  # test "delete data":
  #   let id = 3
  #   query:
  #     delete users
  #     where id == ?id
  #   let user = query:
  #     select users(_)
  #     where id == ?id
  #   assert user == []

  test "more sql function upper":
    let id = 1
    let name = query:
      select users(upper(name))
      where id == ?id
      limit 1
    assert name == "JOHN1"

  test "more sql function lower":
    let id = 1
    let name = query:
      select users(lower(name))
      where id == ?id
      limit 1
    assert name == "john1"

  test "ormin sql function coalesce":
    let id = 1
    let name = query:
      select users(coalesce(name))
      where id == ?id
      limit 1
    assert name == "john1"

  db.close()
