import std/assertions
import ormin
importModel(DbBackend.sqlite, "model_sqlite", includeStatic = true)

static:
  var db: DbConn
  doAssert not compiles(block:
    discard query:
      select tb_serial(sqlCast(typinteger, "text"))
  )
  doAssert not compiles(block:
    discard query:
      select tb_serial(typinteger)
      where typinteger == any(typinteger)
  )
  doAssert not compiles(block:
    discard query:
      select tb_json(typjson ->> "key")
  )
  doAssert not compiles(block:
    discard query:
      select tb_serial(typinteger)
      where isDistinctFrom(typinteger, nil)
  )
