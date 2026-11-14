import std/[assertions, json, options]
import db_connector/db_sqlite as sq except DbConn, open, close, dbError
import ormin
importModel(DbBackend.sqlite, "model_sqlite", includeStatic = true)

static:
  var db: DbConn
  doAssert not compiles(block:
    discard query:
      select tb_serial((typinteger as `uuid`) as value)
  )
  doAssert not compiles(block:
    discard query:
      select tb_serial((typinteger as `integer[]`) as value)
  )
  doAssert not compiles(block:
    discard query:
      select tb_serial(typinteger as `column_alias`)
  )
  doAssert not compiles(block:
    discard query:
      select tb_serial((typinteger as `text`) as `column_alias`)
  )
  doAssert not compiles(block:
    discard query:
      select tb_serial(typinteger as "text")
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

let db = open(":memory:", "", "", "")
sq.exec(db, sql"create table tb_serial (typserial integer primary key, typinteger integer not null)")
sq.exec(db, sql"insert into tb_serial values (1, 42)")

block sqlite_casts:
  let number = 42
  let numericText = "2.5"
  let rows = query:
    select tb_serial((?number as `text`) as label,
      (?numericText as `double precision`) as value,
      (typinteger as `text`) as columnValue)
    where typserial == 1
  doAssert rows == @[("42", 2.5, "42")]
  let aliasNamedText = query:
    select tb_serial(typinteger as text)
    where typserial == 1
  doAssert aliasNamedText == @[42]
  let input = %*{"value": "12"}
  let converted = query:
    select tb_serial((%input["value"] as `integer`) as value)
    where typserial == 1
  doAssert converted == @[12]
  let invalidNumber = "not a number"
  let sqliteConversion = query:
    select tb_serial((?invalidNumber as `integer`) as value)
    where typserial == 1
  doAssert sqliteConversion == @[0]
  type Row = object
    value: Option[int]
  let missing = none(string)
  let nullValue = query(Row):
    select tb_serial((?missing as `integer`) as value)
    where typserial == 1
  doAssert nullValue == @[Row(value: none(int))]
  createProc castValue:
    select tb_serial((?value as `text`) as label)
    where typserial == 1
  doAssert db.castValue(42) == @["42"]
  doAssert db.castValue("hello") == @["hello"]
  let injection = "x'); drop table tb_serial; --"
  let bound = query:
    select tb_serial((?injection as `text`) as value)
    where typserial == 1
  doAssert bound == @[injection]
  doAssert sq.getValue(db, sql"select count(*) from tb_serial") == "1"
close(db)
