import std/[algorithm, assertions, json, math, options]
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
      select tb_serial(typinteger ->> "key")
  )
  doAssert not compiles(block:
    let values = @[1, 2]
    discard query:
      select tb_serial(typinteger)
      where typinteger > any(?values)
  )
  doAssert not compiles(block:
    let values = "[1,2]"
    discard query:
      select tb_serial(typinteger)
      where typinteger == any(?values)
  )
  doAssert not compiles(block:
    discard query:
      select tb_json(jsonPathText(typjson, "{a}"))
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

block sqlite_sequence_membership:
  sq.exec(db, sql"insert into tb_serial values (2, 7), (3, 11)")
  let ids = @[1, 3]
  let selected = query:
    select tb_serial(typserial)
    where typserial == any(?ids)
    orderby typserial
  doAssert selected == @[1, 3]
  let minimum = 10
  let inCte = query:
    with chosen(select tb_serial(typserial, typinteger) where typserial == any(?ids))
    select chosen(typserial)
    where typinteger >= ?minimum
    orderby typserial
  doAssert inCte == @[1, 3]
  let someRows = query:
    select tb_serial(typserial)
    where typserial == some(?ids)
    orderby typserial
  doAssert someRows == @[1, 3]
  let fixed = [1, 3]
  let excluded = query:
    select tb_serial(typserial)
    where typserial != all(?fixed)
    orderby typserial
  doAssert excluded == @[2]
  let empty: seq[int] = @[]
  type Row = object
    value: Option[bool]
  let noMatches = query(Row):
    select tb_serial((typserial == any(?empty)) as value)
    where typserial == 1
  doAssert noMatches == @[Row(value: some(false))]
  let allMatch = query(Row):
    select tb_serial((typserial != all(?empty)) as value)
    where typserial == 1
  doAssert allMatch == @[Row(value: some(true))]
  let missing = none(seq[int])
  let nullArray = query(Row):
    select tb_serial((typserial == any(?missing)) as value)
    where typserial == 1
  doAssert nullArray == @[Row(value: none(bool))]
  let nullExclusion = query(Row):
    select tb_serial((typserial != all(?missing)) as value)
    where typserial == 1
  doAssert nullExclusion == @[Row(value: none(bool))]
  let withNull = @[some(1), none(int)]
  let nullElement = query(Row):
    select tb_serial((typserial == any(?withNull)) as value)
    where typserial == 2
  doAssert nullElement == @[Row(value: none(bool))]
  createProc findIds:
    select tb_serial(typserial)
    where typserial == any(?values)
    orderby typserial
  doAssert db.findIds(@[1, 3]) == @[1, 3]
  createIter eachId:
    select tb_serial(typserial)
    where typserial == any(?values)
    orderby typserial
  var iterated: seq[int]
  for id in db.eachId(@[1, 3]):
    iterated.add id
  doAssert iterated == @[1, 3]
  let tooLarge = @[high(uint64)]
  doAssertRaises(ValueError):
    discard query:
      select tb_serial(typserial)
      where typserial == any(?tooLarge)
  let notFinite = @[NaN]
  doAssertRaises(ValueError):
    discard query:
      select tb_serial(typserial)
      where typserial == any(?notFinite)
  sq.exec(db, sql"create table tb_boolean (typboolean boolean not null)")
  sq.exec(db, sql"insert into tb_boolean values (0), (1)")
  let flags = @[true]
  let trueValues = query:
    select tb_boolean(typboolean)
    where typboolean == any(?flags)
  doAssert trueValues == @[true]

block sqlite_sequence_strings_are_bound:
  sq.exec(db, sql"create table tb_string (typstring text not null)")
  let values = @["", "NULL", "comma,value", "quote\"here", "back\\slash",
    "'); drop table tb_serial; --", " space "]
  for value in values:
    query:
      insert tb_string(typstring = ?value)
  let found = query:
    select tb_string(typstring)
    where typstring == any(?values)
    orderby typstring
  doAssert found == sorted(values)
  let nul = @["nul\0value"]
  doAssertRaises(ValueError):
    discard query:
      select tb_string(typstring)
      where typstring == any(?nul)
  doAssert sq.getValue(db, sql"select count(*) from tb_serial") == "3"

block sqlite_json_access:
  sq.exec(db, sql"create table tb_json (typjson json not null)")
  sq.exec(db, sql"insert into tb_json values (?)",
    """{"name":"alpha","nested":{"enabled":true},"items":[10,20],"empty":null}""")
  let key = "name"
  let rows = query:
    select tb_json((typjson ->> ?key) as name,
      ((typjson -> "items") ->> 1) as second,
      (typjson -> "nested") as nested,
      ((typjson -> "nested") -> "enabled") as enabled,
      ((typjson -> "nested") ->> "enabled") as enabledText)
  doAssert rows == @[("alpha", "20", %*{"enabled": true}, %true, "1")]
  let name = "alpha"
  let matching = query:
    select tb_json((typjson ->> "name") as name)
    where (typjson ->> "name") == ?name
  doAssert matching == @["alpha"]
  type Row = object
    missing: Option[string]
  let absent = query(Row):
    select tb_json((typjson ->> "missing") as missing)
  doAssert absent == @[Row(missing: none(string))]
  type JsonRow = object
    empty, missing: Option[JsonNode]
  let jsonNulls = query(JsonRow):
    select tb_json((typjson -> "empty") as empty,
      (typjson -> "missing") as missing)
  doAssert jsonNulls == @[JsonRow(empty: some(newJNull()), missing: none(JsonNode))]

block sqlite_null_safe_comparisons:
  let missing = none(string)
  let matches = query:
    select tb_serial(typserial)
    where isNotDistinctFrom((nil as `text`), ?missing)
    orderby typserial
  doAssert matches == @[1, 2, 3]
  let number = 42
  let different = query:
    select tb_serial(typserial)
    where isDistinctFrom(typinteger, ?number)
    orderby typserial
  doAssert different == @[2, 3]
  let values = query:
    select tb_serial(isDistinctFrom(typinteger, nil) as present,
      isNotDistinctFrom(nil, nil) as equal)
    where typserial == 1
  doAssert values == @[(true, true)]
close(db)
