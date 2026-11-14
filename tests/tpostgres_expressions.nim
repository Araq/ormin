import std/[assertions, json, options, os, strutils]

import db_connector/db_postgres as pg except DbConn, open, close, dbError
import ormin
importModel(DbBackend.postgre, "postgres_expressions_model", includeStatic = true)

proc checkExpressions(db: DbConn) =
  pg.exec(db, sql(string(sqlSchema).replace("create table", "create temporary table")))
  pg.exec(db, sql"""insert into records values
    (1, '00000000-0000-0000-0000-000000000001',
     '{"name":"alpha","nested":{"enabled":true},"items":[10,20]}',
     1.25, 123456789012345678.123456, '{red,blue}', NULL),
    (2, '00000000-0000-0000-0000-000000000002',
     '{"name":"beta","nested":{"enabled":false},"items":[30]}',
     2.5, 1.000001, '{green,blue}', 'ready'),
    (3, '00000000-0000-0000-0000-000000000003',
     '{"name":"gamma","items":[]}', 3.75, NULL, '{}', '')""")

  block cast_parameters_preserve_their_nim_input_types:
    let number = 42
    let numericText = "2.5"
    let enabled = "true"
    let rows = query:
      select records(sqlCast(?number, "text") as label,
        sqlCast(?numericText, "double precision") as amount,
        sqlCast(?enabled, "boolean") as enabled)
      where id == 1
    doAssert rows == @[("42", 2.5, true)]
    let numeric = "123456789012345678.123456"
    let exact = query:
      select records(sqlCast(?numeric, "numeric") as value, exact_amount)
      where id == 1
    doAssert exact == @[(numeric, numeric)]

  block cast_columns_and_uuid_parameters:
    let uuid = "00000000-0000-0000-0000-000000000002"
    let rows = query:
      select records(sqlCast(id, "text") as label, public_id)
      where public_id == sqlCast(?uuid, "uuid")
    doAssert rows == @[("2", uuid)]

  block null_casts:
    let missing = none(string)
    type Row = object
      value: Option[int]
    let rows = query(Row):
      select records(sqlCast(?missing, "bigint") as value)
      where id == 1
    doAssert rows == @[Row(value: none(int))]

  block json_parameter_casts:
    let text = "{\"name\":\"alpha\"}"
    let document = %*{"name": "alpha"}
    let input = %*{"number": "42", "object": {"ok": true}, "missing": nil}
    let rows = query:
      select records(sqlCast(?text, "jsonb") as first,
        sqlCast(?document, "jsonb") as second,
        sqlCast(%input["number"], "bigint") as number,
        sqlCast(%input["object"], "jsonb") as objectValue)
      where id == 1
    doAssert rows == @[(document, document, 42, %*{"ok": true})]
    let absent = query:
      select records(id)
      where isNotDistinctFrom(note, sqlCast(%input["missing"], "text"))
    doAssert absent == @[1]

  block array_membership:
    let ids = "{1,3}"
    let selected = query:
      select records(id)
      where id == any(sqlCast(?ids, "bigint[]"))
      orderby id
    doAssert selected == @[1, 3]
    let excluded = query:
      select records(id)
      where id != all(sqlCast(?ids, "bigint[]"))
      orderby id
    doAssert excluded == @[2]
    let byTag = query:
      select records(id)
      where "blue" == any(tags)
      orderby id
    doAssert byTag == @[1, 2]
    let uuids = "{00000000-0000-0000-0000-000000000001}"
    let byUuid = query:
      select records(id)
      where public_id == any(sqlCast(?uuids, "uuid[]"))
    doAssert byUuid == @[1]
    let multidimensional = "{{1,2},{3,4}}"
    let byMatrix = query:
      select records(id)
      where id == any(sqlCast(?multidimensional, "bigint[][]"))
      orderby id
    doAssert byMatrix == @[1, 2, 3]

  block empty_and_null_arrays:
    let empty = "{}"
    let none = query:
      select records(id)
      where id == any(sqlCast(?empty, "bigint[]"))
    doAssert none.len == 0
    let allRows = query:
      select records(id)
      where id != all(sqlCast(?empty, "bigint[]"))
      orderby id
    doAssert allRows == @[1, 2, 3]
    let missing = options.none(string)
    let nullArray = query:
      select records(id)
      where id == any(sqlCast(?missing, "bigint[]"))
    doAssert nullArray.len == 0

  block json_extraction_and_paths:
    let path = "{nested,enabled}"
    let rows = query:
      select records((payload ->> "name") as name,
        ((payload -> "items") ->> 1) as item,
        jsonPath(payload, sqlCast(?path, "text[]")) as enabled,
        sqlCast(jsonPathText(payload, ?path), "boolean") as active)
      where id == 1
    doAssert rows == @[("alpha", "20", %true, true)]
    let index = 0
    let first = query:
      select records(((payload -> "items") ->> sqlCast(?index, "integer")) as item)
      where id == 1
    doAssert first == @["10"]
    type Row = object
      missing: Option[string]
    let absent = query(Row):
      select records(jsonPathText(payload, "{not,present}") as missing)
      where id == 1
    doAssert absent == @[Row(missing: options.none(string))]

  block json_and_array_containment:
    let document = %*{"nested": {"enabled": true}}
    let matching = query:
      select records(id)
      where payload @> sqlCast(?document, "jsonb")
    doAssert matching == @[1]
    let reverse = query:
      select records(id)
      where sqlCast(?document, "jsonb") <@ payload
    doAssert reverse == @[1]
    let tags = "{red}"
    let containsTag = query:
      select records(id)
      where records.tags @> sqlCast(?tags, "text[]")
    doAssert containsTag == @[1]
    let overlap = query:
      select records(id)
      where records.tags && sqlCast(?tags, "text[]")
    doAssert overlap == @[1]

  block null_safe_comparisons:
    let missing = options.none(string)
    let absent = query:
      select records(id)
      where isNotDistinctFrom(note, ?missing)
    doAssert absent == @[1]
    let present = query:
      select records(id)
      where isDistinctFrom(note, nil)
      orderby id
    doAssert present == @[2, 3]
    let values = query:
      select records(isNotDistinctFrom(nil, nil) as equal)
      where id == 1
    doAssert values == @[true]

  block generated_routines_and_parameter_order:
    createProc lookupId:
      select records(id)
      where sqlCast(id, "text") == sqlCast(?value, "text") and id >= ?minimum
    createIter lookupIds:
      select records(id)
      where id == any(sqlCast(?values, "bigint[]"))
      orderby id
    doAssert db.lookupId(2, 1) == @[2]
    doAssert db.lookupId("3", 1) == @[3]
    var rows: seq[int]
    for id in db.lookupIds("{1,3}"):
      rows.add id
    doAssert rows == @[1, 3]
    let first = "alpha"
    let second = 2
    let found = query:
      with chosen(select records(id) where (payload ->> "name") == sqlCast(?first, "text"))
      select chosen(id)
      where id < sqlCast(?second, "bigint")
    doAssert found == @[1]

  block ctes_preserve_postgres_column_types:
    let uuid = "00000000-0000-0000-0000-000000000001"
    let selected = query:
      with chosen(select records(id, public_id, sqlCast(tags, "text[]") as labels))
      select chosen(id, labels)
      where "red" == any(labels) and public_id == sqlCast(?uuid, "uuid")
    doAssert selected == @[(1, "{red,blue}")]
    let allColumns = query:
      with chosen(select records(_))
      select chosen(id)
      where "red" == any(tags) and public_id == sqlCast(?uuid, "uuid")
    doAssert allColumns == @[1]

  block cast_values_remain_bound:
    let value = "x'); drop table records; --"
    query:
      update records(note = sqlCast(?value, "text"))
      where id == 2
    doAssert pg.getValue(db, sql"select note from records where id = 2") == value
    doAssert pg.getValue(db, sql"select count(*) from records") == "3"

  block failed_cast_does_not_poison_the_connection:
    let invalid = "not an integer"
    doAssertRaises(DbError):
      discard query:
        select records(sqlCast(?invalid, "bigint") as value)
        where id == 1
    doAssert pg.getValue(db, sql"select count(*) from records") == "3"

static:
  var db: DbConn
  doAssert not compiles(block:
    discard query:
      select records(sqlCast(id, "text); select 1; --"))
  )
  doAssert not compiles(block:
    let dynamicType = "text"
    discard query:
      select records(sqlCast(id, dynamicType))
  )
  doAssert not compiles(block:
    discard query:
      select records(id)
      where id == any(note)
  )
  doAssert not compiles(block:
    discard query:
      select records(any(tags))
  )
  doAssert not compiles(block:
    discard query:
      select records(note ->> "key")
  )
  doAssert not compiles(block:
    discard query:
      select records(jsonPathText(payload, sqlCast("{}", "uuid[]")))
  )

when defined(postgre):
  let target = getEnv("TEST_DATABASE_URL", "postgresql://test@localhost/test_ormin")
  let db = pg.open("", "", "", target)
  try:
    checkExpressions(db)
    echo "PostgreSQL expression tests passed"
  finally:
    pg.close(db)
