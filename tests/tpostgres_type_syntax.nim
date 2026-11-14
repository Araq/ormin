import std/[assertions, strutils]

import ormin/[db_types, importer_core, parsesql_tmp]
from db_connector/db_common import DbTypeKind

block postgres_type_aliases:
  doAssert dbTypFromName("float4") == dbFloat
  doAssert dbTypFromName("float8") == dbFloat
  doAssert dbTypFromName("DOUBLE PRECISION") == dbFloat
  doAssert dbTypFromName("numeric") == dbDecimal

block schema_types_round_trip:
  let schema = """
    create table measurements (
      id bigint primary key,
      value double precision not null,
      labels text[],
      ids uuid[8],
      matrix numeric(18, 4)[][],
      states "public"."status"[]
    );
  """
  let tree = parseSql(schema, considerTypeParams = true)
  let rendered = renderSql(tree)
  doAssert "double precision" in rendered
  doAssert "text[]" in rendered
  doAssert "uuid[8]" in rendered
  doAssert "numeric(18, 4)[][]" in rendered or "numeric(18,4)[][]" in rendered
  doAssert "\"public\".\"status\"[]" in rendered
  doAssert renderSql(parseSql(rendered, considerTypeParams = true)) == rendered

  let model = generateModelCode(schema, "measurements.sql", ImportTarget.postgre)
  doAssert "typ: dbFloat, typeName: \"double precision\"" in model
  doAssert "typ: dbArray, typeName: \"text[]\"" in model
  doAssert "typ: dbArray, typeName: \"uuid[]\"" in model
  doAssert "typ: dbArray, typeName: \"numeric[][]\"" in model
  doAssert "typ: dbArray, typeName: \"public.status[]\"" in model

block malformed_array_types:
  doAssertRaises(SqlParseError):
    discard parseSql("create table records (tags text[);")
  doAssertRaises(SqlParseError):
    discard parseSql("create table records (tags text[invalid]);")
