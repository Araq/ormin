import strutils, db_connector/postgres, json, times

import db_connector/db_common
export db_common

type
  DbConn* = PPGconn    ## encapsulates a database connection
  PStmt = string ## a identifier for the prepared queries

  varcharType* = string
  intType* = int
  floatType* = float
  boolType* = bool
  timestampType* = Datetime
  serialType* = int
  jsonType* = JsonNode

var jsonTimeFormat* = "yyyy-MM-dd HH:mm:ss"

proc dbError*(db: DbConn) {.noreturn.} =
  ## raises a DbError exception.
  var e: ref DbError
  new(e)
  e.msg = $pqErrorMessage(db)
  raise e

proc c_strtod(buf: cstring, endptr: ptr cstring = nil): float64 {.
  importc: "strtod", header: "<stdlib.h>", noSideEffect.}

proc c_strtol(buf: cstring, endptr: ptr cstring = nil, base: cint = 10): int {.
  importc: "strtol", header: "<stdlib.h>", noSideEffect.}

var sid {.compileTime.}: int

proc prepareStmt*(db: DbConn; q: string): PStmt =
  inc sid
  result = "ormin" & $sid
  var res = pqprepare(db, result, q, 0, nil)
  if pqResultStatus(res) != PGRES_COMMAND_OK: dbError(db)

template startBindings*(s: PStmt; n: int) {.dirty.} =
  # pparams is a duplicated array to keep the Nim string alive
  # for the duration of the query. This is safer than relying
  # on the conservative stack marking:
  var pparams: array[n, string]
  var parr: array[n, cstring]

template bindParam*(db: DbConn; s: PStmt; idx: int; x: untyped; t: untyped) =
  # when not (x is t):
  #   {.error: "type mismatch for query argument at position " & $idx.}
  when t is DateTime:
    let xx = x.format("yyyy-MM-dd HH:mm:ss\'.\'ffffffzzzz")
    pparams[idx-1] = $xx
  else:
    pparams[idx-1] = $x

  parr[idx-1] = cstring(pparams[idx-1])

template bindParamUnchecked(db: DbConn; s: PStmt; idx: int; x: untyped; t: untyped) =
  pparams[idx-1] = $x
  parr[idx-1] = cstring(pparams[idx-1])

template bindParamJson*(db: DbConn; s: PStmt; idx: int; xx: JsonNode;
                        t: typedesc) =
  let x = xx
  if x.kind == JNull:
    # a NULL entry is not reflected in the 'pparams' array:
    parr[idx-1] = cstring(nil)
  else:
    bindFromJson(db, s, idx, x, t)

template bindFromJson*(db: DbConn; s: PStmt; idx: int; x: JsonNode;
                       t: typedesc) =
  {.error: "invalid type for JSON object".}

template bindFromJson*(db: DbConn; s: PStmt; idx: int; x: JsonNode;
                       t: typedesc[string]) =
  doAssert x.kind == JString
  let xs = x.str
  bindParamUnchecked(db, s, idx, xs, t)

template bindFromJson*(db: DbConn; s: PStmt; idx: int; x: JsonNode;
                       t: typedesc[int|int64]) =
  doAssert x.kind == JInt
  let xi = x.num
  bindParamUnchecked(db, s, idx, xi, t)

template bindFromJson*(db: DbConn; s: PStmt; idx: int; x: JsonNode;
                       t: typedesc[float64]) =
  doAssert x.kind == JFloat
  let xf = x.fnum
  bindParamUnchecked(db, s, idx, xf, t)

template bindFromJson*(db: DbConn; s: PStmt; idx: int; x: JsonNode;
                       t: typedesc[bool]) =
  doAssert x.kind == JBool
  let xb = x.bval
  bindParamUnchecked(db, s, idx, xb, t)  

template bindFromJson*(db: DbConn; s: PStmt; idx: int; x: JsonNode;
                       t: typedesc[DateTime]) =
  doAssert x.kind == JString
  let dt = x.str
  bindParamUnchecked(db, s, idx, dt, t)

template bindResult*(db: DbConn; s: PStmt; idx: int; dest: int;
                     t: typedesc; name: string) =
  dest = c_strtol(pqgetvalue(queryResult, queryI, idx.cint))

template bindResult*(db: DbConn; s: PStmt; idx: int; dest: int64;
                     t: typedesc; name: string) =
  dest = c_strtol(pqgetvalue(queryResult, queryI, idx.cint))

# XXX This needs testing:
template isTrue(x): untyped = x[0] == 't'

template bindResult*(db: DbConn; s: PStmt; idx: int; dest: bool;
                     t: typedesc; name: string) =
  dest = isTrue(pqgetvalue(queryResult, queryI, idx.cint))

proc fillString(dest: var string; src: cstring; srcLen: int) {.inline.} =
  var i = 0
  while src[i] != '\0':
    dest.add src[i]
    inc i
    
template bindResult*(db: DbConn; s: PStmt; idx: int; dest: var string;
                     t: typedesc; name: string) =
  let src = pqgetvalue(queryResult, queryI, idx.cint)
  let srcLen = int(pqgetlength(queryResult, queryI, idx.cint))
  fillString(dest, src, srcLen)

template bindResult*(db: DbConn; s: PStmt; idx: int; dest: float64;
                     t: typedesc; name: string) =
  dest = c_strtod(pqgetvalue(queryResult, queryI, idx.cint))


template bindResult*(db: DbConn; s: PStmt; idx: int; dest: var DateTime;
                     t: typedesc; name: string) =
  let
    src = $pqgetvalue(queryResult, queryI, idx.cint)
    i = src.find('.')
    tzflag = src[src.len-3]
  if i < 0:
    if tzflag in {'+', '-'}:
      dest = parse(src, "yyyy-MM-dd HH:mm:sszz")
    else:
      dest = parse(src, "yyyy-MM-dd HH:mm:ss")
  else:
    if tzflag in {'+', '-'}:
      let itz = src.len - 3
      let dtstr = src[0..<itz] & '0'.repeat(10-src.len+i) & src[src.len-3 .. ^1]
      dest = parse(dtstr, "yyyy-MM-dd HH:mm:ss\'.\'ffffffzz")
    else:
      let dtstr = src & '0'.repeat(7-src.len+i)
      dest = parse(dtstr, "yyyy-MM-dd HH:mm:ss\'.\'ffffff")

template bindResult*(db: DbConn; s: PStmt; idx: int; dest: JsonNode;
                     t: typedesc; name: string) =
  dest = parseJson($pqgetvalue(queryResult, queryI, idx.cint))

template createJObject*(): untyped = newJObject()
template createJArray*(): untyped = newJArray()

template bindResultJson*(db: DbConn; s: PStmt; idx: int; obj: JsonNode;
                         t: typedesc; name: string) =
  let x = obj
  doAssert x.kind == JObject
  if pqgetisnull(queryResult, queryI, idx.cint) != 0:
    x[name] = newJNull()
  else:
    bindToJson(db, s, idx, x, t, name)

template bindToJson*(db: DbConn; s: PStmt; idx: int; obj: JsonNode;
                     t: typedesc; name: string) =
  {.error: "invalid type for JSON object".}    

template bindToJson*(db: DbConn; s: PStmt; idx: int; obj: JsonNode;
                     t: typedesc[string]; name: string) =
  let src = pqgetvalue(queryResult, queryI, idx.cint)
  obj[name] = newJString($src)  

template bindToJson*(db: DbConn; s: PStmt; idx: int; obj: JsonNode;
                     t: typedesc[int|int64]; name: string) =
  obj[name] = newJInt(c_strtol(pqgetvalue(queryResult, queryI, idx.cint)))

template bindToJson*(db: DbConn; s: PStmt; idx: int; obj: JsonNode;
                     t: typedesc[float64]; name: string) =
  obj[name] = newJFloat(c_strtod(pqgetvalue(queryResult, queryI, idx.cint)))

template bindToJson*(db: DbConn; s: PStmt; idx: int; obj: JsonNode;
                     t: typedesc[bool]; name: string) =
  obj[name] = newJBool(isTrue(pqgetvalue(queryResult, queryI, idx.cint)))

template bindToJson*(db: DbConn; s: PStmt; idx: int; obj: JsonNode;
                     t: typedesc[DateTime]; name: string) =
  var dt: DateTime
  bindResult(db, s, idx, dt, t, name)
  obj[name] = newJString(format(dt, jsonTimeFormat))
  
template bindToJson*(db: DbConn; s: PStmt; idx: int; obj: JsonNode;
                     t: typedesc[JsonNode]; name: string) =
  let src = pqgetvalue(queryResult, queryI, idx.cint)
  obj[name] = parseJson($src)  

template startQuery*(db: DbConn; s: PStmt) =
  when declared(pparams):
    var queryResult {.inject.} = pqexecPrepared(db, s, int32(parr.len),
            cast[cstringArray](addr parr), nil, nil, 0)
  else:
    var queryResult {.inject.} = pqexecPrepared(db, s, int32(0),
            nil, nil, nil, 0)
  if pqResultStatus(queryResult) == PGRES_COMMAND_OK:
    discard # insert does not returns data in pg
  elif pqResultStatus(queryResult) != PGRES_TUPLES_OK: dbError(db)
  var queryI {.inject.} = cint(-1)
  var queryLen {.inject.} = pqntuples(queryResult)

template stopQuery*(db: DbConn; s: PStmt) =
  pqclear(queryResult)

template stepQuery*(db: DbConn; s: PStmt; returnsData: int): bool =
  inc queryI
  queryI < queryLen

template getLastId*(db: DbConn; s: PStmt): int = 0 # XXX to implement

template getAffectedRows*(db: DbConn; s: PStmt): int =
  c_strtol(pqcmdTuples(queryResult))

proc close*(db: DbConn) =
  ## closes the database connection.
  if db != nil: pqfinish(db)

proc open*(connection, user, password, database: string): DbConn =
  ## opens a database connection. Raises `DbError` if the connection could not
  ## be established.
  let
    colonPos = connection.find(':')
    host = if colonPos < 0: connection
           else: substr(connection, 0, colonPos-1)
    port = if colonPos < 0: ""
           else: substr(connection, colonPos+1)
  result = pqsetdbLogin(host, port, nil, nil, database, user, password)
  if pqStatus(result) != CONNECTION_OK: dbError(result) # result = nil
