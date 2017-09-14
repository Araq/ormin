
import strutils, postgres, json

import db_common
export db_common

type
  DbConn* = PPGconn    ## encapsulates a database connection
  PStmt = cstring ## a identifier for the prepared queries

  varchar* = string
  integer* = int
  timestamp* = string

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
  static:
    inc sid
    const name = "ormin" & $sid
  result = cstring(name)
  var res = pqprepare(db, result, q, 0, nil)
  if pqResultStatus(res) != PGRES_COMMAND_OK: dbError(db)

template startBindings*(n: int) {.dirty.} =
  # pparams is a duplicated array to keep the Nim string alive
  # for the duration of the query. This is safer than relying
  # on the conservative stack marking:
  var pparams: array[n, string]
  var parr: array[n, cstring]

template bindParam*(db: DbConn; s: PStmt; idx: int; x: untyped; t: untyped) =
  pparams[idx-1] = $x
  parr[idx-1] = cstring(pparams[idx-1])

template bindParamJson*(db: DbConn; s: PStmt; idx: int; xx: JsonNode;
                        t: typedesc) =
  let x = xx
  if x.kind == JNull:
    # a NULL entry is not reflected in the 'pparams' array:
    parr[idx-1] = cstring(nil)
  else:
    when t is string:
      doAssert x.kind == JString
      let xs = x.str
      bindParam(db, s, idx, xs, t)
    elif (t is int) or (t is int64):
      doAssert x.kind == JInt
      let xi = x.num
      bindParam(db, s, idx, xi, t)
    elif t is float64:
      doAssert x.kind == JFloat
      let xf = x.fnum
      bindParam(db, s, idx, xf, t)
    else:
      {.error: "invalid type for JSON object".}

template bindResult*(db: DbConn; s: PStmt; idx: int; dest: int;
                     t: typedesc; name: string) =
  dest = c_strtol(pqgetvalue(queryResult, queryI, idx.cint))

template bindResult*(db: DbConn; s: PStmt; idx: int; dest: int64;
                     t: typedesc; name: string) =
  dest = c_strtol(pqgetvalue(queryResult, queryI, idx.cint))

proc fillString(dest: var string; src: cstring; srcLen: int) =
  if dest.isNil: dest = newString(srcLen)
  else: setLen(dest, srcLen)
  copyMem(unsafeAddr(dest[0]), src, srcLen)
  dest[srcLen] = '\0'

template bindResult*(db: DbConn; s: PStmt; idx: int; dest: var string;
                     t: typedesc; name: string) =
  let src = pqgetvalue(queryResult, queryI, idx.cint)
  let srcLen = int(pqgetlength(queryResult, queryI, idx.cint))
  fillString(dest, src, srcLen)

template bindResult*(db: DbConn; s: PStmt; idx: int; dest: float64;
                     t: typedesc; name: string) =
  dest = c_strtod(pqgetvalue(queryResult, queryI, idx.cint))

template createJObject*(): untyped = newJObject()
template createJArray*(): untyped = newJArray()

template bindResultJson*(db: DbConn; s: PStmt; idx: int; obj: JsonNode;
                         t: typedesc; name: string) =
  let x = obj
  doAssert x.kind == JObject
  if pqgetisnull(queryResult, queryI, idx.cint) != 0:
    x[name] = newJNull()
  else:
    when t is string:
      let dest = newJString(nil)
      let src = pqgetvalue(queryResult, queryI, idx.cint)
      let srcLen = int(pqgetlength(queryResult, queryI, idx.cint))
      fillString(dest.src, src, srcLen)
      x[name] = dest
    elif (t is int) or (t is int64):
      x[name] = newJInt(c_strtol(pqgetvalue(queryResult, queryI, idx.cint)))
    elif t is float64:
      x[name] = newJFloat(c_strtod(pqgetvalue(queryResult, queryI, idx.cint)))
    else:
      {.error: "invalid type for JSON object".}

template startQuery*(db: DbConn; s: PStmt) =
  when declared(pparams):
    var queryResult {.inject.} = pqexecPrepared(db, s, int32(parr.len),
            cast[cstringArray](addr parr), nil, nil, 0)
  else:
    var queryResult {.inject.} = pqexecPrepared(db, s, int32(0),
            nil, nil, nil, 0)
  if pqResultStatus(queryResult) != PGRES_TUPLES_OK: dbError(db)
  var queryI {.inject.} = cint(-1)
  var queryLen {.inject.} = pqntuples(queryResult)

template stopQuery*(db: DbConn; s: PStmt) =
  pqclear(queryResult)

template stepQuery*(db: DbConn; s: PStmt): bool =
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
  ##
  ## Clients can also use Postgres keyword/value connection strings to
  ## connect.
  ##
  ## Example:
  ##
  ## .. code-block:: nim
  ##
  ##      con = open("", "", "", "host=localhost port=5432 dbname=mydb")
  ##
  ## See http://www.postgresql.org/docs/current/static/libpq-connect.html#LIBPQ-CONNSTRING
  ## for more information.
  ##
  ## Note that the connection parameter is not used but exists to maintain
  ## the nim db api.
  result = pqsetdbLogin(nil, nil, nil, nil, database, user, password)
  if pqStatus(result) != CONNECTION_OK: dbError(result) # result = nil
