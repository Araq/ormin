
import strutils, postgres

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
  var pparams: array[n, string]

template bindParam*(db: DbConn; s: PStmt; idx: int; x: untyped) =
  pparams[idx-1] = $x

template bindResult*(db: DbConn; s: PStmt; idx: int; dest: int) =
  dest = c_strtol(pqgetvalue(queryResult, queryI, idx.cint))

template bindResult*(db: DbConn; s: PStmt; idx: int; dest: int64) =
  dest = c_strtol(pqgetvalue(queryResult, queryI, idx.cint))

proc fillString(dest: var string; src: cstring; srcLen: int) =
  if dest.isNil: dest = newString(srcLen)
  else: setLen(dest, srcLen)
  copyMem(unsafeAddr(dest[0]), src, srcLen)
  dest[srcLen] = '\0'

template bindResult*(db: DbConn; s: PStmt; idx: int; dest: var string) =
  let src = pqgetvalue(queryResult, queryI, idx.cint)
  let srcLen = int(pqgetlength(queryResult, queryI, idx.cint))
  fillString(dest, src, srcLen)

template bindResult*(db: DbConn; s: PStmt; idx: int; dest: float64) =
  dest = c_strtod(pqgetvalue(queryResult, queryI, idx.cint))

template startQuery*(db: DbConn; s: PStmt) =
  when declared(pparams):
    var arr: array[pparams.len, cstring]
    for i in 0..high(arr): arr[i] = cstring(pparams[i])
    var queryResult {.inject.} = pqexecPrepared(db, s, int32(arr.len),
            cast[cstringArray](addr arr), nil, nil, 0)
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
