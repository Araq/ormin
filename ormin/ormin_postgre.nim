import strutils, db_connector/postgres, json, times

import db_connector/db_common
import query_hooks
export db_common

type
  DbConn* = PPGconn    ## encapsulates a database connection
  PStmt = string ## SQL text passed to the extended query protocol

  ManagedPlan = object
    sql, name: string

  ManagedDbConn* = object
    ## Owns one libpq session and at most `maxPlans` named plans in that session.
    raw: DbConn
    plans: seq[ManagedPlan]
    maxPlans, nextPlanId: int
    backendPid: int32
    cacheDisabled: bool

  varcharType* = string
  intType* = int
  floatType* = float
  boolType* = bool
  timestampType* = Datetime
  serialType* = int
  jsonType* = JsonNode

proc `=destroy`(db: ManagedDbConn) =
  if db.raw != nil:
    pqfinish(db.raw)
  `=destroy`(db.plans)

proc `=wasMoved`(db: var ManagedDbConn) =
  db.raw = nil
  wasMoved(db.plans)

proc `=copy`(dest: var ManagedDbConn; source: ManagedDbConn) {.error.}
proc `=dup`(source: ManagedDbConn): ManagedDbConn {.error.}

const DEFAULT_MANAGED_PLAN_LIMIT* = 64

var jsonTimeFormat* = "yyyy-MM-dd HH:mm:ss"

proc dbError*(db: DbConn) {.noreturn.} =
  ## raises a DbError exception.
  var e: ref DbError
  new(e)
  e.msg = $pqErrorMessage(db)
  raise e

proc dbError*(db: var ManagedDbConn) {.noreturn.} =
  ## Raises the same PostgreSQL error as the raw connection, or a clear
  ## error when the owned connection has already been closed.
  if db.raw == nil:
    raise newException(DbError, "Managed PostgreSQL connection is closed")
  dbError(db.raw)

proc c_strtod(buf: cstring, endptr: ptr cstring = nil): float64 {.
  importc: "strtod", header: "<stdlib.h>", noSideEffect.}

proc c_strtol(buf: cstring, endptr: ptr cstring = nil, base: cint = 10): int {.
  importc: "strtol", header: "<stdlib.h>", noSideEffect.}

proc prepareStmt*(db: DbConn; q: string): PStmt =
  when defined(debugOrminTrace):
    echo "[[Ormin Executing]]: ", q
  # Ormin used to create a new named server-side statement for every call.
  # Long-lived connections retained every plan, and the prepare result was
  # never cleared. Execute parameterized SQL directly instead.
  result = q

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

template bindNullParam*(db: DbConn; s: PStmt; idx: int) =
  parr[idx-1] = cstring(nil)

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
  dest = ""
  var i = 0
  while src[i] != '\0':
    dest.add src[i]
    inc i
    
template bindResult*(db: DbConn; s: PStmt; idx: int; dest: var string;
                     t: typedesc; name: string) =
  if pqgetisnull(queryResult, queryI, idx.cint) != 0:
    when defined(nimNoNilSeqs):
      setLen(dest, 0)
    else:
      dest = nil
  else:
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

template bindResult*[T](db: DbConn; s: PStmt; idx: int; dest: var DbValue[T];
                        t: typedesc; name: string) =
  if pqgetisnull(queryResult, queryI, idx.cint) != 0:
    dest.isNull = true
  else:
    dest.isNull = false
    when T is string:
      let src = pqgetvalue(queryResult, queryI, idx.cint)
      let srcLen = int(pqgetlength(queryResult, queryI, idx.cint))
      fillString(dest.value, src, srcLen)
    else:
      bindResult(db, s, idx, dest.value, t, name)

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

template columnIsNull*(db: DbConn; s: PStmt; idx: int): bool =
  pqgetisnull(queryResult, queryI, idx.cint) != 0

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

proc checkQueryResult(db: DbConn; res: PPGresult) =
  if res != nil and pqResultStatus(res) in {PGRES_COMMAND_OK, PGRES_TUPLES_OK}:
    return
  # Copy the result's error before releasing it. Failed PQexecParams calls
  # still return an allocated PGresult that the caller will never stopQuery.
  let detail =
    if res == nil: $pqErrorMessage(db)
    else: $pqResultErrorMessage(res)
  let message = if detail.len > 0: detail else: $pqErrorMessage(db)
  if res != nil:
    pqclear(res)
  raise newException(DbError, message)

template startQuery*(db: DbConn; s: PStmt) =
  when declared(pparams):
    var queryResult {.inject.} = pqexecParams(db, s.cstring, int32(parr.len),
            nil, cast[cstringArray](addr parr), nil, nil, 0)
  else:
    var queryResult {.inject.} = pqexecParams(db, s.cstring, int32(0),
            nil, nil, nil, nil, 0)
  checkQueryResult(db, queryResult)
  var queryI {.inject.} = cint(-1)
  var queryLen {.inject.} = pqntuples(queryResult)

template stopQuery*(db: DbConn; s: PStmt) =
  pqclear(queryResult)

template stepQuery*(db: DbConn; s: PStmt; returnsData: bool): bool =
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

proc openManaged*(conninfo: string;
                  maxPlans = DEFAULT_MANAGED_PLAN_LIMIT): ManagedDbConn =
  ## Opens an owned session. Use a direct PostgreSQL or session-pooled connection;
  ## transaction pooling cannot promise that named plans stay on one backend.
  if maxPlans < 0:
    raise newException(ValueError, "maxPlans must be nonnegative")
  result.raw = pqconnectdb(conninfo.cstring)
  if result.raw == nil:
    raise newException(DbError, "Could not allocate a PostgreSQL connection")
  if pqStatus(result.raw) != CONNECTION_OK:
    let message = $pqErrorMessage(result.raw)
    pqfinish(result.raw)
    result.raw = nil
    raise newException(DbError, message)
  result.maxPlans = maxPlans
  result.backendPid = pqbackendPID(result.raw)

proc borrowRaw*(db: var ManagedDbConn): DbConn =
  ## Borrows the libpq handle for APIs that require it. Do not finish or reset
  ## this handle; use `close` or `reset` on the owner instead.
  db.raw

proc managedPlanCount*(db: var ManagedDbConn): int = db.plans.len

proc close*(db: var ManagedDbConn) =
  ## Closes the session and discards its cache. Repeated close is harmless.
  `=destroy`(db)
  `=wasMoved`(db)

proc reset*(db: var ManagedDbConn) =
  ## Reconnects the same libpq handle and invalidates every cached plan.
  if db.raw == nil:
    raise newException(DbError, "Managed PostgreSQL connection is closed")
  db.plans.setLen(0)
  db.nextPlanId = 0
  db.cacheDisabled = false
  pqreset(db.raw)
  if pqStatus(db.raw) != CONNECTION_OK:
    dbError(db.raw)
  db.backendPid = pqbackendPID(db.raw)

proc planName(db: var ManagedDbConn; sql: string): string =
  for plan in db.plans:
    if plan.sql == sql:
      return plan.name

proc prepareStmt*(db: var ManagedDbConn; q: string): PStmt =
  ## Prepares a query once per owned session, up to its plan limit. Excess
  ## distinct queries use PQexecParams without retaining another server plan.
  if db.raw == nil:
    raise newException(DbError, "Managed PostgreSQL connection is closed")
  if pqStatus(db.raw) != CONNECTION_OK:
    dbError(db.raw)
  let pid = pqbackendPID(db.raw)
  if pid != db.backendPid:
    db.plans.setLen(0)
    db.nextPlanId = 0
    db.backendPid = pid
  when defined(debugOrminTrace):
    echo "[[Ormin Executing]]: ", q
  result = q
  if db.cacheDisabled or db.maxPlans == 0:
    return
  if db.planName(q).len > 0:
    return
  if db.plans.len >= db.maxPlans:
    return
  let name = "ormin_managed_" & $db.nextPlanId
  let res = pqprepare(db.raw, name.cstring, q.cstring, 0, nil)
  if res == nil:
    dbError(db.raw)
  let status = pqResultStatus(res)
  if status != PGRES_COMMAND_OK:
    let detail = $pqResultErrorMessage(res)
    let message = if detail.len > 0: detail else: $pqErrorMessage(db.raw)
    pqclear(res)
    raise newException(DbError, message)
  pqclear(res)
  inc db.nextPlanId
  db.plans.add ManagedPlan(sql: q, name: name)

proc executeManaged(db: var ManagedDbConn; sql: string;
                    params: cstringArray; count: int32): PPGresult =
  if db.raw == nil:
    raise newException(DbError, "Managed PostgreSQL connection is closed")
  let name = db.planName(sql)
  if name.len > 0 and not db.cacheDisabled:
    result = pqexecPrepared(db.raw, name.cstring, count, params, nil, nil, 0)
  else:
    result = pqexecParams(db.raw, sql.cstring, count, nil, params, nil, nil, 0)
  if result == nil:
    dbError(db.raw)
  if pqResultStatus(result) in {PGRES_COMMAND_OK, PGRES_TUPLES_OK}:
    return
  if name.len > 0 and not db.cacheDisabled:
    let sqlState = $pqResultErrorField(result, int32(ord('C')))
    if sqlState in ["26000", "0A000"]:
      # The server lost this plan or changed its result shape. Never replay an
      # uncertain operation, and use direct execution for this session now.
      db.cacheDisabled = true
      db.plans.setLen(0)
  let detail = $pqResultErrorMessage(result)
  let message = if detail.len > 0: detail else: $pqErrorMessage(db.raw)
  pqclear(result)
  result = nil
  raise newException(DbError, message)

template startQuery*(db: ManagedDbConn; s: PStmt) =
  when declared(pparams):
    var queryResult {.inject.} = executeManaged(db, s,
      cast[cstringArray](addr parr), int32(parr.len))
  else:
    var queryResult {.inject.} = executeManaged(db, s, nil, 0)
  # Generated row conversion can raise after a successful execution. Release
  # that result even when the normal stopQuery path is skipped.
  defer:
    if queryResult != nil:
      pqclear(queryResult)
  var queryI {.inject.} = cint(-1)
  var queryLen {.inject.} = pqntuples(queryResult)

template bindParam*(db: ManagedDbConn; s: PStmt; idx: int; x: untyped; t: untyped) =
  bindParam(db.raw, s, idx, x, t)

template bindNullParam*(db: ManagedDbConn; s: PStmt; idx: int) =
  bindNullParam(db.raw, s, idx)

template bindParamJson*(db: ManagedDbConn; s: PStmt; idx: int;
                        x: JsonNode; t: typedesc) =
  bindParamJson(db.raw, s, idx, x, t)

template bindResult*(db: ManagedDbConn; s: PStmt; idx: int;
                     dest: untyped; t: typedesc; name: string) =
  bindResult(db.raw, s, idx, dest, t, name)

template bindResultJson*(db: ManagedDbConn; s: PStmt; idx: int;
                         obj: JsonNode; t: typedesc; name: string) =
  bindResultJson(db.raw, s, idx, obj, t, name)

template columnIsNull*(db: ManagedDbConn; s: PStmt; idx: int): bool =
  columnIsNull(db.raw, s, idx)

template bindToJson*(db: ManagedDbConn; s: PStmt; idx: int;
                     obj: JsonNode; t: typedesc; name: string) =
  bindToJson(db.raw, s, idx, obj, t, name)

template stopQuery*(db: ManagedDbConn; s: PStmt) =
  pqclear(queryResult)
  queryResult = nil

template stepQuery*(db: ManagedDbConn; s: PStmt; returnsData: bool): bool =
  stepQuery(db.raw, s, returnsData)

template getLastId*(db: ManagedDbConn; s: PStmt): int =
  getLastId(db.raw, s)

template getAffectedRows*(db: ManagedDbConn; s: PStmt): int =
  getAffectedRows(db.raw, s)
