
{.deadCodeElim: on.}

import json, times

import db_connector/db_common
import db_connector/sqlite3
import query_hooks
export db_common

type
  DbConn* = PSqlite3  ## encapsulates a database connection

  ManagedPlan = ref object
    sql: string
    statement: PStmt
    inUse: bool

  ManagedSession = ref object
    raw: DbConn
    plans: seq[ManagedPlan]
    maxPlans, activeStatements: int

  ManagedDbConn* = object
    ## Owns a SQLite connection and a bounded cache of prepared statements.
    session: ManagedSession

  ManagedStmt* = object
    ## Owns one use of a statement. Scope exit resets a cached statement or
    ## finalizes a temporary one, including on exceptions and iterator exits.
    session: ManagedSession
    plan: ManagedPlan
    cached: bool

  varcharType* = string
  blobType* = seq[byte]
  intType* = int
  floatType* = float
  boolType* = bool
  timestampType* = DateTime
  jsonType* = JsonNode

# db_connector does not yet expose sqlite3_close_v2. Match its library selection,
# including staticSqlite, so outstanding borrowed handles can finish cleanup.
when defined(staticSqlite):
  proc closeV2(db: DbConn): cint {.cdecl, importc: "sqlite3_close_v2".}
else:
  when defined(windows):
    when defined(nimOldDlls):
      const SqliteLibrary = "sqlite3.dll"
    elif defined(cpu64):
      const SqliteLibrary = "sqlite3_64.dll"
    else:
      const SqliteLibrary = "sqlite3_32.dll"
  elif defined(macosx):
    const SqliteLibrary = "libsqlite3(|.0).dylib"
  else:
    const SqliteLibrary = "libsqlite3.so(|.0)"
  proc closeV2(db: DbConn): cint {.
    cdecl, importc: "sqlite3_close_v2", dynlib: SqliteLibrary.}

proc clearPlans(session: ManagedSession) {.raises: [].} =
  for plan in session.plans:
    discard sqlite3.finalize(plan.statement)
    plan.statement = nil
  session.plans.setLen(0)

proc closeSession(session: ManagedSession) {.raises: [].} =
  if session != nil and session.raw != nil:
    clearPlans(session)
    discard closeV2(session.raw)
    session.raw = nil

proc `=destroy`*(db: ManagedDbConn) =
  closeSession(db.session)
  `=destroy`(db.session)

proc `=wasMoved`*(db: var ManagedDbConn) =
  wasMoved(db.session)

proc `=copy`*(dest: var ManagedDbConn; source: ManagedDbConn) {.error.}
proc `=dup`*(source: ManagedDbConn): ManagedDbConn {.error.}

proc `=destroy`*(s: ManagedStmt) =
  if s.plan != nil:
    if s.plan.statement != nil:
      if s.cached:
        discard sqlite3.reset(s.plan.statement)
        discard clear_bindings(s.plan.statement)
        s.plan.inUse = false
      else:
        discard sqlite3.finalize(s.plan.statement)
        s.plan.statement = nil
    dec s.session.activeStatements
  `=destroy`(s.plan)
  `=destroy`(s.session)

proc `=wasMoved`*(s: var ManagedStmt) =
  wasMoved(s.plan)
  wasMoved(s.session)

proc `=copy`*(dest: var ManagedStmt; source: ManagedStmt) {.error.}
proc `=dup`*(source: ManagedStmt): ManagedStmt {.error.}

const DefaultManagedPlanLimit* = 64

var jsonTimeFormat* = "yyyy-MM-dd HH:mm:ss"

proc dbError*(db: DbConn) {.noreturn.} =
  ## raises a DbError exception.
  var e: ref DbError
  new(e)
  e.msg = $sqlite3.errmsg(db)
  raise e

proc prepareStmt*(db: DbConn; q: string): PStmt =
  when defined(debugOrminTrace):
    echo "[[Ormin Executing]]: ", q
  if prepare_v2(db, q, q.len.cint, result, nil) != SQLITE_OK:
    dbError(db)

proc finalizeStmt*(s: PStmt) {.raises: [].} =
  ## Release a query's statement, including when bindings or result conversion raise.
  ## stopQuery reports execution errors before this nonraising scope cleanup runs.
  discard sqlite3.finalize(s)

template startBindings*(s: PStmt; n: int) =
  if clear_bindings(s) != SQLITE_OK: dbError(db)

template bindParam*(db: DbConn; s: PStmt; idx: int; x, t: untyped) =
  #when not (x is t):
  #  {.error: "type mismatch for query argument at position " & $idx.}
  when t is blobType:
    let xs = x
    let blobPtr = if xs.len == 0:
        cast[pointer](nil)
      else:
        cast[pointer](unsafeAddr(xs[0]))
    if bind_blob(s, idx.cint, blobPtr, xs.len.cint, SQLITE_TRANSIENT) != SQLITE_OK:
      dbError(db)
  elif t is int or t is int64 or t is bool:
    if bind_int64(s, idx.cint, x.int64) != SQLITE_OK: dbError(db)
  elif t is string:
    if bind_text(s, idx.cint, cstring(x), x.len.cint, SQLITE_TRANSIENT) != SQLITE_OK:
      dbError(db)
  elif t is float64:
    if bind_double(s, idx.cint, x) != SQLITE_OK:
      dbError(db)
  elif t is DateTime:
    let xx = if x.nanosecond > 0:
      x.utc().format("yyyy-MM-dd HH:mm:ss\'.\'fff")
    else:
      x.utc().format("yyyy-MM-dd HH:mm:ss")

    if bind_text(s, idx.cint, cstring(xx), xx.len.cint, SQLITE_TRANSIENT) != SQLITE_OK:
      dbError(db)
  elif t is JsonNode:
    let xx = $x
    if bind_text(s, idx.cint, cstring(xx), xx.len.cint, SQLITE_TRANSIENT) != SQLITE_OK:
      dbError(db)
  else:
    {.error: "type mismatch for query argument at position " & $idx.}

template bindNullParam*(db: DbConn; s: PStmt; idx: int) =
  if bind_null(s, idx.cint) != SQLITE_OK:
    dbError(db)

template bindParamJson*(db: DbConn; s: PStmt; idx: int; xx: JsonNode;
                        t: typedesc) =
  let x = xx
  if x.kind == JNull:
    if bind_null(s, idx.cint) != SQLITE_OK: dbError(db)
  else:
    bindFromJson(db, s, idx, x, t)

template bindFromJson*(db: DbConn; s: PStmt; idx: int; x: JsonNode;
                       t: typedesc) =
  {.error: "invalid type for JSON object".}

template bindFromJson*(db: DbConn; s: PStmt; idx: int; x: JsonNode;
                       t: typedesc[string]) =
  doAssert x.kind == JString
  let xs = x.str
  if bind_text(s, idx.cint, cstring(xs), xs.len.cint, SQLITE_TRANSIENT) != SQLITE_OK:
    dbError(db)

template bindFromJson*(db: DbConn; s: PStmt; idx: int; x: JsonNode;
                       t: typedesc[int|int64]) =
  doAssert x.kind == JInt
  let xi = x.num
  if bind_int64(s, idx.cint, xi.int64) != SQLITE_OK: dbError(db)

template bindFromJson*(db: DbConn; s: PStmt; idx: int; x: JsonNode;
                       t: typedesc[float64]) =
  doAssert x.kind == JFloat
  let xf = x.fnum
  if bind_double(s, idx.cint, xf) != SQLITE_OK:
    dbError(db)

template bindFromJson*(db: DbConn; s: PStmt; idx: int; x: JsonNode;
                       t: typedesc[bool]) =
  doAssert x.kind == JBool
  let xb = x.bval
  if bind_int64(s, idx.cint, xb.int64) != SQLITE_OK: dbError(db)

template bindFromJson*(db: DbConn; s: PStmt; idx: int; x: JsonNode;
                       t: typedesc[DateTime]) =
  doAssert x.kind == JString
  let
    dtStr = x.str
    i = dtStr.find('.')
    dt = if i > 0 and dtStr[i+1 .. ^1] == "000":
      dtStr[0 ..< i]
    else:
      dtStr
  if bind_text(s, idx.cint, cstring(dt), dt.len.cint, SQLITE_TRANSIENT) != SQLITE_OK:
    dbError(db)

template bindResult*(db: DbConn; s: PStmt; idx: int; dest: int;
                     t: typedesc; name: string) =
  dest = int column_int64(s, idx.cint)

template bindResult*(db: DbConn; s: PStmt; idx: int; dest: int64;
                     t: typedesc; name: string) =
  dest = column_int64(s, idx.cint)

proc fillString(dest: var string; src: cstring; srcLen: int) =
  when defined(nimNoNilSeqs):
    setLen(dest, srcLen)
  else:
    if dest.isNil: dest = newString(srcLen)
    else: setLen(dest, srcLen)
  if srcLen > 0: 
    copyMem(unsafeAddr(dest[0]), src, srcLen)

proc fillBytes(dest: var seq[byte]; src: pointer; srcLen: int) =
  when defined(nimNoNilSeqs):
    setLen(dest, srcLen)
  else:
    if dest.isNil: dest = newSeq[byte](srcLen)
    else: setLen(dest, srcLen)
  if srcLen > 0:
    copyMem(unsafeAddr(dest[0]), src, srcLen)

template bindResult*(db: DbConn; s: PStmt; idx: int; dest: var string;
                     t: typedesc; name: string) =
  if column_type(s, idx.cint) == SQLITE_NULL:
    when defined(nimNoNilSeqs):
      setLen(dest, 0)
    else:
      dest = nil
  else:
    let srcLen = column_bytes(s, idx.cint)
    let src = column_text(s, idx.cint)
    fillString(dest, src, srcLen)

template bindResult*(db: DbConn; s: PStmt; idx: int; dest: var blobType;
                     t: typedesc; name: string) =
  let srcLen = column_bytes(s, idx.cint)
  let src = column_blob(s, idx.cint)
  fillBytes(dest, src, srcLen)

template bindResult*(db: DbConn; s: PStmt; idx: int; dest: float64;
                     t: typedesc; name: string) =
  dest = column_double(s, idx.cint)

template bindResult*(db: DbConn; s: PStmt; idx: int; dest: bool;
                     t: typedesc; name: string) =
  dest = column_int64(s, idx.cint) != 0

template bindResult*(db: DbConn; s: PStmt; idx: int; dest: DateTime;
                     t: typedesc; name: string) =
  let
    src = $column_text(s, idx.cint)
    i = src.find('.')
  if i < 0:
    dest = parse(src, "yyyy-MM-dd HH:mm:ss", utc())
  else:
    dest = parse(src, "yyyy-MM-dd HH:mm:ss\'.\'fff", utc())

template bindResult*(db: DbConn; s: PStmt; idx: int; dest: JsonNode;
                     t: typedesc; name: string) =
  let src = column_text(s, idx.cint)
  dest = parseJson($src)

template bindResult*[T](db: DbConn; s: PStmt; idx: int; dest: var DbValue[T];
                        t: typedesc; name: string) =
  if column_type(s, idx.cint) == SQLITE_NULL:
    dest.isNull = true
  else:
    dest.isNull = false
    when T is string:
      let srcLen = column_bytes(s, idx.cint)
      let src = column_text(s, idx.cint)
      fillString(dest.value, src, srcLen)
    else:
      bindResult(db, s, idx, dest.value, t, name)

template createJObject*(): untyped = newJObject()
template createJArray*(): untyped = newJArray()

template bindResultJson*(db: DbConn; s: PStmt; idx: int; obj: JsonNode;
                         t: typedesc; name: string) =
  let x = obj
  doAssert x.kind == JObject
  if column_type(s, idx.cint) == SQLITE_NULL:
    x[name] = newJNull()
  else:
    bindToJson(db, s, idx, x, t, name)

template columnIsNull*(db: DbConn; s: PStmt; idx: int): bool =
  column_type(s, idx.cint) == SQLITE_NULL

template bindToJson*(db: DbConn; s: PStmt; idx: int; obj: JsonNode;
                     t: typedesc; name: string) =
  {.error: "invalid type for JSON object".}

template bindToJson*(db: DbConn; s: PStmt; idx: int; obj: JsonNode;
                     t: typedesc[string]; name: string) =
  let dest = newJString("")
  let srcLen = column_bytes(s, idx.cint)
  let src = column_text(s, idx.cint)
  fillString(dest.str, src, srcLen)
  obj[name] = dest

template bindToJson*(db: DbConn; s: PStmt; idx: int; obj: JsonNode;
                     t: typedesc[int|int64]; name: string) =
  obj[name] = newJInt(column_int64(s, idx.cint))

template bindToJson*(db: DbConn; s: PStmt; idx: int; obj: JsonNode;
                     t: typedesc[blobType]; name: string) =
  let srcLen = column_bytes(s, idx.cint)
  let src = column_blob(s, idx.cint)
  var data: blobType
  fillBytes(data, src, srcLen)
  let arr = newJArray()
  for b in data:
    arr.add newJInt(int(b))
  obj[name] = arr

template bindToJson*(db: DbConn; s: PStmt; idx: int; obj: JsonNode;
                     t: typedesc[float64]; name: string) =
  obj[name] = newJFloat(column_double(s, idx.cint))

template bindToJson*(db: DbConn; s: PStmt; idx: int; obj: JsonNode;
                     t: typedesc[bool]; name: string) =
  obj[name] = newJBool(column_int64(s, idx.cint) != 0)

template bindToJson*(db: DbConn; s: PStmt; idx: int; obj: JsonNode;
                     t: typedesc[DateTime]; name: string) =
  var dt: DateTime
  bindResult(db, s, idx, dt, t, name)
  obj[name] = newJString(format(dt, jsonTimeFormat))

template bindToJson*(db: DbConn; s: PStmt; idx: int; obj: JsonNode;
                     t: typedesc[JsonNode]; name: string) =
  let src = column_text(s, idx.cint)
  obj[name] = parseJson($src)

template startQuery*(db: DbConn; s: PStmt) = discard "nothing to do"

template stopQuery*(db: DbConn; s: PStmt) =
  if sqlite3.reset(s) != SQLITE_OK: dbError(db)

template stepQuery*(db: DbConn; s: PStmt; returnsData: bool): bool =
  when returnsData:
    step(s) == SQLITE_ROW
  else:
    step(s) == SQLITE_DONE

template getLastId*(db: DbConn; s: PStmt): int =
  int(last_insert_rowid(db))

template getAffectedRows*(db: DbConn; s: PStmt): int =
  int(changes(db))

proc close*(db: DbConn) =
  ## closes the database connection.
  if sqlite3.close(db) != SQLITE_OK: dbError(db)

proc open*(connection, user, password, database: string): DbConn =
  ## opens a database connection. Raises `EDb` if the connection could not
  ## be established. Only the ``connection`` parameter is used for ``sqlite``.
  if sqlite3.open(connection, result) != SQLITE_OK:
    dbError(result)

proc requireOpen(db: ManagedDbConn): DbConn =
  if db.session == nil or db.session.raw == nil:
    raise newException(DbError, "Managed SQLite connection is closed")
  db.session.raw

proc dbError*(db: ManagedDbConn) {.noreturn.} =
  dbError(requireOpen(db))

proc openManaged*(connection: string; maxPlans = DefaultManagedPlanLimit): ManagedDbConn =
  ## Opens an owned SQLite connection. Retain at most `maxPlans` statements;
  ## zero disables caching. Use the connection from one thread at a time.
  if maxPlans < 0:
    raise newException(ValueError, "maxPlans must be nonnegative")
  result.session = ManagedSession(maxPlans: maxPlans)
  if sqlite3.open(connection, result.session.raw) != SQLITE_OK:
    let message = if result.session.raw == nil:
        "Could not allocate a SQLite connection"
      else:
        $sqlite3.errmsg(result.session.raw)
    closeSession(result.session)
    raise newException(DbError, message)

proc borrowRaw*(db: ManagedDbConn): DbConn =
  ## Borrows the SQLite handle. Do not close it or finalize Ormin's statements.
  ## Finalize any statements you create through this handle yourself.
  if db.session != nil: db.session.raw else: nil

proc managedPlanCount*(db: ManagedDbConn): int =
  if db.session != nil: db.session.plans.len else: 0

proc close*(db: var ManagedDbConn) =
  ## Closes the connection and invalidates outstanding managed statements.
  ## Repeated close is harmless. Borrowed handles must still be cleaned up.
  `=destroy`(db)
  `=wasMoved`(db)

proc reset*(db: var ManagedDbConn) =
  ## Clears the statement cache without reopening the database, preserving
  ## in-memory data and transaction state. Rejects active queries or iterators.
  discard requireOpen(db)
  if db.session.activeStatements != 0:
    raise newException(DbError, "Cannot reset SQLite cache while statements are active")
  clearPlans(db.session)

proc prepareStmt*(db: var ManagedDbConn; q: string): ManagedStmt =
  ## Acquires an idle cached statement, or prepares a temporary one when the
  ## cache is full or the same SQL is already in use by an outer query.
  let raw = requireOpen(db)
  var cacheable = db.session.plans.len < db.session.maxPlans
  for plan in db.session.plans:
    if plan.sql == q:
      if not plan.inUse:
        plan.inUse = true
        inc db.session.activeStatements
        return ManagedStmt(session: db.session, plan: plan, cached: true)
      cacheable = false
      break
  let plan = ManagedPlan(sql: q, statement: prepareStmt(raw, q), inUse: true)
  if plan.statement == nil:
    raise newException(DbError, "SQLite query contains no statement")
  inc db.session.activeStatements
  result = ManagedStmt(session: db.session, plan: plan, cached: cacheable)
  if cacheable:
    db.session.plans.add plan

proc finalizeStmt*(s: var ManagedStmt) {.raises: [].} =
  ## Releases this use of the statement early. Scope exit also releases it.
  `=destroy`(s)
  `=wasMoved`(s)

proc statementHandle(s: ManagedStmt): PStmt =
  if s.plan == nil:
    raise newException(DbError, "Managed SQLite statement is released")
  if s.session.raw == nil or s.plan.statement == nil:
    raise newException(DbError, "Managed SQLite connection is closed")
  s.plan.statement

proc statementHandle(db: ManagedDbConn; s: ManagedStmt): PStmt =
  discard requireOpen(db)
  if db.session != s.session:
    raise newException(DbError, "SQLite statement belongs to another connection")
  statementHandle(s)

template startBindings*(s: ManagedStmt; n: int) =
  if clear_bindings(statementHandle(s)) != SQLITE_OK:
    dbError(s.session.raw)

template bindParam*(db: ManagedDbConn; s: ManagedStmt; idx: int; x, t: untyped) =
  bindParam(db.session.raw, statementHandle(db, s), idx, x, t)

template bindNullParam*(db: ManagedDbConn; s: ManagedStmt; idx: int) =
  bindNullParam(db.session.raw, statementHandle(db, s), idx)

template bindParamJson*(db: ManagedDbConn; s: ManagedStmt; idx: int;
                        x: JsonNode; t: typedesc) =
  bindParamJson(db.session.raw, statementHandle(db, s), idx, x, t)

template bindResult*(db: ManagedDbConn; s: ManagedStmt; idx: int;
                     dest: untyped; t: typedesc; name: string) =
  bindResult(db.session.raw, statementHandle(db, s), idx, dest, t, name)

template bindResultJson*(db: ManagedDbConn; s: ManagedStmt; idx: int;
                         obj: JsonNode; t: typedesc; name: string) =
  bindResultJson(db.session.raw, statementHandle(db, s), idx, obj, t, name)

template columnIsNull*(db: ManagedDbConn; s: ManagedStmt; idx: int): bool =
  columnIsNull(db.session.raw, statementHandle(db, s), idx)

template bindToJson*(db: ManagedDbConn; s: ManagedStmt; idx: int;
                     obj: JsonNode; t: typedesc; name: string) =
  bindToJson(db.session.raw, statementHandle(db, s), idx, obj, t, name)

template startQuery*(db: ManagedDbConn; s: ManagedStmt) =
  discard statementHandle(db, s)

template stopQuery*(db: ManagedDbConn; s: ManagedStmt) =
  stopQuery(db.session.raw, statementHandle(db, s))

template stepQuery*(db: ManagedDbConn; s: ManagedStmt; returnsData: bool): bool =
  stepQuery(db.session.raw, statementHandle(db, s), returnsData)

template getLastId*(db: ManagedDbConn; s: ManagedStmt): int =
  discard statementHandle(db, s)
  getLastId(db.session.raw, s.plan.statement)

template getAffectedRows*(db: ManagedDbConn; s: ManagedStmt): int =
  discard statementHandle(db, s)
  getAffectedRows(db.session.raw, s.plan.statement)
