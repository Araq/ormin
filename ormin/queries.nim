
when not declared(tableNames):
  {.macros.error: "The query DSL requires a tableNames const.".}

when not declared(attributes):
  {.macros.error: "The query DSL requires a attributes const.".}

import macros, strutils
import db_connector/db_common
from os import parentDir, `/`

import db_types
import query_hooks

# SQL dialect specific things:
const
  equals = "="
  nequals = "<>"

type
  Function* = object
    name: string
    arity: int # -1 for 'varargs'
    typ: DbTypeKind # if dbUnknown, use type of the last argument

  SourceColumn = object
    name: string
    typ: DbType

  CteDef = object
    name: string
    sql: string
    cols: seq[SourceColumn]

proc buildHookedParamBinding(prepStmt: NimNode; idx: int; ex, typ: NimNode; isJson: bool): NimNode

var
  functions {.compileTime.} = @[
    Function(name: "count", arity: 1, typ: dbInt),
    Function(name: "coalesce", arity: -1, typ: dbUnknown),
    Function(name: "min", arity: 1, typ: dbUnknown),
    Function(name: "max", arity: 1, typ: dbUnknown),
    Function(name: "avg", arity: 1, typ: dbFloat),
    Function(name: "sum", arity: 1, typ: dbUnknown),
    Function(name: "row_number", arity: 0, typ: dbInt),
    Function(name: "rank", arity: 0, typ: dbInt),
    Function(name: "dense_rank", arity: 0, typ: dbInt),
    Function(name: "percent_rank", arity: 0, typ: dbFloat),
    Function(name: "cume_dist", arity: 0, typ: dbFloat),
    Function(name: "ntile", arity: 1, typ: dbInt),
    Function(name: "isnull", arity: 3, typ: dbUnknown),
    Function(name: "concat", arity: -1, typ: dbVarchar),
    Function(name: "abs", arity: 1, typ: dbUnknown),
    Function(name: "length", arity: 1, typ: dbInt),
    Function(name: "lower", arity: 1, typ: dbVarchar),
    Function(name: "upper", arity: 1, typ: dbVarchar),
    Function(name: "replace", arity: 3, typ: dbVarchar)
  ]

proc isVarargsType(n: NimNode): bool {.compileTime.} =
  ## Checks if the provided type node encodes a varargs parameter.
  case n.kind
  of nnkBracketExpr:
    if n.len > 0:
      let head = n[0]
      if head.kind in {nnkIdent, nnkSym} and head.strVal == "varargs":
        return true
  else:
    discard
  result = false

proc typeNodeToDbKind(n: NimNode): DbTypeKind {.compileTime.} =
  ## Maps the return type of an imported SQL function to DbTypeKind.
  if n.kind == nnkEmpty:
    return dbUnknown
  if isVarargsType(n):
    if n.len > 1:
      return typeNodeToDbKind(n[1])
    return dbUnknown
  let name =
    case n.kind
    of nnkSym, nnkIdent:
      n.strVal
    else:
      $n
  result = dbTypFromName(name)
  if result == dbUnknown and name.len > 4 and name.endsWith("Type"):
    result = dbTypFromName(name[0..^5])

proc registerImportSqlFunction(name: string; arity: int; typ: DbTypeKind) {.compileTime.} =
  ## Adds or updates a Function descriptor for vendor specific SQL routines.
  let lname = name.toLowerAscii()
  for f in mitems(functions):
    if f.name.toLowerAscii() == lname and f.arity == arity:
      f.typ = typ
      return
  let f = Function(name: name, arity: arity, typ: typ)
  when defined(debugOrminDsl):
    echo "registerImportSqlFunction: ", f
  functions.add f

macro importSql*(n: typed): untyped =
  ## Registers a Nim proc as callable SQL function within the query DSL.
  if n.kind notin {nnkProcDef, nnkFuncDef}:
    macros.error("{.importSql.} can only be applied to proc or func definitions", n)
  let params = n[3]
  expectKind(params, nnkFormalParams)
  var paramCount = 0
  var hasVarargs = false
  for i in 1..<params.len:
    let identDefs = params[i]
    expectKind(identDefs, nnkIdentDefs)
    if identDefs.len < 3:
      macros.error("unexpected parameter definition in {.importSql.}", identDefs)
    let defaultValue = identDefs[^1]
    if defaultValue.kind != nnkEmpty:
      macros.error("{.importSql.} procs cannot declare default values", defaultValue)
    let typeNode = identDefs[^2]
    if isVarargsType(typeNode):
      if hasVarargs:
        macros.error("{.importSql.} supports only a single varargs parameter", identDefs)
      if params.len > 2:
        macros.error("varargs parameters cannot be combined with fixed parameters", identDefs)
      if identDefs.len - 2 != 1:
        macros.error("varargs parameter must declare exactly one identifier", identDefs)
      hasVarargs = true
    else:
      paramCount += identDefs.len - 2
  let arity = if hasVarargs: -1 else: paramCount
  let fnName = n[0].strVal
  let retKind = typeNodeToDbKind(params[0])
  registerImportSqlFunction(fnName, arity, retKind)
  result = newStmtList()

type
  Env = seq[(int, string)]
  Params = seq[tuple[ex, typ: NimNode; isJson: bool]]
  QueryKind = enum
    qkNone,
    qkSelect,
    qkJoin,
    qkInsert,
    qkReplace,
    qkUpdate,
    qkDelete,
    qkInsertReturning
  QueryBuilder = ref object
    head, fromm, join, values, where, groupby, having, orderby: string
    limit, offset, returning, onConflict, onConflictWhere: string
    env: Env
    ctes: seq[CteDef]
    cteBase: int
    kind: QueryKind
    retType: NimNode
    singleRow, retTypeIsJson: bool
    retNames: seq[string]
    params: Params
    coln, qmark, aliasGen: int
    colAliases: seq[(string, DbType)]
    # Track inserted column values for potential SQLite RETURNING handling
    insertedValues: seq[(string, NimNode)]
    # For SQLite: expression to return instead of last_insert_rowid()
    retExpr: NimNode
    onConflictTargetSet, onConflictActionSet, onConflictIsDoUpdate, onConflictWhereSet: bool

# Execute a non-row SQL statement strictly (errors on failure)
template execNoRowsStrict*(sqlStmt: string) =
  when defined(debugOrminTrace):
    echo "[[Ormin Executing]]: ", q
  let s {.gensym.} = prepareStmt(db, sqlStmt)
  startQuery(db, s)
  if stepQuery(db, s, false):
    stopQuery(db, s)
  else:
    stopQuery(db, s)
    dbError(db)

# Execute a non-row SQL statement, relying on startQuery to raise on failure
template execNoRowsLoose(sqlStmt: string) =
  when defined(debugOrminTrace):
    echo "[[Ormin Executing]]: ", sqlStmt
  let s {.gensym.} = prepareStmt(db, sqlStmt)
  startQuery(db, s)
  discard stepQuery(db, s, false)
  stopQuery(db, s)

proc newQueryBuilder(): QueryBuilder {.compileTime.} =
  QueryBuilder(head: "", fromm: "", join: "", values: "", where: "",
    groupby: "", having: "", orderby: "", limit: "", offset: "",
    returning: "", onConflict: "", onConflictWhere: "",
    env: @[], ctes: @[], cteBase: 0, kind: qkNone, params: @[],
    retType: newNimNode(nnkTupleTy), singleRow: false,
    retTypeIsJson: false, retNames: @[],
    coln: 0, qmark: 0, aliasGen: 1, colAliases: @[],
    insertedValues: @[], retExpr: newEmptyNode(),
    onConflictTargetSet: false, onConflictActionSet: false,
    onConflictIsDoUpdate: false, onConflictWhereSet: false)

proc getAlias(q: QueryBuilder; tabIndex: int): string =
  result = tableNames[tabIndex][0] & $q.aliasGen
  inc q.aliasGen

proc placeholder(q: QueryBuilder): string =
  when dbBackend == DbBackend.postgre:
    inc q.qmark
    result = "$" & $q.qmark
  else:
    result = "?"

proc cteEnvIndex(i: int): int {.compileTime.} = tableNames.len + i
proc isCteEnvIndex(i: int): bool {.compileTime.} = i >= tableNames.len
proc fromCteEnvIndex(i: int): int {.compileTime.} = i - tableNames.len

proc lookupCte(ctes: openArray[CteDef]; name: string): int {.compileTime.} =
  result = -1
  for i, cte in ctes:
    if cmpIgnoreCase(cte.name, name) == 0:
      return i

proc sourceName(q: QueryBuilder; source: int): string {.compileTime.} =
  if isCteEnvIndex(source):
    result = q.ctes[fromCteEnvIndex(source)].name
  else:
    result = tableNames[source]

proc sourceColumns(q: QueryBuilder; source: int): seq[SourceColumn] {.compileTime.} =
  if isCteEnvIndex(source):
    result = q.ctes[fromCteEnvIndex(source)].cols
  else:
    for a in attributes:
      if a.tabIndex == source:
        result.add SourceColumn(name: a.name, typ: DbType(kind: a.typ))

proc sourceLookup(q: QueryBuilder; table: string): int {.compileTime.} =
  for i, t in tableNames:
    if cmpIgnoreCase(t, table) == 0:
      return i
  let cteIdx = lookupCte(q.ctes, table)
  if cteIdx >= 0:
    return cteEnvIndex(cteIdx)
  result = -1

proc sourceAlias(q: QueryBuilder; source: int; sourceName: string): string {.compileTime.} =
  if q.kind == qkJoin and q.env.len > 0 and q.env[^1][0] == source:
    result = q.env[^1][1]
  elif isCteEnvIndex(source):
    result = sourceName.toLowerAscii() & $q.aliasGen
    inc q.aliasGen
  else:
    result = q.getAlias(source)

proc joinKeyword(kind: string): string {.compileTime.} =
  case kind.toLowerAscii()
  of "join", "innerjoin":
    "inner join "
  of "outerjoin":
    "outer join "
  of "leftjoin", "leftouterjoin":
    "left outer join "
  of "rightjoin", "rightouterjoin":
    "right outer join "
  of "fulljoin", "fullouterjoin":
    "full outer join "
  of "crossjoin":
    "cross join "
  else:
    ""

proc lookup(table, attr: string; qb: QueryBuilder; alias: var string): DbType =
  var found = false
  var foundSource = -1
  for e in qb.env:
    if table.len == 0 or cmpIgnoreCase(sourceName(qb, e[0]), table) == 0:
      for col in sourceColumns(qb, e[0]):
        if cmpIgnoreCase(col.name, attr) == 0:
          if found:
            if foundSource != e[0] or alias != e[1]:
              return DbType(kind: dbUnknown)
          found = true
          foundSource = e[0]
          alias = e[1]
          result = col.typ

proc lookup(table, attr: string; qb: QueryBuilder): DbType =
  var alias: string
  result = lookup(table, attr, qb, alias)

proc lookup(table: openArray[string], name: string): int =
  result = -1
  for i, t in table:
    if cmpIgnoreCase(t, name) == 0:
      return i

proc autoJoin(join: var string, src: (int, string), dest: int;
              destAlias: string): bool =
  var srcCol = -1
  var destCol = -1
  for i, a in attributes:
    if a.tabIndex == src[0] and a.key < 0 and attributes[-a.key - 1].tabIndex == dest:
      if srcCol < 0:
        srcCol = i
        destCol = -a.key - 1
      else:
        return false
  if srcCol >= 0:
    join.add " on "
    join.add destAlias
    join.add "."
    join.add attributes[destCol].name
    join.add equals
    join.add src[1]
    join.add "."
    join.add attributes[srcCol].name
    result = true

proc `$`(a: DbType): string = $a.kind

proc checkBool(a: DbType; n: NimNode) =
  if a.kind != dbBool:
    macros.error "expected type 'bool', but got: " & $a, n

proc checkInt(a: DbType; n: NimNode) =
  if a.kind notin {dbInt, dbSerial}:
    macros.error "expected type 'int', but got: " & $a, n

proc checkCompatible(a, b: DbType; n: NimNode) =
  # Treat serial and int as compatible
  if not (a.kind == b.kind or (a.kind == dbSerial and b.kind == dbInt) or (a.kind == dbInt and b.kind == dbSerial)):
    macros.error "incompatible types: " & $a & " and " & $b, n

proc checkCompatibleSet(a, b: DbType; n: NimNode) =
  discard "too implement; might require a richer type system"

proc toNimType(t: DbTypeKind): NimNode {.compileTime.} =
  let name = ($t).substr(2).toLowerAscii & "Type"
  result = ident(name)

proc toNimType(t: DbType): NimNode {.compileTime.} = toNimType(t.kind)

proc escIdent(dest: var string; src: string) =
  if allCharsInSet(src, {'\33'..'\127'}):
    dest.add(src)
  else:
    dest.add("\"" & replace(src, "\"", "\"\"") & "\"")

proc fmtTableList(tableNames: openArray[string]): string =
  result = ""
  for i, t in tableNames:
    if i > 10: break
    if i > 0: result.add ", "
    result.add t

proc nodeName(n: NimNode): string {.compileTime.} =
  case n.kind
  of nnkIdent, nnkSym:
    result = n.strVal
  of nnkAccQuoted:
    if n.len == 1:
      result = nodeName(n[0])
    else:
      result = ""
  else:
    result = ""

proc isQueryClause(name: string): bool {.compileTime.} =
  case name.toLowerAscii()
  of "with", "select", "distinct", "insert", "update", "replace", "delete",
      "where", "join", "innerjoin", "outerjoin", "leftjoin", "leftouterjoin",
      "rightjoin", "rightouterjoin", "fulljoin", "fullouterjoin", "crossjoin",
      "groupby", "orderby", "having", "limit", "offset", "returning", "produce",
      "onconflict", "donothing", "doupdate":
    result = true
  else:
    result = false

proc isSetOpName(name: string): bool {.compileTime.} =
  case name.toLowerAscii()
  of "union", "intersect", "except":
    result = true
  else:
    result = false

proc isSetOpCall(n: NimNode): bool {.compileTime.} =
  n.kind == nnkCall and isSetOpName(nodeName(n[0]))

proc isNullLiteral(n: NimNode): bool {.compileTime.} =
  case n.kind
  of nnkNilLit:
    result = true
  of nnkIdent, nnkSym:
    result = cmpIgnoreCase(n.strVal, "null") == 0
  else:
    result = false

proc peelTrailingCommand(n: NimNode): tuple[core, tail: NimNode] {.compileTime.} =
  if n.kind == nnkCommand and n.len == 2 and n[1].kind == nnkCommand and
      nodeName(n[1][0]) == "on":
    return (copyNimTree(n), newEmptyNode())

  if n.kind == nnkCommand and n.len == 2 and n[1].kind == nnkCommand and
      nodeName(n[1][0]).toLowerAscii() in ["like", "ilike"]:
    return (copyNimTree(n), newEmptyNode())

  if n.kind == nnkCommand and n.len == 2 and n[1].kind == nnkCommand and
      not isQueryClause(nodeName(n[0])) and not isSetOpName(nodeName(n[0])):
    return (copyNimTree(n[0]), copyNimTree(n[1]))

  if (n.kind == nnkCommand and (isQueryClause(nodeName(n[0])) or isSetOpName(nodeName(n[0])))) or
      isSetOpCall(n):
    return (copyNimTree(n), newEmptyNode())

  if n.len > 0:
    let idx = n.len - 1
    let peeled = peelTrailingCommand(n[idx])
    if peeled.tail.kind != nnkEmpty:
      result.core = copyNimTree(n)
      result.core[idx] = peeled.core
      result.tail = peeled.tail
      return result

  result = (copyNimTree(n), newEmptyNode())

proc flattenQueryCommands(n: NimNode; parts: var seq[NimNode]) {.compileTime.} =
  case n.kind
  of nnkStmtList:
    for it in n:
      flattenQueryCommands(it, parts)
  of nnkCall:
    let name = nodeName(n[0])
    if isQueryClause(name):
      var cmd = newNimNode(nnkCommand)
      for it in n:
        cmd.add copyNimTree(it)
      parts.add cmd
    else:
      parts.add copyNimTree(n)
  of nnkCommand:
    var cmd = copyNimTree(n)
    if cmd.len >= 2:
      let idx = cmd.len - 1
      let peeled = peelTrailingCommand(cmd[idx])
      cmd[idx] = peeled.core
      parts.add cmd
      if peeled.tail.kind != nnkEmpty:
        flattenQueryCommands(peeled.tail, parts)
    else:
      parts.add cmd
  else:
    parts.add copyNimTree(n)

proc queryh(n: NimNode; q: QueryBuilder)
proc queryAsString(q: QueryBuilder, n: NimNode): string
proc applyQueryNode(n: NimNode; q: QueryBuilder)
proc renderInlineQuery(n: NimNode; params: var Params;
                       qb: QueryBuilder): tuple[sql: string, typ: DbType]
proc cond(n: NimNode; q: var string; params: var Params;
          expected: DbType, qb: QueryBuilder): DbType

proc renderWindowClause(n: NimNode; q: var string; params: var Params;
                        qb: QueryBuilder) {.compileTime.} =
  let op = nodeName(n[0]).toLowerAscii()
  case op
  of "partitionby":
    q.add "partition by "
    for i in 1..<n.len:
      discard cond(n[i], q, params, DbType(kind: dbUnknown), qb)
      if i < n.len - 1: q.add ", "
  of "orderby":
    q.add "order by "
    for i in 1..<n.len:
      discard cond(n[i], q, params, DbType(kind: dbUnknown), qb)
      if i < n.len - 1: q.add ", "
  else:
    macros.error "unsupported window clause: " & op, n

proc lookupColumnInEnv(n: NimNode; q: var string; params: var Params;
                      expected: DbType, qb: QueryBuilder): DbType =
  expectKind(n, nnkIdent)
  let name = $n
  block checkAliases:
    for a in qb.colAliases:
      if cmpIgnoreCase(a[0], name) == 0:
        result = a[1]
        break checkAliases
    var alias: string
    result = lookup("", name, qb, alias)
    if result.kind == dbUnknown:
      macros.error "unknown column name: " & name, n
    elif qb.kind in {qkSelect, qkJoin}:
      doAssert alias.len >= 0
      q.add alias
      q.add '.'
  escIdent(q, name)

proc cond(n: NimNode; q: var string; params: var Params;
          expected: DbType, qb: QueryBuilder): DbType =
  case n.kind
  of nnkIdent:
    let name = $n
    if name == "_":
      q.add "*"
      result = DbType(kind: dbUnknown)
    elif cmpIgnoreCase(name, "null") == 0:
      q.add "NULL"
      result = expected
    else:
      result = lookupColumnInEnv(n, q, params, expected, qb)
  of nnkDotExpr:
    let t = $n[0]
    let a = $n[1]
    escIdent(q, t)
    q.add '.'
    escIdent(q, a)
    result = lookup(t, a, qb)
  of nnkPar, nnkStmtListExpr:
    if n.len == 1:
      q.add "("
      result = cond(n[0], q, params, expected, qb)
      q.add ")"
    else:
      macros.error "tuple construction not allowed here", n
  of nnkCurly:
    q.add "("
    let a = cond(n[0], q, params, expected, qb)
    for i in 1..<n.len:
      q.add ", "
      let b = cond(n[i], q, params, a, qb)
      checkCompatible(a, b, n[i])
    q.add ")"
    result = DbType(kind: dbSet)
  of nnkNilLit:
    q.add "NULL"
    result = expected
  of nnkDistinctTy:
    q.add "distinct "
    result = cond(n[0], q, params, expected, qb)
  of nnkStrLit, nnkRStrLit, nnkTripleStrLit:
    result = expected
    if result.kind == dbUnknown:
      # macros.error "cannot infer the type of the literal", n
      result.kind = dbVarchar
    if result.kind == dbBlob:
      # For SQL string literals, single quotes must be doubled.
      # Using strutils.escape would introduce backslashes which are
      # not valid escapes in standard SQL/SQLite. So replace `'` with `''`.
      q.add("b'" & n.strVal.replace("'", "''") & "'")
    else:
      # Standard SQL quoting for text values
      q.add("'" & n.strVal.replace("'", "''") & "'")
  of nnkIntLit..nnkInt64Lit:
    result = expected
    if result.kind == dbUnknown:
      result.kind = dbInt
    q.add $n.intVal
  of nnkFloatLit:
    result = expected
    if result.kind == dbUnknown:
      result.kind = dbFloat
    q.add $n.floatVal
  of nnkInfix:
    let op = $n[0]
    case op
    of "and", "or":
      result = DbType(kind: dbBool)
      let a = cond(n[1], q, params, result, qb)
      checkBool a, n[1]
      q.add ' '
      q.add op
      q.add ' '
      let b = cond(n[2], q, params, result, qb)
      checkBool b, n[2]
    of "<=", "<", ">=", ">", "==", "!=", "=~":
      if isNullLiteral(n[1]) or isNullLiteral(n[2]):
        if op != "==" and op != "!=":
          macros.error "NULL comparisons only support == and !=", n
        if isNullLiteral(n[1]) and isNullLiteral(n[2]):
          macros.error "NULL cannot be compared against NULL", n
        let target = if isNullLiteral(n[1]): n[2] else: n[1]
        discard cond(target, q, params, DbType(kind: dbUnknown), qb)
        if op == "==":
          q.add " is NULL"
        else:
          q.add " is not NULL"
      else:
        let env = qb.env
        if env.len == 2:
          qb.env = @[env[0]]
        let a = cond(n[1], q, params, DbType(kind: dbUnknown), qb)
        q.add ' '
        if op == "==": q.add equals
        elif op == "!=": q.add nequals
        elif op == "=~": q.add "like"
        else: q.add op
        q.add ' '
        if env.len == 2:
          qb.env = @[env[1]]
        let b = cond(n[2], q, params, a, qb)
        checkCompatible a, b, n
      result = DbType(kind: dbBool)
    of "in", "notin":
      let a = cond(n[1], q, params, DbType(kind: dbUnknown), qb)
      if n[2].kind == nnkInfix and $n[2][0] == "..":
        if op == "in": q.add " between "
        else: q.add " not between "
        let r = n[2]
        let b = cond(r[1], q, params, a, qb)
        checkCompatible a, b, n
        q.add " and "
        let c = cond(r[2], q, params, a, qb)
        checkCompatible a, c, n
      else:
        if op == "in": q.add " in "
        else: q.add " not in "
        let b = cond(n[2], q, params, a, qb)
        checkCompatibleSet a, b, n
      result = DbType(kind: dbBool)
    of "as":
      result = cond(n[1], q, params, expected, qb)
      q.add " as "
      expectKind n[2], nnkIdent
      let alias = $n[2]
      escIdent(q, alias)
      qb.colAliases.add((alias, result))
      if expected.kind != dbUnknown:
        checkCompatible result, expected, n
    of "&":
      let a = cond(n[1], q, params, DbType(kind: dbVarchar), qb)
      q.add " || "
      let b = cond(n[2], q, params, a, qb)
      checkCompatible a, b, n
      result = DbType(kind: dbVarchar)
    else:
      # treat as arithmetic operator:
      result = cond(n[1], q, params, DbType(kind: dbUnknown), qb)
      q.add ' '
      q.add op
      q.add ' '
      let b = cond(n[2], q, params, result, qb)
      checkCompatible result, b, n

  of nnkPrefix:
    let op = $n[0]
    case op
    of "?", "%":
      q.add placeHolder(qb)
      result = expected
      if result.kind == dbUnknown:
        macros.error "cannot infer the type of the placeholder", n
      else:
        params.add((ex: n[1], typ: toNimType(result), isJson: op == "%"))
    of "not":
      result = DbType(kind: dbBool)
      q.add "not "
      let a = cond(n[1], q, params, result, qb)
      checkBool a, n[1]
    of "!!":
      result = expected
      if result.kind == dbUnknown:
        macros.error "cannot infer the type of the literal", n
      let arg = n[1]
      if arg.kind in {nnkStrLit..nnkTripleStrLit}: q.add arg.strVal
      else: q.add repr(n[1])
    else:
      # treat as arithmetic operator:
      q.add ' '
      q.add op
      q.add ' '
      result = cond(n[1], q, params, DbType(kind: dbUnknown), qb)
  of nnkCall:
    let op = nodeName(n[0])
    if isSetOpCall(n):
      let subq = renderInlineQuery(n, params, qb)
      q.add subq.sql
      result = subq.typ
      return
    if op == "over":
      if n.len < 2:
        macros.error "over requires at least one expression", n
      result = cond(n[1], q, params, expected, qb)
      q.add " over ("
      for i in 2..<n.len:
        if i > 2: q.add " "
        if n[i].kind notin nnkCallKinds:
          macros.error "window clauses must be calls like partitionby(...) or orderby(...)", n[i]
        renderWindowClause(n[i], q, params, qb)
      q.add ")"
      return
    if op == "exists":
      expectLen n, 2
      let subq = renderInlineQuery(n[1], params, qb)
      q.add "exists ("
      q.add subq.sql
      q.add ")"
      result = DbType(kind: dbBool)
      return
    if op == "asc" or op == "desc":
      expectLen n, 2
      result = cond(n[1], q, params, DbType(kind: dbUnknown), qb)
      q.add ' '
      q.add op
      return
    for f in functions:
      if f.name.toLowerAscii() == op.toLowerAscii():
        if f.arity == n.len-1 or (f.arity == -1 and n.len > 1):
          q.add op
          q.add "("
          for i in 1..<n.len:
            result = cond(n[i], q, params, DbType(kind: f.typ), qb)
            if i < n.len-1: q.add ", "
          if f.typ != dbUnknown: result.kind = f.typ
          q.add ")"
          return
        else:
          macros.error "function " & op & " takes " & $f.arity & " arguments", n
    macros.error "unknown function " & op
  of nnkIfStmt, nnkIfExpr:
    q.add "\Lcase "
    result = DbType(kind: dbUnknown)
    for x in n:
      case x.kind
      of nnkElifBranch, nnkElifExpr:
        q.add "\L  when "
        checkBool(cond(x[0], q, params, DbType(kind: dbBool), qb), x[0])
        q.add " then"
      of nnkElse, nnkElseExpr:
        q.add "\L  else"
      else: macros.error "illformed if expression", n
      q.add "\L    "
      let t = cond(x[^1], q, params, result, qb)
      if result.kind == dbUnknown: result = t
      else: checkCompatible(result, t, x)
    q.add "\Lend"
  of nnkCommand:
    let head = nodeName(n[0])
    if head == "select" or head == "distinct":
      let subq = renderInlineQuery(n, params, qb)
      q.add subq.sql
      result = subq.typ
    elif n.len == 2 and n[1].kind == nnkCommand and n[1].len == 2:
      let op = nodeName(n[1][0]).toLowerAscii()
      if op in ["like", "ilike"]:
        let env = qb.env
        if env.len == 2:
          qb.env = @[env[0]]
        let a =
          if op == "ilike" and dbBackend == DbBackend.sqlite:
            q.add "lower("
            let t = cond(n[0], q, params, DbType(kind: dbUnknown), qb)
            q.add ")"
            t
          else:
            cond(n[0], q, params, DbType(kind: dbUnknown), qb)
        q.add " "
        if op == "ilike" and dbBackend != DbBackend.sqlite:
          q.add "ilike"
        else:
          q.add "like"
        q.add " "
        if env.len == 2:
          qb.env = @[env[1]]
        let b =
          if op == "ilike" and dbBackend == DbBackend.sqlite:
            q.add "lower("
            let t = cond(n[1][1], q, params, a, qb)
            q.add ")"
            t
          else:
            cond(n[1][1], q, params, a, qb)
        checkCompatible a, b, n
        result = DbType(kind: dbBool)
      else:
        macros.error "construct not supported in condition: " & treeRepr n, n
    else:
      macros.error "construct not supported in condition: " & treeRepr n, n
  else:
    macros.error "construct not supported in condition: " & treeRepr n, n

proc generateRoutine(name: NimNode, q: QueryBuilder;
                     sql: string; k: NimNodeKind): NimNode =
  let prepStmt = ident($name & "PrepStmt")
  let prepare = newVarStmt(prepStmt, newCall(bindSym"prepareStmt", ident"db", newLit(sql)))

  let body = newStmtList()
  # Ensure the prepared statement is created before binding/starting the query
  body.add prepare

  var finalParams = newNimNode(nnkFormalParams)
  if q.retTypeIsJson:
    if k == nnkIteratorDef:
      body.add newVarStmt(ident"res", newCall(bindSym"createJObject"))
    else:
      body.add newAssignment(ident"result", newCall(bindSym"createJArray"))
    finalParams.add ident"JsonNode"
  else:
    var rtyp = if q.retType.len > 1:
      q.retType
    else:
      q.retType[0][1]
    body.add newTree(nnkVarSection, newIdentDefs(ident"res", rtyp))
    if k != nnkIteratorDef:
      rtyp = nnkBracketExpr.newTree(ident"seq", rtyp)
    finalParams.add rtyp
  when dbBackend == DbBackend.postgre:
    finalParams.add newIdentDefs(ident"db", newTree(nnkDotExpr, ident"ormin_postgre", ident"DbConn"))
  elif dbBackend == DbBackend.sqlite:
    finalParams.add newIdentDefs(ident"db", newTree(nnkDotExpr, ident"ormin_sqlite", ident"DbConn"))
  else:
    finalParams.add newIdentDefs(ident"db", ident("DbConn"))
  var i = 1
  if q.params.len > 0:
    body.add newCall(bindSym"startBindings", prepStmt, newLit(q.params.len))
    for p in q.params:
      if p.isJson:
        finalParams.add newIdentDefs(p.ex, ident"JsonNode")
        body.add buildHookedParamBinding(prepStmt, i, p.ex, p.typ, true)
      else:
        finalParams.add newIdentDefs(p.ex, p.typ)
        body.add buildHookedParamBinding(prepStmt, i, p.ex, p.typ, false)
      inc i
  body.add newCall(bindSym"startQuery", ident"db", prepStmt)
  let yld = newStmtList()
  if k != nnkIteratorDef:
    if q.retTypeIsJson:
      yld.add newVarStmt(ident"res", newCall(bindSym"createJObject"))
  let fn = if q.retTypeIsJson: bindSym"bindResultJson" else: bindSym"bindResult"
  if q.retType.len > 1:
    var i = 0
    for r in q.retType:
      template resAt(i) {.dirty.} = res[i]
      let resx = if q.retTypeIsJson: ident"res" else: getAst(resAt(newLit(i)))
      yld.add newCall(fn, ident"db", prepStmt, newLit(i),
                      resx, copyNimTree r[1], newLit q.retNames[i])
      inc i
  else:
    yld.add newCall(fn, ident"db", prepStmt, newLit(0), ident"res",
                    copyNimTree q.retType[0][1], newLit q.retNames[0])
  if k == nnkIteratorDef:
    yld.add newTree(nnkYieldStmt, ident"res")
  else:
    yld.add newCall("add", ident"result", ident"res")

  let whileStmt = newTree(nnkWhileStmt,
    newCall(bindSym"stepQuery", ident"db", prepStmt, newLit true), yld)
  body.add whileStmt
  body.add newCall(bindSym"stopQuery", ident"db", prepStmt)

  let iter = newTree(k,
    name,
    newEmptyNode(),
    newEmptyNode(),
    finalParams,
    newEmptyNode(), # pragmas
    newEmptyNode(),
    body)
  result = newStmtList(prepare, iter)

proc getColumnName(n: NimNode): string =
  case n.kind
  of nnkPar:
    if n.len == 1: result = getColumnName(n[0])
  of nnkInfix:
    if $n[0] == "as": result = getColumnName(n[2])
  of nnkIdent, nnkSym:
    result = $n
  else:
    # this is a little hacky, if the column name starts with
    # a space, it means "could not extract the column name"
    # but we need to emit this macros.error lazily:
    result = " " & repr(n)

proc retName(q: QueryBuilder; i: int; n: NimNode): string =
  result = q.retNames[i]
  if q.retTypeIsJson and result.startsWith(" "):
    macros.error "cannot extract column name of:" & result, n
    result = ""

proc selectAll(q: QueryBuilder; tabIndex: int; arg, lineInfo: NimNode) =
  proc fieldImpl(q: QueryBuilder; arg: NimNode; name: string): NimNode =
    if q.retTypeIsJson:
      result = newTree(nnkBracketExpr, arg, newLit(name))
    else:
      result = newTree(nnkDotExpr, arg, ident(name))
  template field(): untyped = fieldImpl(q, arg, a.name)
  case q.kind
  of qkSelect, qkJoin:
    for a in sourceColumns(q, tabIndex):
        if q.coln > 0: q.head.add ", "
        inc q.coln
        q.retType.add nnkIdentDefs.newTree(newIdentNode(a.name), toNimType(a.typ), newEmptyNode())
        q.retNames.add a.name
        doAssert q.env.len > 0
        q.head.add q.env[^1][1]
        q.head.add '.'
        escIdent(q.head, a.name)
  of qkInsert, qkInsertReturning, qkReplace:
    if isCteEnvIndex(tabIndex):
      macros.error "cannot insert into a CTE", lineInfo
    for a in attributes:
      # we do not set the primary key:
      if a.tabIndex == tabIndex and a.key != 1:
        if q.coln > 0: q.head.add ", "
        escIdent(q.head, a.name)
        inc q.coln
        q.params.add((ex: field(), typ: toNimType(a.typ), isJson: q.retTypeIsJson))
        if q.values.len > 0: q.values.add ", "
        q.values.add placeholder(q)
  of qkUpdate:
    if isCteEnvIndex(tabIndex):
      macros.error "cannot update a CTE", lineInfo
    for a in attributes:
      if a.tabIndex == tabIndex and a.key != 1:
        if q.coln > 0: q.head.add ", "
        escIdent(q.head, a.name)
        inc q.coln
        q.params.add((ex: field(), typ: toNimType(a.typ), isJson: q.retTypeIsJson))
        q.head.add " = "
        q.head.add placeholder(q)
  else:
    macros.error "select '_' not supported for this construct", lineInfo


proc tableSel(n: NimNode; q: QueryBuilder) =
  if n.kind == nnkCall and q.kind != qkDelete:
    let call = n
    let tab = $call[0]
    let tabIndex = sourceLookup(q, tab)
    if tabIndex < 0:
      macros.error "unknown table name: " & tab & " from: " & fmtTableList(tableNames), n
      return
    let alias = sourceAlias(q, tabIndex, tab)
    if q.kind == qkSelect:
      escIdent(q.fromm, tab)
      q.fromm.add " as " & alias
    elif q.kind != qkJoin:
      escIdent(q.head, tab)
    if q.kind == qkUpdate: q.head.add " set "
    elif q.kind notin {qkSelect, qkJoin}: q.head.add "("

    if q.env.len == 0 or q.env[^1] != (tabIndex, alias):
      q.env.add((tabindex, alias))
    for i in 1..<call.len:
      let col = call[i]
      if col.kind == nnkExprEqExpr and q.kind in {qkInsert, qkInsertReturning, qkUpdate, qkReplace}:
        let colname = $col[0]
        if colname == "_":
          selectAll(q, tabIndex, col[1], col)
        else:
          let coltype = lookup("", colname, q)
          if coltype.kind == dbUnknown:
            macros.error "unkown column name: " & colname, col
          else:
            if q.coln > 0: q.head.add ", "
            escIdent(q.head, colname)
            #q.params.add newIdentDefs(col[1], toNimType(coltype))
            inc q.coln
          if q.kind == qkUpdate:
            q.head.add " = "
            discard cond(col[1], q.head, q.params, coltype, q)
          else:
            if q.values.len > 0: q.values.add ", "
            discard cond(col[1], q.values, q.params, coltype, q)
          # Track inserted values for potential SQLite RETURNING support
          if q.kind in {qkInsert, qkInsertReturning, qkReplace}:
            var valNode = col[1]
            if valNode.kind == nnkPrefix and (let opv = $valNode[0]; opv == "?" or opv == "%"):
              q.insertedValues.add((colname, valNode[1]))
            elif valNode.kind in {nnkStrLit, nnkRStrLit, nnkTripleStrLit, nnkIntLit..nnkInt64Lit, nnkFloatLit}:
              q.insertedValues.add((colname, valNode))

      elif col.kind == nnkPrefix and (let op = $col[0]; op == "?" or op == "%"):
        let colname = $col[1]
        let coltype = lookup("", colname, q)
        if coltype.kind == dbUnknown:
          macros.error "unkown column name: " & colname, col
        else:
          if q.coln > 0: q.head.add ", "
          escIdent(q.head, colname)
          inc q.coln
          q.params.add((ex: col[1], typ: toNimType(coltype), isJson: op == "%"))
        if q.kind == qkUpdate:
          q.head.add " = "
          q.head.add placeholder(q)
        else:
          if q.values.len > 0: q.values.add ", "
          q.values.add placeholder(q)
      elif col.kind == nnkIdent and $col == "_":
        selectAll(q, tabIndex, ident"arg", col)
      elif q.kind in {qkSelect, qkJoin}:
        if q.coln > 0: q.head.add ", "
        inc q.coln
        let t =
          if col.kind in {nnkIdent, nnkSym}:
            let colname = $col
            var typ = DbType(kind: dbUnknown)
            for srcCol in sourceColumns(q, tabIndex):
              if cmpIgnoreCase(srcCol.name, colname) == 0:
                typ = srcCol.typ
                break
            if typ.kind == dbUnknown:
              macros.error "unknown column name: " & colname, col
            q.head.add q.env[^1][1]
            q.head.add '.'
            escIdent(q.head, colname)
            typ
          else:
            cond(col, q.head, q.params, DbType(kind: dbUnknown), q)
        q.retType.add nnkIdentDefs.newTree(newIdentNode(getColumnName(col)), toNimType(t), newEmptyNode())
        q.retNames.add getColumnName(col)
      else:
        macros.error "unknown selector: " & repr(n), n
    if q.kind notin {qkUpdate, qkSelect, qkJoin}: q.head.add ")"
  elif n.kind in {nnkIdent, nnkAccQuoted, nnkSym} and q.kind == qkDelete:
    let tab = $n
    let tabIndex = sourceLookup(q, tab)
    if tabIndex < 0:
      macros.error "unknown table name: " & tab & " from: " & fmtTableList(tableNames), n
      return
    if isCteEnvIndex(tabIndex):
      macros.error "cannot delete from a CTE", n
    escIdent(q.head, tab)
    q.env.add((tabindex, q.getAlias(tabIndex)))
  elif n.kind == nnkRStrLit:
    q.head.add n.strVal
  else:
    macros.error "unknown selector: " & repr(n), n


proc queryh(n: NimNode; q: QueryBuilder) =
  var n = n
  if n.kind == nnkCall:
    let c = newNimNode(nnkCommand)
    for i in 0..<n.len:
      c.add n[i]
    n = c
  expectKind n, nnkCommand
  let kind = nodeName(n[0]).toLowerAscii()
  case kind
  of "with":
    expectLen n, 2
    expectKind n[1], nnkCall
    if n[1].len != 2:
      macros.error "with expects syntax like with cteName(select ...)", n[1]
    let cteName = nodeName(n[1][0])
    if cteName.len == 0:
      macros.error "with requires a CTE name", n[1][0]
    if lookupCte(q.ctes, cteName) >= 0:
      macros.error "duplicate CTE name: " & cteName, n[1][0]
    var subq = newQueryBuilder()
    subq.qmark = q.qmark
    subq.aliasGen = q.aliasGen
    subq.ctes = q.ctes
    subq.cteBase = q.ctes.len
    applyQueryNode(n[1][1], subq)
    if subq.kind notin {qkSelect, qkJoin}:
      macros.error "CTEs require a select-style query", n[1][1]
    q.qmark = subq.qmark
    q.aliasGen = subq.aliasGen
    for p in subq.params:
      q.params.add p
    var cols: seq[SourceColumn] = @[]
    for i, name in subq.retNames:
      let typNode =
        if i < subq.retType.len and subq.retType[i].kind == nnkIdentDefs and subq.retType[i].len > 1:
          subq.retType[i][1]
        else:
          newEmptyNode()
      cols.add SourceColumn(name: name, typ: DbType(kind: typeNodeToDbKind(typNode)))
    q.ctes.add CteDef(name: cteName, sql: queryAsString(subq, n[1][1]), cols: cols)
  of "select":
    q.kind = qkSelect
    q.head = "select "
    expectLen n, 2
    if n[1].kind == nnkCommand and nodeName(n[1][0]) == "distinct":
      expectLen n[1], 2
      q.head = "select distinct "
      tableSel(n[1][1], q)
    else:
      tableSel(n[1], q)
  of "distinct":
    q.kind = qkSelect
    q.head = "select distinct "
    expectLen n, 2
    tableSel(n[1], q)
  of "insert":
    q.kind = qkInsert
    q.head = "insert into "
    expectLen n, 2
    tableSel(n[1], q)
  of "update":
    q.kind = qkUpdate
    q.head = "update "
    expectLen n, 2
    tableSel(n[1], q)
  of "replace":
    q.kind = qkReplace
    q.head = "replace into "
    expectLen n, 2
    tableSel(n[1], q)
  of "delete":
    q.kind = qkDelete
    q.head = "delete from "
    expectLen n, 2
    tableSel(n[1], q)
  of "where":
    expectLen n, 2
    if q.kind in {qkInsert, qkInsertReturning}:
      if not q.onConflictTargetSet or not q.onConflictActionSet or not q.onConflictIsDoUpdate:
        macros.error "'where' for insert is only supported after 'onconflict(...)' and 'doupdate(...)'", n
      if q.onConflictWhereSet:
        macros.error "conflict update 'where' can only be specified once", n
      var conflictWhere = ""
      # In PostgreSQL upsert WHERE, bare column names are ambiguous between
      # target table and EXCLUDED. Resolve bare identifiers against target table.
      let oldKind = q.kind
      let oldEnv = q.env
      if q.env.len > 0:
        let source = q.env[^1][0]
        q.kind = qkSelect
        q.env = @[(source, sourceName(q, source))]
      let t = cond(n[1], conflictWhere, q.params, DbType(kind: dbBool), q)
      q.kind = oldKind
      q.env = oldEnv
      checkBool(t, n)
      q.onConflictWhere = " where " & conflictWhere
      q.onConflictWhereSet = true
    else:
      let t = cond(n[1], q.where, q.params, DbType(kind: dbBool), q)
      checkBool(t, n)
  of "join", "innerjoin", "outerjoin", "leftjoin", "leftouterjoin",
      "rightjoin", "rightouterjoin", "fulljoin", "fullouterjoin", "crossjoin":
    q.join.add "\L" & joinKeyword(kind)
    expectLen n, 2
    let joinClause = n[1]
    if kind == "crossjoin" and joinClause.kind == nnkCommand and joinClause.len == 2 and
       joinClause[1].kind == nnkCommand and joinClause[1].len == 2 and $joinClause[1][0] == "on":
      macros.error "crossjoin does not support an on clause", n
    if joinClause.kind == nnkCommand and joinClause.len == 2 and
       joinClause[1].kind == nnkCommand and joinClause[1].len == 2 and $joinClause[1][0] == "on" and
       joinClause[0].kind == nnkCall:
      let tab = $joinClause[0][0]
      let tabIndex = sourceLookup(q, tab)
      if tabIndex < 0:
        macros.error "unknown table name: " & tab & " from: " & fmtTableList(tableNames), n
      else:
        escIdent(q.join, tab)
        let alias = sourceAlias(q, tabIndex, tab)
        q.join.add " as " & alias
        var oldEnv = q.env
        q.env = @[(tabIndex, alias)]
        q.kind = qkJoin
        tableSel(joinClause[0], q)
        swap q.env, oldEnv
        let onn = joinClause[1][1]
        q.join.add " on "
        oldEnv = q.env
        q.env.add((tabIndex, alias))
        let t = cond(onn, q.join, q.params, DbType(kind: dbBool), q)
        swap q.env, oldEnv
        checkBool(t, onn)
    elif joinClause.kind == nnkCall:
      let tab = $joinClause[0]
      let tabIndex = sourceLookup(q, tab)
      if tabIndex < 0:
        macros.error "unknown table name: " & tab & " from: " & fmtTableList(tableNames), n[1][0]
      else:
        if kind == "crossjoin":
          let alias = sourceAlias(q, tabIndex, tab)
          escIdent(q.join, tab)
          q.join.add " as " & alias
          var oldEnv = q.env
          q.env = @[(tabIndex, alias)]
          q.kind = qkJoin
          tableSel(n[1], q)
          swap q.env, oldEnv
        else:
          # auto join:
          if isCteEnvIndex(tabIndex) or isCteEnvIndex(q.env[^1][0]):
            macros.error "automatic joins are only supported for base tables", n
          let alias = q.getAlias(tabIndex)
          escIdent(q.join, tab)
          q.join.add " as " & alias
          if not autoJoin(q.join, q.env[^1], tabIndex, alias):
            macros.error "cannot compute auto join from: " & tableNames[q.env[^1][0]] & " to: " & tab, n
          var oldEnv = q.env
          q.env = @[(tabIndex, alias)]
          q.kind = qkJoin
          tableSel(n[1], q)
          swap q.env, oldEnv
    else:
      macros.error "unknown query component " & repr(n), n
  of "groupby":
    for i in 1..<n.len:
      discard cond(n[i], q.groupby, q.params, DbType(kind: dbUnknown), q)
  of "orderby":
    for i in 1..<n.len:
      discard cond(n[i], q.orderby, q.params, DbType(kind: dbUnknown), q)
      if i != n.len - 1: q.orderby &= ", "
  of "having":
    expectLen n, 2
    let t = cond(n[1], q.having, q.params, DbType(kind: dbBool), q)
    checkBool(t, n[1])
  of "limit":
    expectLen n, 2
    if n[1].kind == nnkIntLit and n[1].intVal == 1:
      q.singleRow = true
    let t = cond(n[1], q.limit, q.params, DbType(kind: dbInt), q)
    checkInt(t, n[1])
  of "offset":
    expectLen n, 2
    let t = cond(n[1], q.offset, q.params, DbType(kind: dbInt), q)
    checkInt(t, n[1])
  of "onconflict":
    if q.kind notin {qkInsert, qkInsertReturning}:
      macros.error "'onconflict' only possible within 'insert'", n
    if q.onConflictTargetSet:
      macros.error "'onconflict' can only be specified once", n
    if n.len < 2:
      macros.error "'onconflict' expects one or more columns", n
    q.onConflict = "\Lon conflict ("
    for i in 1..<n.len:
      let col = n[i]
      let colname = nodeName(col)
      if colname.len == 0:
        macros.error "'onconflict' columns must be identifiers", col
      if lookup("", colname, q).kind == dbUnknown:
        macros.error "unknown column name: " & colname, col
      if i > 1:
        q.onConflict.add ", "
      escIdent(q.onConflict, colname)
    q.onConflict.add ")"
    q.onConflictTargetSet = true
  of "donothing":
    if q.kind notin {qkInsert, qkInsertReturning}:
      macros.error "'donothing' only possible within 'insert'", n
    if not q.onConflictTargetSet:
      macros.error "'donothing' requires a preceding 'onconflict' clause", n
    if q.onConflictActionSet:
      macros.error "conflict action already set; choose only one of 'donothing' or 'doupdate'", n
    expectLen n, 1
    q.onConflict.add " do nothing"
    q.onConflictActionSet = true
    q.onConflictIsDoUpdate = false
  of "doupdate":
    if q.kind notin {qkInsert, qkInsertReturning}:
      macros.error "'doupdate' only possible within 'insert'", n
    if not q.onConflictTargetSet:
      macros.error "'doupdate' requires a preceding 'onconflict' clause", n
    if q.onConflictActionSet:
      macros.error "conflict action already set; choose only one of 'donothing' or 'doupdate'", n
    if n.len < 2:
      macros.error "'doupdate' expects assignments like doupdate(col = value)", n
    q.onConflict.add " do update set "
    for i in 1..<n.len:
      let assignment = n[i]
      if assignment.kind != nnkExprEqExpr:
        macros.error "'doupdate' expects assignments like doupdate(col = value)", assignment
      let colname = nodeName(assignment[0])
      if colname.len == 0:
        macros.error "'doupdate' assignments must target a column identifier", assignment[0]
      let coltype = lookup("", colname, q)
      if coltype.kind == dbUnknown:
        macros.error "unknown column name: " & colname, assignment[0]
      if i > 1:
        q.onConflict.add ", "
      escIdent(q.onConflict, colname)
      q.onConflict.add " = "
      discard cond(assignment[1], q.onConflict, q.params, coltype, q)
    q.onConflictActionSet = true
    q.onConflictIsDoUpdate = true
  of "returning":
    if q.kind != qkInsert:
      macros.error "'returning' only possible within 'insert'"
    q.kind = qkInsertReturning
    expectLen n, 2
    when dbBackend == DbBackend.sqlite:
      q.returning = ";\nselect last_insert_rowid()"
    elif dbBackend == DbBackend.mysql:
      q.returning = ";\nselect LAST_INSERT_ID()"
    else:
      q.returning = " returning "
    var colname = ""
    let nimType = toNimType lookupColumnInEnv(n[1], colname, q.params, DbType(kind: dbUnknown), q)
    q.singleRow = true
    when dbBackend != DbBackend.sqlite:
      q.retType.add newIdentDefs(ident(colname), nimType)
      q.retNames.add colname
    else:
      discard nimType

    # check if the column is inserted, if so, use the inserted expression to return the value
    var found = false
    for p in q.insertedValues:
      if cmpIgnoreCase(p[0], colname) == 0:
        q.retExpr = p[1]
        found = true
        break

    when dbBackend == DbBackend.postgre:
      q.returning.add colname
  of "produce":
    expectLen n, 2
    if eqIdent(n[1], "json"):
      q.retTypeIsJson = true
    elif eqIdent(n[1], "nim") or eqIdent(n[1], "tuple"):
      q.retTypeIsJson = false
    else:
      macros.error "produce expects 'json' or 'nim', but got: " & repr(n[1]), n
  else:
    macros.error "unknown query component " & repr(n), n

proc queryAsString(q: QueryBuilder, n: NimNode): string =
  if q.onConflictTargetSet and not q.onConflictActionSet:
    macros.error "'onconflict' requires either 'donothing' or 'doupdate'", n
  if q.onConflictWhereSet and not q.onConflictIsDoUpdate:
    macros.error "conflict update 'where' requires 'doupdate(...)'", n
  if q.cteBase < q.ctes.len:
    result.add "with "
    for i in q.cteBase..<q.ctes.len:
      if i > q.cteBase:
        result.add ",\L"
      escIdent(result, q.ctes[i].name)
      result.add " as (\L"
      result.add q.ctes[i].sql
      result.add "\L)"
    result.add "\L"
  result.add q.head
  if q.fromm.len > 0:
    result.add "\Lfrom "
    result.add q.fromm
  if q.join.len > 0:
    result.add q.join
  if q.values.len > 0:
    result.add "\Lvalues ("
    result.add q.values
    result.add ")"
  if q.onConflict.len > 0:
    result.add q.onConflict
  if q.onConflictWhere.len > 0:
    result.add q.onConflictWhere
  if q.where.len > 0:
    if q.kind in {qkSelect, qkJoin, qkUpdate, qkDelete}:
      result.add "\Lwhere "
      result.add q.where
    else:
      macros.error "'where' is not supported for this query kind", n
  if q.groupby.len > 0:
    result.add "\Lgroup by "
    result.add q.groupby
  if q.having.len > 0:
    result.add "\Lhaving "
    result.add q.having
  if q.orderby.len > 0:
    result.add "\Lorder by "
    result.add q.orderby
  if q.limit.len > 0:
    result.add "\Llimit "
    result.add q.limit
  if q.offset.len > 0:
    result.add "\Loffset "
    result.add q.offset
  when dbBackend != DbBackend.sqlite:
    if q.returning.len > 0:
      result.add q.returning
  when defined(debugOrminSql):
    macros.hint("Ormin SQL:\n" & $result, n)

proc sameReturnShape(a, b: NimNode): bool {.compileTime.} =
  if a.len != b.len:
    return false
  for i in 0..<a.len:
    let at = if a[i].kind == nnkIdentDefs and a[i].len > 1: a[i][1] else: a[i]
    let bt = if b[i].kind == nnkIdentDefs and b[i].len > 1: b[i][1] else: b[i]
    if repr(at) != repr(bt):
      return false
  result = true

proc buildSetOpQueryParts(op: string; branches: openArray[NimNode];
                          q: QueryBuilder; lineInfo: NimNode) {.compileTime.} =
  if q.kind != qkNone or q.head.len > 0 or q.params.len > 0:
    macros.error "set operations must form the whole query", lineInfo
  if branches.len < 2:
    macros.error "set operations require at least two queries", lineInfo

  q.kind = qkSelect
  q.singleRow = false

  for i, branchNode in branches:
    var branch = newQueryBuilder()
    branch.qmark = q.qmark
    branch.aliasGen = q.aliasGen
    branch.ctes = q.ctes
    branch.cteBase = q.ctes.len
    applyQueryNode(branchNode, branch)
    if branch.kind notin {qkSelect, qkJoin}:
      macros.error "set operations only support select-style queries", branchNode

    if i > 0:
      q.head.add "\L" & op & "\L"
    if isSetOpCall(branchNode):
      q.head.add "("
      q.head.add queryAsString(branch, branchNode)
      q.head.add ")"
    else:
      q.head.add queryAsString(branch, branchNode)

    q.qmark = branch.qmark
    q.aliasGen = branch.aliasGen
    for p in branch.params:
      q.params.add p

    if i == 0:
      q.retType = branch.retType
      q.retNames = branch.retNames
      q.retTypeIsJson = branch.retTypeIsJson
    elif branch.retTypeIsJson != q.retTypeIsJson or
        not sameReturnShape(branch.retType, q.retType):
      macros.error "all set operation branches must return the same types", branchNode

proc buildSetOpQuery(n: NimNode; q: QueryBuilder) {.compileTime.} =
  var branches: seq[NimNode] = @[]
  for i in 1..<n.len:
    branches.add n[i]
  buildSetOpQueryParts(nodeName(n[0]).toLowerAscii(), branches, q, n)

proc isSetOpToken(n: NimNode): bool {.compileTime.} =
  n.kind in {nnkIdent, nnkSym, nnkAccQuoted} and isSetOpName(nodeName(n))

proc applyQueryNode(n: NimNode; q: QueryBuilder) =
  if isSetOpCall(n):
    buildSetOpQuery(n, q)
    return

  var flattened: seq[NimNode]
  flattenQueryCommands(n, flattened)
  var hasInfixSetOp = false
  for part in flattened:
    if isSetOpToken(part):
      hasInfixSetOp = true
      break

  if hasInfixSetOp:
    var op = ""
    var branches: seq[NimNode] = @[]
    var currentBranch = newStmtList()
    proc flushBranch(lineInfo: NimNode) {.compileTime.} =
      if currentBranch.len == 0:
        macros.error "expected query before set operation", lineInfo
      branches.add currentBranch
      currentBranch = newStmtList()
    for part in flattened:
      if isSetOpToken(part):
        let currentOp = nodeName(part).toLowerAscii()
        if op.len == 0:
          op = currentOp
        elif op != currentOp:
          macros.error "mixed infix set operations are not supported; use nesting for precedence", part
        flushBranch(part)
      else:
        if part.kind in {nnkCommand, nnkCall} or isSetOpCall(part):
          currentBranch.add part
        else:
          macros.error "illformed query", part
    if op.len == 0:
      macros.error "expected set operation between queries", n
    if currentBranch.len == 0:
      macros.error "set operation requires a query after " & op, n
    branches.add currentBranch
    buildSetOpQueryParts(op, branches, q, n)
    return

  for part in flattened:
    if isSetOpCall(part):
      buildSetOpQuery(part, q)
    elif part.kind in {nnkCommand, nnkCall}:
      queryh(part, q)
    else:
      macros.error "illformed query", part

proc renderInlineQuery(n: NimNode; params: var Params;
                       qb: QueryBuilder): tuple[sql: string, typ: DbType] =
  var subq = newQueryBuilder()
  subq.qmark = qb.qmark
  subq.aliasGen = qb.aliasGen
  subq.ctes = qb.ctes
  subq.cteBase = qb.ctes.len
  applyQueryNode(n, subq)
  if subq.kind notin {qkSelect, qkJoin}:
    macros.error "subqueries require a select-style query", n
  qb.qmark = subq.qmark
  qb.aliasGen = subq.aliasGen
  for p in subq.params:
    params.add p
  result.sql = queryAsString(subq, n)
  result.typ = DbType(kind: dbSet)

proc makeSeq(retType: NimNode; singleRow: bool): NimNode =
  if not singleRow:
    result = newTree(nnkBracketExpr, bindSym"seq", retType)
  else:
    result = retType

proc buildHookedParamBinding(prepStmt: NimNode; idx: int; ex, typ: NimNode; isJson: bool): NimNode =
  if isJson:
    return newCall(bindSym"bindParamJson", ident"db", prepStmt, newLit(idx), ex, typ)

  result = quote do:
    block:
      var converted: DbValue[`typ`]
      toQueryHook(converted, `ex`)
      if converted.isNull:
        bindNullParam(db, `prepStmt`, `idx`)
      else:
        bindParam(db, `prepStmt`, `idx`, converted.value, `typ`)

proc buildHookedResultAssign(prepStmt, destExpr, destType, sourceType: NimNode; idx: int; colName: string): NimNode =
  result = quote do:
      var rawValue: DbValue[`sourceType`]
      bindResult(db, `prepStmt`, `idx`, rawValue, `sourceType`, `colName`)
      `destExpr`.fromQueryHook(rawValue)

proc buildQueryHookAction(q: QueryBuilder; prepStmt, res, retType: NimNode; singleRow: bool): NimNode =
  let selectedCount = newLit(q.retType.len)

  let mapped = genSym(nskVar, "mapped")
  let mappedStmt = newStmtList()
  mappedStmt.add quote do:
    var `mapped` = `retType`()
  for idx, name in q.retNames:
    let fieldName = ident(name)
    let sourceType = q.retType[idx][1]
    let destExpr = quote do:
      `mapped`.`fieldName`
    let hooked = buildHookedResultAssign(prepStmt, destExpr, retType, sourceType, idx, name)
    mappedStmt.add quote do:
      when compiles(`mapped`.`fieldName`):
        `hooked`
  if singleRow:
    mappedStmt.add quote do:
      `res` = `mapped`
  else:
    mappedStmt.add quote do:
      `res`.add(`mapped`)

  let scalarStmt = newStmtList()
  let mappedScalar = if singleRow: res else: genSym(nskVar, "mapped")
  let sourceType = q.retType[0][1]
  if not singleRow:
    scalarStmt.add quote do:
      var `mappedScalar`: `retType`
  scalarStmt.add buildHookedResultAssign(prepStmt, mappedScalar, retType, sourceType, 0, q.retNames[0])
  if not singleRow:
    scalarStmt.add quote do:
      `res`.add(`mappedScalar`)

  result = quote do:
    block:
      when `retType` is object or `retType` is ref object:
        `mappedStmt`
      else:
        when `selectedCount` != 1:
          {.error: "query(T): scalar mapping expects exactly one selected column".}
        else:
          `scalarStmt`

proc queryImpl(q: QueryBuilder; body: NimNode; attempt, produceJson: bool): NimNode =
  expectKind body, nnkStmtList
  expectMinLen body, 1

  q.retTypeIsJson = produceJson
  applyQueryNode(body, q)
  let sql = queryAsString(q, body)
  let prepStmt = genSym(nskLet)
  let res = genSym(nskVar)
  result = newTree(
    if q.retType.len > 0: nnkStmtListExpr else: nnkStmtList,
    newLetStmt(prepStmt, newCall(bindSym"prepareStmt", ident"db", newLit sql))
  )
  let rtyp = if q.retType.len > 1 or q.retType.len == 0:
    q.retType
  else:
    q.retType[0][1]
  if q.retType.len > 0:
    if q.singleRow:
      if q.retTypeIsJson:
        result.add newVarStmt(res, newCall(bindSym"createJObject"))
      else:
        result.add newTree(nnkVarSection, newIdentDefs(res, rtyp))
    else:
      if q.retTypeIsJson:
        result.add newVarStmt(res, newCall(bindSym"createJArray"))
      else:
        result.add newTree(nnkVarSection, newIdentDefs(res,
          newTree(nnkBracketExpr, bindSym"seq", rtyp),
          newTree(nnkPrefix, bindSym"@", newTree(nnkBracket))))
  let blk = newStmtList()
  var i = 1
  if q.params.len > 0:
    blk.add newCall(bindSym"startBindings", prepStmt, newLit(q.params.len))
    for p in q.params:
      blk.add buildHookedParamBinding(prepStmt, i, p.ex, p.typ, p.isJson)
      inc i
  blk.add newCall(bindSym"startQuery", ident"db", prepStmt)
  var body = newStmtList()
  let it = if q.singleRow: res else: genSym(nskVar)
  if not q.singleRow and q.retType.len > 0:
    if q.retTypeIsJson:
      body.add newVarStmt(it, newCall(bindSym"createJObject"))
    else:
      body.add newTree(nnkVarSection, newIdentDefs(it, rtyp))

  let fn = if q.retTypeIsJson: bindSym"bindResultJson" else: bindSym"bindResult"
  if q.retType.len > 1:
    var i = 0
    for r in q.retType:
      template resAt(x, i) {.dirty.} = x[i]
      let resx = if q.retTypeIsJson: it else: getAst(resAt(it, newLit(i)))

      body.add newCall(fn, ident"db", prepStmt, newLit(i),
                       resx, (if r.len > 0: r[1] else: r), newLit retName(q, i, body))
      inc i
  elif q.retType.len > 0:
    body.add newCall(fn, ident"db", prepStmt, newLit(0),
                     it, rtyp, newLit retName(q, 0, body))
  else:
    body.add newTree(nnkDiscardStmt, newEmptyNode())

  template ifStmt2(prepStmt, returnsData: bool; action) {.dirty.} =
    bind stepQuery
    bind stopQuery
    bind dbError
    if stepQuery(db, prepStmt, returnsData):
      action
      stopQuery(db, prepStmt)
    else:
      stopQuery(db, prepStmt)
      dbError(db)

  template ifStmt1(prepStmt, returnsData: bool; action) {.dirty.} =
    bind stepQuery
    bind stopQuery
    if stepQuery(db, prepStmt, returnsData):
      action
    stopQuery(db, prepStmt)

  template whileStmt(prepStmt, res, it, action) {.dirty.} =
    bind stepQuery
    bind stopQuery
    while stepQuery(db, prepStmt, true):
      action
      add res, it
    stopQuery(db, prepStmt)

  template insertQueryReturningId(prepStmt) {.dirty.} =
    bind stepQuery
    bind stopQuery
    bind getLastId
    bind dbError
    var insertedId = -1
    if stepQuery(db, prepStmt, false):
      insertedId = getLastId(db, prepStmt)
      stopQuery(db, prepStmt)
    else:
      stopQuery(db, prepStmt)
      dbError(db)
    insertedId

  let returnsData = q.kind in {qkSelect, qkJoin, qkInsertReturning}
  if not q.singleRow and q.retType.len > 0:
    blk.add getAst(whileStmt(prepStmt, res, it, body))
  elif dbBackend == DbBackend.sqlite and q.kind == qkInsertReturning and q.retExpr.kind != nnkEmpty:
    # For SQLite, emulate RETURNING by returning the inserted expression value.
    # Execute the insert as a non-row statement, then yield the expression.
    if attempt:
      blk.add getAst(ifStmt1(prepStmt, false, newStmtList()))
    else:
      blk.add getAst(ifStmt2(prepStmt, false, newStmtList()))
    blk.add q.retExpr
  elif q.returning.len > 0 and dbBackend == DbBackend.sqlite:
    blk.add getAst(insertQueryReturningId(prepStmt))
    # fix #14 delete not return value
  elif (q.kind == qkInsert or q.kind == qkUpdate or q.kind == qkDelete) and dbBackend == DbBackend.postgre:
    blk.add getAst(ifStmt1(prepStmt, returnsData, body))
  else:
    if attempt:
      blk.add getAst(ifStmt1(prepStmt, returnsData, body))
    else:
      blk.add getAst(ifStmt2(prepStmt, returnsData, body))

  result.add newTree(nnkBlockStmt, newEmptyNode(), blk)
  if q.retType.len > 0:
    result.add res

proc queryHookImpl(q: QueryBuilder; body: NimNode; attempt: bool; retType: NimNode): NimNode =
  expectKind body, nnkStmtList
  expectMinLen body, 1

  q.retTypeIsJson = false
  applyQueryNode(body, q)
  if q.kind notin {qkSelect, qkJoin}:
    macros.error "query(T) currently supports select/join queries only", body
  if q.retType.len == 0:
    macros.error "query(T) requires a query that returns data", body
  if q.retTypeIsJson:
    macros.error "query(T) does not support 'produce json'", body

  let sql = queryAsString(q, body)
  let prepStmt = genSym(nskLet)
  let res = genSym(nskVar)
  result = newTree(
    nnkStmtListExpr,
    newLetStmt(prepStmt, newCall(bindSym"prepareStmt", ident"db", newLit sql))
  )
  if q.singleRow:
    result.add newTree(nnkVarSection, newIdentDefs(res, copyNimTree(retType), newEmptyNode()))
  else:
    result.add newTree(nnkVarSection, newIdentDefs(res,
      newTree(nnkBracketExpr, bindSym"seq", copyNimTree(retType)),
      newTree(nnkPrefix, bindSym"@", newTree(nnkBracket))))

  let blk = newStmtList()
  var i = 1
  if q.params.len > 0:
    blk.add newCall(bindSym"startBindings", prepStmt, newLit(q.params.len))
    for p in q.params:
      blk.add buildHookedParamBinding(prepStmt, i, p.ex, p.typ, p.isJson)
      inc i
  blk.add newCall(bindSym"startQuery", ident"db", prepStmt)

  let action = buildQueryHookAction(q, prepStmt, res, retType, q.singleRow)

  if q.singleRow:
    if attempt:
      blk.add newTree(nnkIfStmt,
        newTree(nnkElifBranch,
          newCall(bindSym"stepQuery", ident"db", prepStmt, newLit true),
          action
        )
      )
      blk.add newCall(bindSym"stopQuery", ident"db", prepStmt)
    else:
      blk.add newTree(nnkIfStmt,
        newTree(nnkElifBranch,
          newCall(bindSym"stepQuery", ident"db", prepStmt, newLit true),
          newStmtList(action, newCall(bindSym"stopQuery", ident"db", prepStmt))
        ),
        newTree(nnkElse,
          newStmtList(
            newCall(bindSym"stopQuery", ident"db", prepStmt),
            newCall(bindSym"dbError", ident"db")
          )
        )
      )
  else:
    blk.add newTree(nnkWhileStmt,
      newCall(bindSym"stepQuery", ident"db", prepStmt, newLit true),
      action
    )
    blk.add newCall(bindSym"stopQuery", ident"db", prepStmt)

  result.add newTree(nnkBlockStmt, newEmptyNode(), blk)
  result.add res

macro query*(args: varargs[untyped]): untyped =
  if args.len == 1:
    let body = args[0]
    var q = newQueryBuilder()
    result = queryImpl(q, body, false, false)
    when defined(debugOrminDsl):
      macros.hint("Ormin Query: " & repr(result), body)
    return

  if args.len != 2:
    macros.error("query expects either `query: ...` or `query(T): ...`", args)

  let retType = args[0]
  let body = args[1]
  var q = newQueryBuilder()
  result = queryHookImpl(q, body, false, retType)
  when defined(debugOrminDsl):
    macros.hint("Ormin Query(T): " & repr(result), body)

macro tryQuery*(args: varargs[untyped]): untyped =
  if args.len == 1:
    let body = args[0]
    var q = newQueryBuilder()
    result = queryImpl(q, body, true, false)
    when defined(debugOrminDsl):
      macros.hint("Ormin TryQuery: " & repr(result), body)
    return

  if args.len != 2:
    macros.error("tryQuery expects either `tryQuery: ...` or `tryQuery(T): ...`", args)

  let retType = args[0]
  let body = args[1]
  var q = newQueryBuilder()
  result = queryHookImpl(q, body, true, retType)
  when defined(debugOrminDsl):
    macros.hint("Ormin TryQuery(T): " & repr(result), body)

# -------------------------
# Transactions DSL
# -------------------------

# Transaction state for nested transactions
var txDepth {.threadvar.}: int

proc getTxDepth*(): int = 
  result = txDepth

proc isTopTx*(): bool = 
  result = txDepth == 1

proc incTxDepth*() = 
  inc txDepth

proc decTxDepth*() = 
  dec txDepth

template txBegin*(sp: untyped) =
  if isTopTx():
    execNoRowsLoose("begin transaction")
  else:
    execNoRowsLoose("savepoint " & sp)

template txCommit*(sp: untyped) =
  if isTopTx():
    execNoRowsLoose("commit")
  else:
    execNoRowsLoose("release savepoint " & sp)

template txRollback*(sp: untyped) =
  if isTopTx():
    execNoRowsLoose("rollback")
  else:
    execNoRowsLoose("rollback to savepoint " & sp)

template transaction*(body: untyped) =
  ## Runs the body inside a database transaction. Commits on success,
  ## rolls back on any exception and rethrows. Supports nesting via savepoints.
  block:
    incTxDepth()
    let sp = "ormin_tx_" & $txDepth

    try:
      txBegin(sp)
      `body`
      txCommit(sp)
    except DbError:
      txRollback(sp)
      raise
    except CatchableError, Defect:
      txRollback(sp)
      raise
    finally:
      decTxDepth()

macro getBlock(blk: untyped): untyped =
  result = blk[0]

template transaction*(body, other: untyped) =
  ## Runs the body inside a database transaction. Commits on success,
  ## rolls back on any exception and rethrows. Supports nesting via savepoints.
  block:
    incTxDepth()
    let sp = "ormin_tx_" & $txDepth

    try:
      txBegin(sp)
      `body`
      txCommit(sp)
    except DbError:
      txRollback(sp)
      getBlock(`other`)
    except CatchableError, Defect:
      txRollback(sp)
      raise
    finally:
      decTxDepth()

proc createRoutine(name, query: NimNode; k: NimNodeKind): NimNode =
  expectKind query, nnkStmtList
  expectMinLen query, 1

  var q = newQueryBuilder()
  applyQueryNode(query, q)
  if q.kind notin {qkSelect, qkJoin}:
    macros.error "query must be a 'select' or 'join'", query
  let sql = queryAsString(q, query)
  result = generateRoutine(name, q, sql, k)
  when defined(debugOrminDsl):
    macros.hint("Ormin Query: " & repr(result), query)

macro createIter*(name, query: untyped): untyped =
  ## Creates an iterator of the given 'name' that iterates
  ## over the result set as described in 'query'.
  result = createRoutine(name, query, nnkIteratorDef)

macro createProc*(name, query: untyped): untyped =
  ## Creates an iterator of the given 'name' that iterates
  ## over the result set as described in 'query'.
  result = createRoutine(name, query, nnkProcDef)

type
  ProtoBuilder = ref object
    msgId: int
    dispClient, types, server, procs, retType: NimNode
    retNames: seq[string]
    foundObj, singleRow: bool
    sectionName: string

proc getTypename(n: NimNode): NimNode =
  result = n[0][0]
  if result.kind == nnkPostfix:
    result = result[1]

proc addFields(n: NimNode; b: ProtoBuilder): NimNode =
  if n.kind == nnkObjectTy and not b.foundObj:
    b.foundObj = true
    expectLen n, 3
    var x = n[2]
    if x.kind == nnkEmpty:
      x = newTree(nnkRecList)
      n[2] = x
    expectKind x, nnkRecList
    doAssert b.retType.len == b.retNames.len, "ormin: types and column names do not match"
    for i in 0 ..< b.retType.len:
      x.add newTree(nnkIdentDefs, newTree(nnkPostfix, ident"*", ident(b.retNames[i])),
                    b.retType[i][1], newEmptyNode())
    return n
  result = copyNimNode(n)
  for i in 0 ..< n.len:
    result.add addFields(n[i], b)

proc transformClient(n: NimNode; b: ProtoBuilder): NimNode =
  template sendReqImpl(x, msgkind): untyped {.dirty.} =
    let req = newJObject()
    req["cmd"] = %msgkind
    req["arg"] = cast[JsonNode](x)
    send(req)
  template sendReqImplNoArg(msgkind): untyped {.dirty.} =
    let req = newJObject()
    req["cmd"] = %msgkind
    req["arg"] = newJNull()
    send(req)
  if n.kind in nnkCallKinds and n[0].kind == nnkIdent and $n[0] == "recv":
    var castDest: NimNode
    if n.len == 2:
      castDest = n[1]
    else:
      expectLen n, 1
      # this can happen for the new 'returning' support:
      let retType = if b.retType.len != 0 or b.retType.kind != nnkTupleTy: b.retType else: ident"int"
      castDest = makeSeq(retType, b.singleRow)
    return newTree(nnkCast, castDest, ident"data")
  elif n.kind == nnkTypeSection:
    b.foundObj = false
    let t = addFields(n, b)
    b.retType = getTypename(n)
    b.types.add t
    return newTree(nnkNone)
  elif n.kind == nnkProcDef:
    let p = n.params
    if p.len == 1:
      n.body = getAst(sendReqImplNoArg(b.msgId))
    else:
      expectLen p, 2
      expectKind p[1], nnkIdentDefs
      if p[1].len != 3:
        macros.error "proc must have one or zero parameters", p
      n.body = getAst(sendReqImpl(p[1][0], b.msgId))
    b.procs.add n
    return newTree(nnkNone)
  elif n.kind in {nnkLetSection, nnkVarSection}:
    b.procs.add n
    return newTree(nnkNone)
  result = copyNimNode(n)
  for i in 0 ..< n.len:
    let x = transformClient(n[i], b)
    if x.kind != nnkNone: result.add x

proc transformServer(n: NimNode; b: ProtoBuilder): NimNode =
  template sendImpl(x, msgkind): untyped {.dirty.} =
    result = newJObject()
    result["cmd"] = %msgkind
    result["data"] = x

  template broadCastImpl(x, msgkind): untyped {.dirty.} =
    receivers = Receivers.all
    result = newJObject()
    result["cmd"] = %msgkind
    result["data"] = x

  if n.kind in nnkCallKinds and n[0].kind == nnkIdent:
    case $n[0]
    of "send":
      expectLen n, 2
      return getAst(sendImpl(n[1], b.msgId+1))
    of "broadcast":
      expectLen n, 2
      return getAst(broadCastImpl(n[1], b.msgId+1))
    of "query":
      expectLen n, 2
      var qb = newQueryBuilder()
      let q = queryImpl(qb, n[1], false, true)
      b.retType = qb.retType
      b.singleRow = qb.singleRow
      b.retNames = qb.retNames
      return q
    of "tryQuery":
      expectLen n, 2
      var qb = newQueryBuilder()
      let q = queryImpl(qb, n[1], true, true)
      b.retType = qb.retType
      b.singleRow = qb.singleRow
      b.retNames = qb.retNames
      return q

  result = copyNimNode(n)
  for i in 0 ..< n.len:
    result.add transformServer(n[i], b)

proc protoImpl(n: NimNode; b: ProtoBuilder): NimNode =
  case n.kind
  of nnkCallKinds:
    if n[0].kind == nnkIdent:
      let op = $n[0]
      case op
      of "server":
        expectLen n, 2, 3
        if n.len == 3:
          expectKind(n[1], nnkStrLit)
          b.sectionName = n[1].strVal
        return newTree(nnkOfBranch, newLit(b.msgId), transformServer(n[^1], b))
      of "client":
        expectLen n, 2, 3
        if n.len == 3:
          expectKind(n[1], nnkStrLit)
          if b.sectionName != n[1].strVal:
            macros.error "section names of client/server pair do not match", n[1]
        var clientPart = transformClient(n[^1], b)
        if clientPart.kind == nnkNone or (clientPart.kind == nnkStmtList and clientPart.len == 0):
          clientPart = newStmtList(newTree(nnkDiscardStmt, newEmptyNode()))
        b.dispClient.add newTree(nnkOfBranch, newLit(b.msgId+1), clientPart)
        inc b.msgId, 2
        return newTree(nnkNone)
      of "common":
        expectLen n, 2
        expectKind n[1], nnkStmtList
        for s in n[1]:
          b.types.add copyNimTree(s)
          b.server.add s
        return newTree(nnkNone)
  else: discard
  result = copyNimNode(n)
  for i in 0 ..< n.len:
    let x = protoImpl(n[i], b)
    if x.kind != nnkNone: result.add x

macro protocol*(name: static[string]; body: untyped): untyped =
  template serverProc(body) {.dirty.} =
    proc dispatch(inp: JsonNode; receivers: var Receivers): JsonNode =
      let arg = inp["arg"]
      let cmd = inp["cmd"].getInt()
      body

  template clientProc(body) {.dirty.} =
    proc recvMsg*(inp: JsonNode) =
      let data = inp["data"]
      let cmd = inp["cmd"].getInt()
      body

  var b = ProtoBuilder(msgId: 0, dispClient: newTree(nnkCaseStmt, ident"cmd"),
    types: newStmtList(), procs: newStmtList(), server: newStmtList())
  let branches = protoImpl(body, b)
  b.dispClient.add newTree(nnkElse, newStmtList(newTree(nnkDiscardStmt, newEmptyNode())))
  let disp = newTree(nnkCaseStmt, ident"cmd")
  for branch in branches: disp.add branch
  disp.add newTree(nnkElse, newStmtList(newTree(nnkDiscardStmt, newEmptyNode())))

  let client = getAst(clientProc(b.dispClient))
  var clientBody = newStmtList(newCommentStmtNode"Generated by Ormin. DO NOT EDIT!")
  for typ in b.types: clientBody.add typ
  for prc in b.procs: clientBody.add prc
  clientBody.add client
  for xx in clientBody:
    assert xx.kind != nnkStmtList
  writeFile(parentDir(instantiationInfo(-1, true)[0]) / name, repr clientBody)

  b.server.add getAst(serverProc(disp))
  result = b.server
  when defined(debugOrminDsl):
    macros.hint("Ormin Query: " & repr(result), body)
