
when not declared(tableNames):
  {.error: "The query DSL requires a tableNames const.".}

when not declared(attributes):
  {.error: "The query DSL requires a attributes const.".}

import macros, strutils, db_common

# SQL dialect specific things:
const
  equals = "="
  nequals = "<>"

type
  Function = object
    name: string
    arity: int # -1 for 'varargs'
    typ: DbTypeKind # if dbUnknown, use type of the last argument

const
  functions = [
    Function(name: "count", arity: 1, typ: dbInt),
    Function(name: "coalesce", arity: -1, typ: dbUnknown),
    Function(name: "min", arity: 1, typ: dbUnknown),
    Function(name: "max", arity: 1, typ: dbUnknown),
    Function(name: "avg", arity: 1, typ: dbFloat),
    Function(name: "sum", arity: 1, typ: dbUnknown),
    Function(name: "isnull", arity: 3, typ: dbUnknown),
    Function(name: "concat", arity: -1, typ: dbVarchar)
  ]

type
  Env = seq[int]

type
  QueryKind = enum
    qkNone,
    qkSelect,
    qkJoin,
    qkInsert,
    qkReplace,
    qkUpdate,
    qkDelete
  QueryBuilder = ref object
    head, fromm, join, values, where, groupby, having, orderby: string
    env: seq[int]
    kind: QueryKind
    params, retType: NimNode
    coln, qmark: int

proc newQueryBuilder(): QueryBuilder {.compileTime.} =
  QueryBuilder(head: "", fromm: "", join: "", values: "", where: "",
    groupby: "", having: "", orderby: "",
    env: @[], kind: qkNone, params: newNimNode(nnkFormalParams),
    retType: newNimNode(nnkPar), coln: 0, qmark: 0)

proc placeholder(q: QueryBuilder): string =
  when dbBackend == DbBackend.postgre:
    inc q.qmark
    result = "$" & $q.qmark
  else:
    result = "?"

proc lookup(table, attr: string; env: Env): DbType =
  var candidate = -1
  for i, m in attributes:
    if cmpIgnoreCase(m.name, attr) == 0 and m.tabIndex in env:
      if table.len == 0 or cmpIgnoreCase(tableNames[m.tabIndex], table) == 0:
        # ambiguous match?
        if candidate < 0: candidate = i
        else: return DbType(kind: dbUnknown)
  result = if candidate < 0: DbType(kind: dbUnknown)
           else: DbType(kind: attributes[candidate].typ)

proc autoJoin(join: var string, src, dest: int): bool =
  var srcCol = -1
  var destCol = -1
  for i, a in attributes:
    if a.tabIndex == src and a.key < 0 and attributes[-a.key - 1].tabIndex == dest:
      if srcCol < 0:
        srcCol = i
        destCol = -a.key - 1
      else:
        return false
  if srcCol >= 0:
    join.add " on "
    join.add attributes[srcCol].name
    join.add equals
    join.add attributes[destCol].name
    result = true

proc `$`(a: DbType): string = $a.kind

proc checkBool(a: DbType; n: NimNode) =
  if a.kind != dbBool:
    error "expected type 'bool', but got: " & $a, n

proc checkCompatible(a, b: DbType; n: NimNode) =
  if a.kind != b.kind:
    error "incompatible types: " & $a & " and " & $b, n

proc checkCompatibleSet(a, b: DbType; n: NimNode) =
  discard "too implement; might require a richer type system"

proc toNimType(t: DbType): NimNode {.compileTime.} =
  let name = ($t.kind).substr(2).toLowerAscii
  result = ident(name)

proc escIdent(dest: var string; src: string) =
  if allCharsInSet(src, {'\33'..'\127'}):
    dest.add(src)
  else:
    dest.add("\"" & replace(src, "\"", "\"\"") & "\"")

proc cond(n: NimNode; q: var string; params: NimNode;
          expected: DbType, qb: QueryBuilder): DbType =
  case n.kind
  of nnkIdent:
    let name = $n
    if name == "_":
      q.add "*"
      result = DbType(kind: dbUnknown)
    else:
      escIdent(q, name)
      result = lookup("", name, qb.env)
      if result.kind == dbUnknown:
        error "unknown column name: " & name, n
  of nnkDotExpr:
    let t = $n[0]
    let a = $n[0]
    escIdent(q, t)
    q.add '.'
    escIdent(q, a)
    result = lookup(t, a, qb.env)
  of nnkPar:
    if n.len == 1:
      q.add "("
      result = cond(n[0], q, params, expected, qb)
      q.add ")"
    else:
      error "tuple construction not allowed here", n
  of nnkCurly:
    q.add "("
    let a = cond(n[0], q, params, DbType(kind: dbUnknown), qb)
    for i in 1..<n.len:
      q.add ", "
      let b = cond(n[i], q, params, a, qb)
      checkCompatible(a, b, n[i])
    q.add ")"
    result = DbType(kind: dbSet)
  of nnkStrLit, nnkTripleStrLit:
    result = expected
    if result.kind == dbUnknown:
      error "cannot infer the type of the literal", n
    if result.kind == dbBlob:
      q.add(escape(n.strVal, "b'", "'"))
    else:
      q.add(escape(n.strVal, "'", "'"))
  of nnkRStrLit:
    result = expected
    if result.kind == dbUnknown:
      error "cannot infer the type of the literal", n
    q.add n.strVal
  of nnkIntLit..nnkInt64Lit:
    result = expected
    if result.kind == dbUnknown:
      result.kind = dbInt
    q.add $n.intVal
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
    of "<=", "<", ">=", ">", "==", "!=":
      let a = cond(n[1], q, params, DbType(kind: dbUnknown), qb)
      q.add ' '
      if op == "==": q.add equals
      elif op == "!=": q.add nequals
      else: q.add op
      q.add ' '
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
        let b = cond(n[2], q, params, DbType(kind: dbUnknown), qb)
        checkCompatibleSet a, b, n
      result = DbType(kind: dbBool)
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
    of "?":
      q.add placeHolder(qb)
      result = expected
      if result.kind == dbUnknown:
        error "cannot infer the type of the placeholder", n
      else:
        params.add newIdentDefs(n[1], toNimType(result))
    of "not":
      result = DbType(kind: dbBool)
      q.add "not "
      let a = cond(n[1], q, params, result, qb)
      checkBool a, n[1]
    else:
      # treat as arithmetic operator:
      q.add ' '
      q.add op
      q.add ' '
      result = cond(n[1], q, params, DbType(kind: dbUnknown), qb)
  of nnkCall:
    let op = $n[0]
    for f in functions:
      if f.name == op:
        if f.arity == n.len-1 or (f.arity == -1 and n.len > 1):
          q.add op
          q.add "("
          for i in 1..<n.len:
            result = cond(n[i], q, params, DbType(kind: dbUnknown), qb)
          if f.typ != dbUnknown: result.kind = f.typ
          q.add ")"
          return
        else:
          error "function " & op & " takes " & $f.arity & " arguments", n
    error "unknown function " & op
  of nnkCommand:
    # select subquery
    if n.len == 2 and $n[0] == "select" and n[1].kind == nnkCall:
      let call = n[1]
      let tab = $call[0]
      let tabIndex = tableNames.find(tab)
      if tabIndex < 0:
        error "unknown table name: " & tab, n[1][0]
      else:
        var subenv: seq[int] = @[tabIndex]
        swap(qb.env, subenv)
        var subselect = "select "
        for i in 1..<call.len:
          if i > 1: subselect.add ", "
          discard cond(call[i], subselect, params, DbType(kind: dbUnknown), qb)
        subselect.add " from "
        escIdent(subselect, tab)
        q.add subselect
        swap(qb.env, subenv)
    elif n.len == 2 and $n[0] == "select" and n[1].kind == nnkCommand:
      result = DbType(kind: dbSet)
      let cmd = n[1]
      var subselect = "select "
      var subenv: seq[int] = qb.env
      if cmd.len >= 1 and cmd[0].kind == nnkCall:
        let call = cmd[0]
        let tab = $call[0]
        let tabIndex = tableNames.find(tab)
        if tabIndex < 0:
          error "unknown table name: " & tab, n[1][0]
        else:
          qb.env = @[tabindex]
          for i in 1..<call.len:
            if i > 1: subselect.add ", "
            discard cond(call[i], subselect, params, DbType(kind: dbUnknown), qb)
          subselect.add " from "
          escIdent(subselect, tab)
      if cmd.len >= 2 and cmd[1].kind in nnkCallKinds and $cmd[1][0] == "where":
        subselect.add " where "
        discard cond(cmd[1][1], subselect, params, DbType(kind: dbBool), qb)
      qb.env = subenv
      q.add subselect
    else:
      error "construct not supported in condition: " & treeRepr n, n
  else:
    error "construct not supported in condition: " & treeRepr n, n

proc generateIter(name, params, retType: NimNode; q: string): NimNode =
  let prepStmt = ident($name & "PrepStmt")
  let prepare = newVarStmt(prepStmt, newCall(bindSym"prepareStmt", ident"db", newLit(q)))

  let body = newStmtList(
    newTree(nnkVarSection, newIdentDefs(ident"res", retType))
  )
  var finalParams = newNimNode(nnkFormalParams)
  finalParams.add retType
  finalParams.add newIdentDefs(ident"db", ident("DbConn"))
  var i = 1
  if params.len > 0:
    body.add newCall(bindSym"startBindings", newLit(params.len))
    for p in params:
      finalParams.add p
      body.add newCall(bindSym"bindParam", ident"db", prepStmt, newLit(i), p[0])
      inc i
  body.add newCall(bindSym"startQuery", ident"db", prepStmt)
  let yld = newStmtList()
  if retType.len > 1:
    var i = 0
    for r in retType:
      template resAt(res, i) = res[i]
      yld.add newCall(bindSym"bindResult", ident"db", prepStmt, newLit(i),
                      getAst(resAt(ident"res", i)))
      inc i
  else:
    yld.add newCall(bindSym"bindResult", ident"db", prepStmt, newLit(0), ident"res")
  yld.add newTree(nnkYieldStmt, ident"res")

  let whileStmt = newTree(nnkWhileStmt, newCall(bindSym"stepQuery", ident"db", prepStmt), yld)
  body.add whileStmt
  body.add newCall(bindSym"stopQuery", ident"db", prepStmt)

  let iter = newTree(nnkIteratorDef,
    name,
    newEmptyNode(),
    newEmptyNode(),
    finalParams,
    newEmptyNode(), # pragmas
    newEmptyNode(),
    body)
  result = newStmtList(prepare, iter)

proc tableSel(n: NimNode; q: QueryBuilder) =
  if n.kind == nnkCall and q.kind != qkDelete:
    let call = n
    let tab = $call[0]
    let tabIndex = tableNames.find(tab)
    if tabIndex < 0:
      error "unknown table name: " & tab, n
      return
    if q.kind == qkSelect:
      escIdent(q.fromm, tab)
    elif q.kind != qkJoin:
      escIdent(q.head, tab)
    if q.kind == qkUpdate: q.head.add " set "
    elif q.kind notin {qkSelect, qkJoin}: q.head.add "("

    q.env.add tabindex
    for i in 1..<call.len:
      let col = call[i]
      if col.kind == nnkExprEqExpr and q.kind in {qkInsert, qkUpdate, qkInsert}:
        let colname = $col[0]
        let coltype = lookup("", colname, q.env)
        if coltype.kind == dbUnknown:
          error "unkown column name: " & colname, col
        else:
          if q.coln > 0: q.head.add ", "
          escIdent(q.head, colname)
          #q.params.add newIdentDefs(col[1], toNimType(coltype))
          inc q.coln
        if q.kind == qkInsert:
          if q.values.len > 0: q.values.add ", "
          discard cond(col[1], q.values, q.params, coltype, q)
        else:
          q.head.add " = "
          discard cond(col[1], q.head, q.params, coltype, q)

      elif col.kind == nnkPrefix and $col[0] == "?":
        let colname = $col[1]
        let coltype = lookup("", colname, q.env)
        if coltype.kind == dbUnknown:
          error "unkown column name: " & colname, col
        else:
          if q.coln > 0: q.head.add ", "
          escIdent(q.head, colname)
          inc q.coln
          q.params.add newIdentDefs(col[1], toNimType(coltype))
        if q.kind == qkInsert:
          if q.values.len > 0: q.values.add ", "
          q.values.add placeholder(q)
        else:
          q.head.add " = "
          q.head.add placeholder(q)
      elif q.kind in {qkSelect, qkJoin}:
        if q.coln > 0: q.head.add ", "
        inc q.coln
        let t = cond(col, q.head, q.params, DbType(kind: dbUnknown), q)
        q.retType.add toNimType(t)
      else:
        error "unknown selector: " & repr(n), n
    if q.kind notin {qkUpdate, qkSelect, qkJoin}: q.head.add ")"
  elif n.kind in {nnkIdent, nnkAccQuoted, nnkSym} and q.kind == qkDelete:
    let tab = $n
    let tabIndex = tableNames.find(tab)
    if tabIndex < 0:
      error "unknown table name: " & tab, n
      return
    escIdent(q.head, tab)
    q.env.add tabindex
  elif n.kind == nnkRStrLit:
    q.head.add n.strVal
  else:
    error "unknown selector: " & repr(n), n


proc queryh(n: NimNode; q: QueryBuilder) =
  expectKind n, nnkCommand
  let kind = $n[0]
  case kind
  of "select":
    q.kind = qkSelect
    q.head = "select "
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
    let t = cond(n[1], q.where, q.params, DbType(kind: dbBool), q)
    checkBool(t, n)
  of "join", "innerjoin", "outerjoin":
    if kind == "outerjoin": q.join.add "\Louter join "
    else: q.join.add "\Linner join "
    expectLen n, 2
    let cmd = n[1]
    if cmd.kind == nnkCommand and cmd.len == 2 and
       cmd[1].kind == nnkCommand and cmd[1].len == 2 and $cmd[1][0] == "on" and
       cmd[0].kind == nnkCall:
      let tab = $cmd[0][0]
      let tabIndex = tableNames.find(tab)
      if tabIndex < 0:
        error "unknown table name: " & tab, n
      else:
        escIdent(q.join, tab)
        var oldEnv = q.env
        q.env = @[tabIndex]
        q.kind = qkJoin
        tableSel(cmd[0], q)
        swap q.env, oldEnv
      let onn = cmd[1][1]
      q.join.add " on "
      let t = cond(onn, q.join, q.params, DbType(kind: dbBool), q)
      checkBool(t, onn)
    elif cmd.kind == nnkCall:
      # auto join:
      let tab = $cmd[0]
      let tabIndex = tableNames.find(tab)
      if tabIndex < 0:
        error "unknown table name: " & tab, n
      else:
        escIdent(q.join, tab)
        if not autoJoin(q.join, q.env[^1], tabIndex):
          error "cannot compute auto join from: " & tableNames[q.env[^1]] & " to: " & tab, n
        var oldEnv = q.env
        q.env = @[tabIndex]
        q.kind = qkJoin
        tableSel(n[1], q)
        swap q.env, oldEnv
    else:
      error "unknown query component " & repr(n), n
  of "groupby":
    for i in 1..<n.len:
      discard cond(n[i], q.groupby, q.params, DbType(kind: dbUnknown), q)
  of "orderby":
    for i in 1..<n.len:
      discard cond(n[i], q.orderby, q.params, DbType(kind: dbUnknown), q)
  of "having":
    expectLen n, 2
    let t = cond(n[1], q.having, q.params, DbType(kind: dbBool), q)
    checkBool(t, n[1])
  else:
    error "unknown query component " & repr(n), n

proc queryAsString(q: QueryBuilder): string =
  result = q.head
  if q.fromm.len > 0:
    result.add "\Lfrom "
    result.add q.fromm
  if q.join.len > 0:
    result.add q.join
  if q.values.len > 0:
    result.add "\Lvalues ("
    result.add q.values
    result.add ")"
  if q.where.len > 0:
    result.add "\Lwhere "
    result.add q.where
  if q.groupby.len > 0:
    result.add "\Lgroup by "
    result.add q.groupby
  if q.having.len > 0:
    result.add "\Lhaving "
    result.add q.having
  if q.orderby.len > 0:
    result.add "\Lorder by "
    result.add q.orderby
  when false:
    if q.limit.len > 0:
      result.add "\Llimit "
      result.add q.limit
    if q.offset.len > 0:
      result.add "\Loffset "
      result.add q.offset

proc newGlobalVar(name, value: NimNode): NimNode =
  result = newTree(nnkVarSection,
    newTree(nnkIdentDefs, newTree(nnkPragmaExpr, name,
      newTree(nnkPragma, ident"global")), newEmptyNode(), value)
  )

proc queryImpl(body: NimNode; attempt: bool): NimNode =
  expectKind body, nnkStmtList
  expectMinLen body, 1

  var q = newQueryBuilder()
  for b in body:
    if b.kind == nnkCommand: queryh(b, q)
    else: error "illformed query", b
  let sql = queryAsString(q)
  let prepStmt = genSym(nskVar)
  let res = genSym(nskVar)
  result = newTree(if q.retType.len > 0: nnkStmtListExpr else: nnkStmtList,
    newGlobalVar(prepStmt, newCall(bindSym"prepareStmt", ident"db", newLit sql))
  )
  if q.retType.len > 0:
    result.add newTree(nnkVarSection, newIdentDefs(res, q.retType))
  let blk = newStmtList()
  var i = 1
  if q.params.len > 0:
    blk.add newCall(bindSym"startBindings", newLit(q.params.len))
    for p in q.params:
      blk.add newCall(bindSym"bindParam", ident"db", prepStmt, newLit(i), p[0])
      inc i
  blk.add newCall(bindSym"startQuery", ident"db", prepStmt)
  var body = newStmtList()
  if q.retType.len > 1:
    var i = 0
    for r in q.retType:
      template resAt(res, i) = res[i]
      body.add newCall(bindSym"bindResult", ident"db", prepStmt, newLit(i),
                         getAst(resAt(res, i)))
      inc i
  elif q.retType.len > 0:
    body.add newCall(bindSym"bindResult", ident"db", prepStmt, newLit(0), res)
  else:
    body.add newTree(nnkDiscardStmt, newEmptyNode())

  template ifStmt2(prepStmt, action) {.dirty.} =
    if stepQuery(db, prepStmt):
      action
    else:
      dbError(db)

  template ifStmt1(prepStmt, action) {.dirty.} =
    if stepQuery(db, prepStmt):
      action

  if attempt:
    blk.add getAst(ifStmt1(prepStmt, body))
  else:
    blk.add getAst(ifStmt2(prepStmt, body))
  blk.add newCall(bindSym"stopQuery", ident"db", prepStmt)
  result.add newTree(nnkBlockStmt, newEmptyNode(), blk)
  if q.retType.len > 0:
    result.add res
  when defined(debugOrminDsl):
    echo repr result

macro query*(body: untyped): untyped =
  result = queryImpl(body, false)

macro tryQuery*(body: untyped): untyped =
  result = queryImpl(body, true)

macro createIter*(name, query: untyped): untyped =
  ## Creates an iterator of the given 'name' that iterates
  ## over the result set as described in 'query'.
  expectKind query, nnkStmtList
  expectMinLen query, 1

  var q = newQueryBuilder()
  for b in query:
    if b.kind == nnkCommand: queryh(b, q)
    else: error "illformed query", b
  if q.kind != qkSelect:
    error "query for iterator must be a 'select'", query
  let sql = queryAsString(q)
  result = generateIter(name, q.params, q.retType, sql)
  when defined(debugOrminDsl):
    echo repr result
