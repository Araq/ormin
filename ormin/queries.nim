
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
    Function(name: "concat", arity: -1, typ: dbVarchar),
    Function(name: "asc", arity: 1, typ: dbUnknown),
    Function(name: "desc", arity: 1, typ: dbUnknown)
  ]

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
    qkDelete
  QueryBuilder = ref object
    head, fromm, join, values, where, groupby, having, orderby: string
    limit, offset: string
    env: Env
    kind: QueryKind
    retType: NimNode
    singleRow, retTypeIsJson: bool
    retNames: seq[string]
    params: Params
    coln, qmark, aliasGen: int
    colAliases: seq[(string, DbType)]

proc newQueryBuilder(): QueryBuilder {.compileTime.} =
  QueryBuilder(head: "", fromm: "", join: "", values: "", where: "",
    groupby: "", having: "", orderby: "", limit: "", offset: "",
    env: @[], kind: qkNone, params: @[],
    retType: newNimNode(nnkPar), singleRow: false,
    retTypeIsJson: false, retNames: @[],
    coln: 0, qmark: 0, aliasGen: 1, colAliases: @[])

proc getAlias(q: QueryBuilder; tabIndex: int): string =
  result = tableNames[tabIndex][0] & $q.aliasGen
  inc q.aliasGen

proc placeholder(q: QueryBuilder): string =
  when dbBackend == DbBackend.postgre:
    inc q.qmark
    result = "$" & $q.qmark
  else:
    result = "?"

proc lookup(table, attr: string; env: Env; alias: var string): DbType =
  var candidate = -1
  for i, m in attributes:
    if cmpIgnoreCase(m.name, attr) == 0:
      var inScope = false
      for e in env:
        if e[0] == m.tabIndex:
          alias = e[1]
          inScope = true
          break
      if (inScope and table.len == 0) or
          cmpIgnoreCase(tableNames[m.tabIndex], table) == 0:
        # ambiguous match?
        if candidate < 0: candidate = i
        else: return DbType(kind: dbUnknown)
  result = if candidate < 0: DbType(kind: dbUnknown)
           else: DbType(kind: attributes[candidate].typ)

proc lookup(table, attr: string; env: Env): DbType =
  var alias: string
  result = lookup(table, attr, env, alias)

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
    error "expected type 'bool', but got: " & $a, n

proc checkInt(a: DbType; n: NimNode) =
  if a.kind != dbInt:
    error "expected type 'int', but got: " & $a, n

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

proc cond(n: NimNode; q: var string; params: var Params;
          expected: DbType, qb: QueryBuilder): DbType =
  case n.kind
  of nnkIdent:
    let name = $n
    if name == "_":
      q.add "*"
      result = DbType(kind: dbUnknown)
    else:
      block checkAliases:
        for a in qb.colAliases:
          if cmpIgnoreCase(a[0], name) == 0:
            result = a[1]
            break checkAliases
        var alias: string
        result = lookup("", name, qb.env, alias)
        if result.kind == dbUnknown:
          error "unknown column name: " & name, n
        elif qb.kind in {qkSelect, qkJoin}:
          doAssert alias.len >= 0
          q.add alias
          q.add '.'
      escIdent(q, name)
  of nnkDotExpr:
    let t = $n[0]
    let a = $n[0]
    escIdent(q, t)
    q.add '.'
    escIdent(q, a)
    result = lookup(t, a, qb.env)
  of nnkPar, nnkStmtListExpr:
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
  of nnkStrLit, nnkRStrLit, nnkTripleStrLit:
    result = expected
    if result.kind == dbUnknown:
      error "cannot infer the type of the literal", n
    if result.kind == dbBlob:
      q.add(escape(n.strVal, "b'", "'"))
    else:
      q.add(escape(n.strVal, "'", "'"))
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
    of "as":
      result = cond(n[1], q, params, expected, qb)
      q.add " as "
      expectKind n[2], nnkIdent
      let alias = $n[2]
      escIdent(q, alias)
      qb.colAliases.add((alias, result))
      if expected.kind != dbUnknown:
        checkCompatible result, expected, n
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
        error "cannot infer the type of the placeholder", n
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
        error "cannot infer the type of the literal", n
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
      else: error "illformed if expression", n
      q.add "\L    "
      let t = cond(x[^1], q, params, result, qb)
      if result.kind == dbUnknown: result = t
      else: checkCompatible(result, t, x)
    q.add "\Lend"
  of nnkCommand:
    # select subquery
    if n.len == 2 and $n[0] == "select" and n[1].kind == nnkCall:
      let call = n[1]
      let tab = $call[0]
      let tabIndex = tableNames.find(tab)
      if tabIndex < 0:
        error "unknown table name: " & tab, n[1][0]
      else:
        let alias = qb.getAlias(tabIndex)
        var subenv = @[(tabIndex, alias)]
        swap(qb.env, subenv)
        var subselect = "select "
        for i in 1..<call.len:
          if i > 1: subselect.add ", "
          discard cond(call[i], subselect, params, DbType(kind: dbUnknown), qb)
        subselect.add " from "
        escIdent(subselect, tab)
        subselect.add " as " & alias
        q.add subselect
        swap(qb.env, subenv)
    elif n.len == 2 and $n[0] == "select" and n[1].kind == nnkCommand:
      result = DbType(kind: dbSet)
      let cmd = n[1]
      var subselect = "select "
      var subenv = qb.env
      if cmd.len >= 1 and cmd[0].kind == nnkCall:
        let call = cmd[0]
        let tab = $call[0]
        let tabIndex = tableNames.find(tab)
        if tabIndex < 0:
          error "unknown table name: " & tab, n[1][0]
        else:
          let alias = qb.getAlias(tabIndex)
          qb.env = @[(tabindex, alias)]
          for i in 1..<call.len:
            if i > 1: subselect.add ", "
            discard cond(call[i], subselect, params, DbType(kind: dbUnknown), qb)
          subselect.add " from "
          escIdent(subselect, tab)
          subselect.add " as " & alias
      if cmd.len >= 2 and cmd[1].kind in nnkCallKinds and $cmd[1][0] == "where":
        subselect.add " where "
        discard cond(cmd[1][1], subselect, params, DbType(kind: dbBool), qb)
      qb.env = subenv
      q.add subselect
    else:
      error "construct not supported in condition: " & treeRepr n, n
  else:
    error "construct not supported in condition: " & treeRepr n, n

proc generateRoutine(name: NimNode, q: QueryBuilder;
                     sql: string; k: NimNodeKind): NimNode =
  let prepStmt = ident($name & "PrepStmt")
  let prepare = newVarStmt(prepStmt, newCall(bindSym"prepareStmt", ident"db", newLit(sql)))

  let body = newStmtList()

  var finalParams = newNimNode(nnkFormalParams)
  if q.retTypeIsJson:
    if k == nnkIteratorDef:
      body.add newVarStmt(ident"res", newCall(bindSym"createJObject"))
    else:
      body.add newAssignment(ident"result", newCall(bindSym"createJArray"))
    finalParams.add ident"JsonNode"
  else:
    body.add newTree(nnkVarSection, newIdentDefs(ident"res", q.retType))
    finalParams.add q.retType
  finalParams.add newIdentDefs(ident"db", ident("DbConn"))
  var i = 1
  if q.params.len > 0:
    body.add newCall(bindSym"startBindings", prepStmt, newLit(q.params.len))
    for p in q.params:
      if p.isJson:
        finalParams.add newIdentDefs(p.ex, ident"JsonNode")
        body.add newCall(bindSym"bindParamJson", ident"db", prepStmt, newLit(i), p.ex, p.typ)
      else:
        finalParams.add newIdentDefs(p.ex, p.typ)
        body.add newCall(bindSym"bindParam", ident"db", prepStmt, newLit(i), p.ex, p.typ)
      inc i
  body.add newCall(bindSym"startQuery", ident"db", prepStmt)
  let yld = newStmtList()
  if k != nnkIteratorDef:
    yld.add newVarStmt(ident"res", newCall(bindSym"createJObject"))
  let fn = if q.retTypeIsJson: bindSym"bindResultJson" else: bindSym"bindResult"
  if q.retType.len > 1:
    var i = 0
    for r in q.retType:
      template resAt(i) {.dirty.} = res[i]
      let resx = if q.retTypeIsJson: ident"res" else: getAst(resAt(newLit(i)))
      yld.add newCall(fn, ident"db", prepStmt, newLit(i),
                      resx, copyNimTree r, newLit q.retNames[i])
      inc i
  else:
    yld.add newCall(fn, ident"db", prepStmt, newLit(0), ident"res",
                    copyNimTree q.retType, newLit q.retNames[0])
  if k == nnkIteratorDef:
    yld.add newTree(nnkYieldStmt, ident"res")
  else:
    yld.add newCall("add", ident"result", ident"res")

  let whileStmt = newTree(nnkWhileStmt,
    newCall(bindSym"stepQuery", ident"db", prepStmt, newLit 1), yld)
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
    # but we need to emit this error lazily:
    result = " " & repr(n)

proc retName(q: QueryBuilder; i: int; n: NimNode): string =
  result = q.retNames[i]
  if q.retTypeIsJson and result.startsWith(" "):
    error "cannot extract column name of:" & result, n
    result = ""

proc tableSel(n: NimNode; q: QueryBuilder) =
  if n.kind == nnkCall and q.kind != qkDelete:
    let call = n
    let tab = $call[0]
    let tabIndex = tableNames.find(tab)
    if tabIndex < 0:
      error "unknown table name: " & tab, n
      return
    let alias = q.getAlias(tabIndex)
    if q.kind == qkSelect:
      escIdent(q.fromm, tab)
      q.fromm.add " as " & alias
    elif q.kind != qkJoin:
      escIdent(q.head, tab)
    if q.kind == qkUpdate: q.head.add " set "
    elif q.kind notin {qkSelect, qkJoin}: q.head.add "("

    q.env.add((tabindex, alias))
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

      elif col.kind == nnkPrefix and (let op = $col[0]; op == "?" or op == "%"):
        let colname = $col[1]
        let coltype = lookup("", colname, q.env)
        if coltype.kind == dbUnknown:
          error "unkown column name: " & colname, col
        else:
          if q.coln > 0: q.head.add ", "
          escIdent(q.head, colname)
          inc q.coln
          q.params.add((ex: col[1], typ: toNimType(coltype), isJson: op == "%"))
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
        q.retNames.add getColumnName(col)
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
    q.env.add((tabindex, q.getAlias(tabIndex)))
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
        let alias = q.getAlias(tabIndex)
        q.join.add " as " & alias
        var oldEnv = q.env
        q.env = @[(tabIndex, alias)]
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
        let alias = q.getAlias(tabIndex)
        escIdent(q.join, tab)
        q.join.add " as " & alias
        if not autoJoin(q.join, q.env[^1], tabIndex, alias):
          error "cannot compute auto join from: " & tableNames[q.env[^1][0]] & " to: " & tab, n
        var oldEnv = q.env
        q.env = @[(tabIndex, alias)]
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
  of "produce":
    expectLen n, 2
    if eqIdent(n[1], "json"):
      q.retTypeIsJson = true
    elif eqIdent(n[1], "nim"):
      q.retTypeIsJson = false
    else:
      error "produce expects 'json' or 'nim', but got: " & repr(n[1]), n
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
  if q.limit.len > 0:
    result.add "\Llimit "
    result.add q.limit
  if q.offset.len > 0:
    result.add "\Loffset "
    result.add q.offset
  when defined(debugOrminSql):
    echo "\n", result

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
    if q.singleRow:
      if q.retTypeIsJson:
        result.add newVarStmt(res, newCall(bindSym"createJObject"))
      else:
        result.add newTree(nnkVarSection, newIdentDefs(res, q.retType))
    else:
      if q.retTypeIsJson:
        result.add newVarStmt(res, newCall(bindSym"createJArray"))
      else:
        result.add newTree(nnkVarSection, newIdentDefs(res,
          newTree(nnkBracketExpr, bindSym"seq", q.retType),
          newTree(nnkPrefix, bindSym"@", newTree(nnkBracket))))
  let blk = newStmtList()
  var i = 1
  if q.params.len > 0:
    blk.add newCall(bindSym"startBindings", prepStmt, newLit(q.params.len))
    for p in q.params:
      let fn = if p.isJson: bindSym"bindParamJson" else: bindSym"bindParam"
      blk.add newCall(fn, ident"db", prepStmt, newLit(i), p.ex, p.typ)
      inc i
  blk.add newCall(bindSym"startQuery", ident"db", prepStmt)
  var body = newStmtList()
  let it = if q.singleRow: res else: genSym(nskVar)
  if not q.singleRow and q.retType.len > 0:
    if q.retTypeIsJson:
      result.add newVarStmt(it, newCall(bindSym"createJObject"))
    else:
      result.add newTree(nnkVarSection, newIdentDefs(it, q.retType))

  let fn = if q.retTypeIsJson: bindSym"bindResultJson" else: bindSym"bindResult"
  if q.retType.len > 1:
    var i = 0
    for r in q.retType:
      template resAt(x, i) {.dirty.} = x[i]
      let resx = if q.retTypeIsJson: it else: getAst(resAt(it, newLit(i)))

      body.add newCall(fn, ident"db", prepStmt, newLit(i),
                       resx, r, newLit retName(q, i, body))
      inc i
  elif q.retType.len > 0:
    body.add newCall(fn, ident"db", prepStmt, newLit(0),
                     it, q.retType, newLit retName(q, 0, body))
  else:
    body.add newTree(nnkDiscardStmt, newEmptyNode())

  template ifStmt2(prepStmt, returnsData, action) {.dirty.} =
    if stepQuery(db, prepStmt, returnsData):
      action
      stopQuery(db, prepStmt)
    else:
      stopQuery(db, prepStmt)
      dbError(db)

  template ifStmt1(prepStmt, returnsData, action) {.dirty.} =
    if stepQuery(db, prepStmt, returnsData):
      action
    stopQuery(db, prepStmt)

  template whileStmt(prepStmt, res, it, action) {.dirty.} =
    while stepQuery(db, prepStmt, 1):
      action
      add res, it
    stopQuery(db, prepStmt)

  let returnsData = q.kind in {qkSelect, qkJoin}
  if not q.singleRow and q.retType.len > 0:
    blk.add getAst(whileStmt(prepStmt, res, it, body))
  else:
    if attempt:
      blk.add getAst(ifStmt1(prepStmt, returnsData, body))
    else:
      blk.add getAst(ifStmt2(prepStmt, returnsData, body))

  result.add newTree(nnkBlockStmt, newEmptyNode(), blk)
  if q.retType.len > 0:
    result.add res
  when defined(debugOrminDsl):
    echo repr result

macro query*(body: untyped): untyped =
  result = queryImpl(body, false)

macro tryQuery*(body: untyped): untyped =
  result = queryImpl(body, true)

proc createRoutine(name, query: NimNode; k: NimNodeKind): NimNode =
  expectKind query, nnkStmtList
  expectMinLen query, 1

  var q = newQueryBuilder()
  for b in query:
    if b.kind == nnkCommand: queryh(b, q)
    else: error "illformed query", b
  if q.kind != qkSelect:
    error "query must be a 'select'", query
  let sql = queryAsString(q)
  result = generateRoutine(name, q, sql, k)
  when defined(debugOrminDsl):
    echo repr result

macro createIter*(name, query: untyped): untyped =
  ## Creates an iterator of the given 'name' that iterates
  ## over the result set as described in 'query'.
  result = createRoutine(name, query, nnkIteratorDef)

macro createProc*(name, query: untyped): untyped =
  ## Creates an iterator of the given 'name' that iterates
  ## over the result set as described in 'query'.
  result = createRoutine(name, query, nnkProcDef)
