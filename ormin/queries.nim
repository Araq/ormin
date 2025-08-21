
when not declared(tableNames):
  {.error: "The query DSL requires a tableNames const.".}

when not declared(attributes):
  {.error: "The query DSL requires a attributes const.".}

import macros, strutils
import db_connector/db_common
from os import parentDir, `/`

# SQL dialect specific things:
const
  equals = "="
  nequals = "<>"

type
  Function* = object
    name: string
    arity: int # -1 for 'varargs'
    typ: DbTypeKind # if dbUnknown, use type of the last argument

var
  functions {.compileTime.} = @[
    Function(name: "count", arity: 1, typ: dbInt),
    Function(name: "coalesce", arity: -1, typ: dbUnknown),
    Function(name: "min", arity: 1, typ: dbUnknown),
    Function(name: "max", arity: 1, typ: dbUnknown),
    Function(name: "avg", arity: 1, typ: dbFloat),
    Function(name: "sum", arity: 1, typ: dbUnknown),
    Function(name: "isnull", arity: 3, typ: dbUnknown),
    Function(name: "concat", arity: -1, typ: dbVarchar),
    Function(name: "abs", arity: 1, typ: dbUnknown),
    Function(name: "length", arity: 1, typ: dbInt),
    Function(name: "lower", arity: 1, typ: dbVarchar),
    Function(name: "upper", arity: 1, typ: dbVarchar),
    Function(name: "replace", arity: 3, typ: dbVarchar)
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
    qkDelete,
    qkInsertReturning
  QueryBuilder = ref object
    head, fromm, join, values, where, groupby, having, orderby: string
    limit, offset, returning: string
    env: Env
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

proc newQueryBuilder(): QueryBuilder {.compileTime.} =
  QueryBuilder(head: "", fromm: "", join: "", values: "", where: "",
    groupby: "", having: "", orderby: "", limit: "", offset: "",
    returning: "",
    env: @[], kind: qkNone, params: @[],
    retType: newNimNode(nnkTupleTy), singleRow: false,
    retTypeIsJson: false, retNames: @[],
    coln: 0, qmark: 0, aliasGen: 1, colAliases: @[],
    insertedValues: @[], retExpr: newEmptyNode())

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
    error "expected type 'bool', but got: " & $a, n

proc checkInt(a: DbType; n: NimNode) =
  if a.kind != dbInt:
    error "expected type 'int', but got: " & $a, n

proc checkCompatible(a, b: DbType; n: NimNode) =
  if a.kind != b.kind:
    error "incompatible types: " & $a & " and " & $b, n

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
    result = lookup("", name, qb.env, alias)
    if result.kind == dbUnknown:
      error "unknown column name: " & name, n
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
    else:
      result = lookupColumnInEnv(n, q, params, expected, qb)
  of nnkDotExpr:
    let t = $n[0]
    let a = $n[1]
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
    let a = cond(n[0], q, params, expected, qb)
    for i in 1..<n.len:
      q.add ", "
      let b = cond(n[i], q, params, a, qb)
      checkCompatible(a, b, n[i])
    q.add ")"
    result = DbType(kind: dbSet)
  of nnkStrLit, nnkRStrLit, nnkTripleStrLit:
    result = expected
    if result.kind == dbUnknown:
      # error "cannot infer the type of the literal", n
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
      let tabIndex = tableNames.lookup(tab)
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
        let tabIndex = tableNames.lookup(tab)
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
      if cmd.len == 2:
        if cmd[1].kind in nnkCallKinds and $cmd[1][0] == "where":
          subselect.add " where "
          discard cond(cmd[1][1], subselect, params, DbType(kind: dbBool), qb)
        elif cmd[1].kind in nnkCallKinds and $cmd[1][0] == "groupby":
          subselect.add " group by "
          let hav = cmd[1][1]
          if hav.kind in nnkCallKinds and $hav[1][0] == "having":
            discard cond(hav[0], subselect, params, DbType(kind: dbBool), qb)
            subselect.add " having "
            discard cond(hav[1][1], subselect, params, DbType(kind: dbBool), qb)
          else:
            discard cond(hav, subselect, params, DbType(kind: dbBool), qb)
        else:
          error "construct not supported in condition: " & treeRepr cmd, cmd
      elif cmd.len >= 2:
        error "construct not supported in condition: " & treeRepr cmd, cmd
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
    var rtyp = if q.retType.len > 1:
      q.retType
    else:
      q.retType[0][1]
    body.add newTree(nnkVarSection, newIdentDefs(ident"res", rtyp))
    if k != nnkIteratorDef:
      rtyp = nnkBracketExpr.newTree(ident"seq", rtyp)
    finalParams.add rtyp
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
    # but we need to emit this error lazily:
    result = " " & repr(n)

proc retName(q: QueryBuilder; i: int; n: NimNode): string =
  result = q.retNames[i]
  if q.retTypeIsJson and result.startsWith(" "):
    error "cannot extract column name of:" & result, n
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
    for a in attributes:
      if a.tabIndex == tabIndex:
        if q.coln > 0: q.head.add ", "
        inc q.coln
        let t = a.typ
        q.retType.add nnkIdentDefs.newTree(newIdentNode(a.name), toNimType(t), newEmptyNode())
        q.retNames.add a.name
        doAssert q.env.len > 0
        q.head.add q.env[^1][1]
        q.head.add '.'
        escIdent(q.head, a.name)
  of qkInsert, qkInsertReturning, qkReplace:
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
    for a in attributes:
      if a.tabIndex == tabIndex and a.key != 1:
        if q.coln > 0: q.head.add ", "
        escIdent(q.head, a.name)
        inc q.coln
        q.params.add((ex: field(), typ: toNimType(a.typ), isJson: q.retTypeIsJson))
        q.head.add " = "
        q.head.add placeholder(q)
  else:
    error "select '_' not supported for this construct", lineInfo


proc tableSel(n: NimNode; q: QueryBuilder) =
  if n.kind == nnkCall and q.kind != qkDelete:
    let call = n
    let tab = $call[0]
    let tabIndex = tableNames.lookup(tab)
    if tabIndex < 0:
      error "tableSel: unknown table name: " & tab & " from: " & $tableNames, n
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
      if col.kind == nnkExprEqExpr and q.kind in {qkInsert, qkInsertReturning, qkUpdate, qkReplace}:
        let colname = $col[0]
        if colname == "_":
          selectAll(q, tabIndex, col[1], col)
        else:
          let coltype = lookup("", colname, q.env)
          if coltype.kind == dbUnknown:
            error "unkown column name: " & colname, col
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
        let coltype = lookup("", colname, q.env)
        if coltype.kind == dbUnknown:
          error "unkown column name: " & colname, col
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
        let t = cond(col, q.head, q.params, DbType(kind: dbUnknown), q)
        q.retType.add nnkIdentDefs.newTree(newIdentNode(getColumnName(col)), toNimType(t), newEmptyNode())
        q.retNames.add getColumnName(col)
      else:
        error "unknown selector: " & repr(n), n
    if q.kind notin {qkUpdate, qkSelect, qkJoin}: q.head.add ")"
  elif n.kind in {nnkIdent, nnkAccQuoted, nnkSym} and q.kind == qkDelete:
    let tab = $n
    let tabIndex = tableNames.lookup(tab)
    if tabIndex < 0:
      error "tableSel:del: unknown table name: " & tab, n
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
      let tabIndex = tableNames.lookup(tab)
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
        oldEnv = q.env
        q.env.add((tabIndex, alias))
        let t = cond(onn, q.join, q.params, DbType(kind: dbBool), q)
        swap q.env, oldEnv
        checkBool(t, onn)
    elif cmd.kind == nnkCall:
      # auto join:
      let tab = $cmd[0]
      let tabIndex = tableNames.lookup(tab)
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
  of "returning":
    if q.kind != qkInsert:
      error "'returning' only possible within 'insert'"
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

    # check if the column is inserted, if so, use the inserted expression
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
      error "produce expects 'json' or 'nim', but got: " & repr(n[1]), n
  else:
    error "unknown query component " & repr(n), n

proc queryAsString(q: QueryBuilder, n: NimNode): string =
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
  when dbBackend != DbBackend.sqlite:
    if q.returning.len > 0:
      result.add q.returning
  when defined(debugOrminSql):
    hint("Ormin SQL:\n" & $result, n)

proc newGlobalVar(name, typ: NimNode, value: NimNode): NimNode =
  result = newTree(nnkVarSection,
    newTree(nnkIdentDefs, newTree(nnkPragmaExpr, name,
      newTree(nnkPragma, ident"global")), typ, value)
  )

proc makeSeq(retType: NimNode; singleRow: bool): NimNode =
  if not singleRow:
    result = newTree(nnkBracketExpr, bindSym"seq", retType)
  else:
    result = retType

proc queryImpl(q: QueryBuilder; body: NimNode; attempt, produceJson: bool): NimNode =
  expectKind body, nnkStmtList
  expectMinLen body, 1

  q.retTypeIsJson = produceJson
  for b in body:
    if b.kind == nnkCommand: queryh(b, q)
    else: error "illformed query", b
  let sql = queryAsString(q, body)
  let prepStmt = genSym(nskVar)
  let res = genSym(nskVar)
  let prepStmtCall = newCall(bindSym"prepareStmt", ident"db", newLit sql)
  result = newTree(
    if q.retType.len > 0: nnkStmtListExpr else: nnkStmtList,
    # really hack-ish
    newGlobalVar(prepStmt, newCall(bindSym"typeof", prepStmtCall), newEmptyNode()),
    getAst(once(newAssignment(prepStmt, prepStmtCall)))
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
      let fn = if p.isJson: bindSym"bindParamJson" else: bindSym"bindParam"
      blk.add newCall(fn, ident"db", prepStmt, newLit(i), p.ex, p.typ)
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

macro query*(body: untyped): untyped =
  var q = newQueryBuilder()
  result = queryImpl(q, body, false, false)
  when defined(debugOrminDsl):
    hint("Ormin Query: " & repr(result), body)

macro tryQuery*(body: untyped): untyped =
  var q = newQueryBuilder()
  result = queryImpl(q, body, true, false)
  when defined(debugOrminDsl):
    hint("Ormin Query: " & repr(result), body)

proc createRoutine(name, query: NimNode; k: NimNodeKind): NimNode =
  expectKind query, nnkStmtList
  expectMinLen query, 1

  var q = newQueryBuilder()
  for b in query:
    if b.kind == nnkCommand: queryh(b, q)
    else: error "illformed query", b
  if q.kind notin {qkSelect, qkJoin}:
    error "query must be a 'select' or 'join'", query
  let sql = queryAsString(q, query)
  result = generateRoutine(name, q, sql, k)
  when defined(debugOrminDsl):
    hint("Ormin Query: " & repr(result), query)

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
        error "proc must have one or zero parameters", p
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
            error "section names of client/server pair do not match", n[1]
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
    hint("Ormin Query: " & repr(result), body)
