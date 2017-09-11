
import macros, strutils, db_common

proc getCols(n: NimNode): NimNode =
  if n.kind == nnkCall and n.len >= 2 and n[0].kind == nnkIdent:
    if n.len == 2 and n[1].kind == nnkStmtList and n[1].len == 1 and
        n[1][0].kind == nnkPar:
      result = n[1][0]

# SQL dialect specific things:
const
  placeHolder = "?"
  equals = "="
  nequals = "<>"

type
  DbBackend* {.pure.} = enum
    sqlite, postgre, mysql

  Attr = object
    name: string
    tabIndex: int
    typ: DbTypekind
    key: int   # 0 nothing special,
               # +1 -- primary key
               # -N -- references attribute N

  Function = object
    name: string
    arity: int # -1 for 'varargs'
    typ: DbTypeKind # if dbUnknown, use type of the last argument

const backend = DbBackend.postgre

const
  tableNames = ["tableA", "tableB"]
  attributes = [
    Attr(name: "a", tabIndex: 0, typ: dbInt, key: 1),
    Attr(name: "b", tabIndex: 0, typ: dbVarchar, key: 0),
    Attr(name: "c", tabIndex: 1, typ: dbInt, key: 1),
    Attr(name: "aref", tabIndex: 1, typ: dbInt, key: -1)
  ]

  functions = [
    Function(name: "count", arity: 1, typ: dbInt),
    Function(name: "coalesce", arity: -1, typ: dbUnknown),
    Function(name: "min", arity: 1, typ: dbUnknown),
    Function(name: "max", arity: 1, typ: dbUnknown),
    Function(name: "avg", arity: 1, typ: dbFloat),
    Function(name: "sum", arity: 1, typ: dbUnknown),
    Function(name: "isnull", arity: 3, typ: dbUnknown)
  ]

type
  Env = seq[int]

proc lookup(table, attr: string; env: Env): DbType =
  var candidate = -1
  for i, m in attributes:
    if m.name == attr and m.tabIndex in env:
      if table.len == 0 or tableNames[m.tabIndex] == table:
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
      else: return false
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
          expected: DbType, env: Env): DbType =
  case n.kind
  of nnkIdent:
    let name = $n
    if name == "_":
      q.add "*"
      result = DbType(kind: dbUnknown)
    else:
      escIdent(q, name)
      result = lookup("", name, env)
  of nnkDotExpr:
    let t = $n[0]
    let a = $n[0]
    escIdent(q, t)
    q.add '.'
    escIdent(q, a)
    result = lookup(t, a, env)
  of nnkPar:
    if n.len == 1:
      q.add "("
      result = cond(n[0], q, params, expected, env)
      q.add ")"
    else:
      error "tuple construction not allowed here", n
  of nnkCurly:
    q.add "("
    let a = cond(n[0], q, params, DbType(kind: dbUnknown), env)
    for i in 1..<n.len:
      q.add ", "
      let b = cond(n[i], q, params, a, env)
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
      let a = cond(n[1], q, params, result, env)
      checkBool a, n[1]
      q.add ' '
      q.add op
      q.add ' '
      let b = cond(n[2], q, params, result, env)
      checkBool b, n[2]
    of "<=", "<", ">=", ">", "==", "!=":
      let a = cond(n[1], q, params, DbType(kind: dbUnknown), env)
      q.add ' '
      if op == "==": q.add equals
      elif op == "!=": q.add nequals
      else: q.add op
      q.add ' '
      let b = cond(n[2], q, params, a, env)
      checkCompatible a, b, n
      result = DbType(kind: dbBool)
    of "in", "notin":
      let a = cond(n[1], q, params, DbType(kind: dbUnknown), env)
      if n[2].kind == nnkInfix and $n[2][0] == "..":
        if op == "in": q.add " between "
        else: q.add " not between "
        let r = n[2]
        let b = cond(r[1], q, params, a, env)
        checkCompatible a, b, n
        q.add " and "
        let c = cond(r[2], q, params, a, env)
        checkCompatible a, c, n
      else:
        q.add ' '
        q.add op
        q.add ' '
        let b = cond(n[2], q, params, DbType(kind: dbUnknown), env)
        checkCompatibleSet a, b, n
      result = DbType(kind: dbBool)
    else:
      # treat as arithmetic operator:
      result = cond(n[1], q, params, DbType(kind: dbUnknown), env)
      q.add ' '
      q.add op
      q.add ' '
      let b = cond(n[2], q, params, result, env)
      checkCompatible result, b, n

  of nnkPrefix:
    let op = $n[0]
    case op
    of "?":
      q.add placeHolder
      result = expected
      if result.kind == dbUnknown:
        error "cannot infer the type of the placeholder", n
      else:
        params.add newIdentDefs(n[1], toNimType(result))
    of "not":
      result = DbType(kind: dbBool)
      q.add "not "
      let a = cond(n[1], q, params, result, env)
      checkBool a, n[1]
    else:
      # treat as arithmetic operator:
      result = cond(n[1], q, params, DbType(kind: dbUnknown), env)
      q.add ' '
      q.add op
      q.add ' '
      let b = cond(n[2], q, params, result, env)
      checkCompatible result, b, n
  of nnkCall:
    let op = $n[0]
    for f in functions:
      if f.name == op:
        if f.arity == n.len-1 or (f.arity == -1 and n.len > 1):
          q.add op
          q.add "("
          for i in 1..<n.len:
            result = cond(n[i], q, params, DbType(kind: dbUnknown), env)
          if f.typ != dbUnknown: result.kind = f.typ
          q.add ")"
          return
        else:
          error "function " & op & " takes " & $f.arity & " arguments", n
    error "unknown function " & op
  of nnkCommand:
    # select subquery
    if n.len == 2 and $n[0] == "select" and n[1].kind == nnkCommand:
      result = DbType(kind: dbSet)
      let cmd = n[1]
      var subselect = "select "
      var subenv: seq[int] = @[]
      if cmd.len >= 1 and cmd[0].kind == nnkCall:
        let call = cmd[0]
        let tab = $call[0]
        let tabIndex = tableNames.find(tab)
        if tabIndex < 0:
          error "unknown table name: " & tab, n[1][0]
        else:
          subenv.add tabindex
          for i in 1..<call.len:
            let col = call[i]
            let colname = $col
            let coltype = lookup("", colname, subenv)
            if coltype.kind == dbUnknown:
              error "unkown column name: " & colname, col
            else:
              if i > 1: subselect.add ", "
              escIdent(subselect, colname)
          subselect.add " from "
          escIdent(subselect, tab)
      if cmd.len >= 2 and cmd[1].kind in nnkCallKinds and $cmd[1][0] == "where":
        subselect.add " where "
        discard cond(cmd[1][1], subselect, params, DbType(kind: dbBool), subenv)
      q.add subselect
    else:
      error "construct not supported in condition: " & treeRepr n, n
  else:
    error "construct not supported in condition: " & treeRepr n, n

proc selectImpl(n: NimNode; params, retType: NimNode): string =
  expectKind n, nnkStmtList
  var i = 0
  var j = 0
  var q = "select "
  var env: seq[int] = @[]
  var fromm, where, join, groupby, orderby, having, limit, offset = ""
  for x in n:
    let c = getCols(x)
    if c != nil:
      let tab = $x[0]
      let tabIndex = tableNames.find(tab)
      if tabIndex < 0:
        error "unknown table name: " & tab, x
      else:
        if env.len == 0:
          escIdent(fromm, tab)
        else:
          join.add "\ninner join "
          escIdent(join, tab)
          if not autoJoin(join, tabIndex, env[^1]):
            error "cannot compute auto join for table: " & tab, x
        env.add tabIndex
      let thisTable = @[tabIndex]
      for col in c:
        #let colname = $col
        if j > 0: q.add ", "
        let t = cond(col, q, params, DbType(kind: dbUnknown), thisTable)
        retType.add toNimType(t)
        when false:
          let coltype = lookup("", colname, thisTable)
          if coltype.kind == dbUnknown:
            error "unkown column name: " & colname, col
          else:
            if j > 0: q.add ", "
            escIdent(q, colname)
            retType.add toNimType(t)
        inc j

    elif x.kind in nnkCallKinds:
      let op = $x[0]
      case op
      of "join", "innerjoin":
        # XXX this does not work yet
        join.add "inner join"
        let t = cond(x[1], join, params, DbType(kind: dbBool), env)
        checkBool(t, x[1])
      of "where":
        expectLen x, 2
        let t = cond(x[1], where, params, DbType(kind: dbBool), env)
        checkBool(t, x[1])
      of "groupby":
        for i in 1..<x.len:
          discard cond(x[i], groupby, params, DbType(kind: dbUnknown), env)
      of "orderby":
        for i in 1..<x.len:
          discard cond(x[i], orderby, params, DbType(kind: dbUnknown), env)
      of "having":
        expectLen x, 2
        let t = cond(x[1], having, params, DbType(kind: dbBool), env)
        checkBool(t, x[1])
      else:
        error "invalid select section: " & op, x
    else:
      error "invalid select construct", x
    inc i
  result = q & "\nfrom " & fromm
  if join.len > 0:
    result.add join
  if where.len > 0:
    result.add "\nwhere "
    result.add where
  if groupby.len > 0:
    result.add "\ngroup by "
    result.add groupby
  if having.len > 0:
    result.add "\nhaving "
    result.add having
  if orderby.len > 0:
    result.add "\norder by "
    result.add orderby
  if limit.len > 0:
    result.add "\nlimit "
    result.add limit
  if offset.len > 0:
    result.add "\noffset "
    result.add offset

proc generateIter(name, params, retType: NimNode; q: string): NimNode =
  let prepStmt = ident($name & "PrepStmt")
  let prepare = newVarStmt(prepStmt, newCall("prepareStmt", ident"db", newLit(q)))

  let body = newStmtList(
    newTree(nnkVarSection, newIdentDefs(ident"res", retType))
  )
  var finalParams = newNimNode(nnkFormalParams)
  finalParams.add retType
  finalParams.add newIdentDefs(ident"db", ident("DbHandle"))
  for p in params:
    finalParams.add p
    body.add newCall("bindParam", ident"db", prepStmt, p[0])
  body.add newCall("startQuery", ident"db", prepStmt)
  let yld = newStmtList(newCall("stepQuery", ident"db", prepStmt))
  if retType.len > 1:
    var i = 0
    for r in retType:
      yld.add newCall("bindResult", ident"db", prepStmt, ident"res", newLit(i))
      inc i
  else:
    yld.add newCall("bindWholeResult", ident"db", prepStmt, ident"res")
  yld.add newTree(nnkYieldStmt, ident"res")

  let whileStmt = newTree(nnkWhileStmt, bindSym"true", yld)
  body.add whileStmt
  body.add newCall("stopQuery", ident"db", prepStmt)

  let iter = newTree(nnkIteratorDef,
    name,
    newEmptyNode(),
    newEmptyNode(),
    finalParams,
    newEmptyNode(), # pragmas
    newEmptyNode(),
    body)
  result = newStmtList(prepare, iter)
  when defined(debugOrminDsl):
    echo repr result

macro rowsq*(name, body: untyped): untyped =
  ## Creates an iterator of the given 'name' that iterates
  ## over the result set as described in 'body'.
  var params = newNimNode(nnkFormalParams)
  var retType = newNimNode(nnkPar)
  let q = selectImpl(body, params, retType)
  result = generateIter(name, params, retType, q)

proc generateCmd(name, params, retType: NimNode; q: string): NimNode =
  let prepStmt = ident($name & "PrepStmt")
  let prepare = newVarStmt(prepStmt, newCall("prepareStmt", ident"db", newLit(q)))

  let body = newStmtList()
  var finalParams = newNimNode(nnkFormalParams)
  if retType.len > 0:
    finalParams.add retType
  else:
    finalParams.add newEmptyNode()
  finalParams.add newIdentDefs(ident"db", ident("DbHandle"))
  for p in params:
    finalParams.add p
    body.add newCall("bindParam", ident"db", prepStmt, p[0])
  body.add newCall("execQuery", ident"db", prepStmt)
  if retType.len > 1:
    var i = 0
    for r in retType:
      body.add newCall("bindResult", ident"db", prepStmt, ident"result", newLit(i))
      inc i
  elif retType.len > 0:
    body.add newCall("bindWholeResult", ident"db", prepStmt, ident"result")
  let prc = newTree(nnkProcDef,
    name,
    newEmptyNode(),
    newEmptyNode(),
    finalParams,
    newEmptyNode(), # pragmas
    newEmptyNode(),
    body)
  result = newStmtList(prepare, prc)
  when defined(debugOrminDsl):
    echo repr result

proc insertOrReplace(name, body: NimNode; startTok: string): NimNode =
  var params = newNimNode(nnkFormalParams)
  var retType = newNimNode(nnkPar)

  expectKind body, nnkStmtList
  expectMinLen body, 1

  var env: seq[int] = @[]
  var q = startTok
  var values = ""
  var output = ""
  for b in body:
    if b.kind == nnkCall:
      let call = b
      let tab = $call[0]
      let tabIndex = tableNames.find(tab)
      if tabIndex < 0:
        error "unknown table name: " & tab, b
        return
      escIdent(q, tab)
      q.add "("

      env.add tabindex
      for i in 1..<call.len:
        let col = call[i]
        if col.kind == nnkExprEqExpr:
          let colname = $col[0]
          let coltype = lookup("", colname, env)
          if coltype.kind == dbUnknown:
            error "unkown column name: " & colname, col
          else:
            if params.len > 0: q.add ", "
            escIdent(q, colname)
            params.add newIdentDefs(col[1], toNimType(coltype))
          if values.len > 0: values.add ", "
          discard cond(col[1], values, params, coltype, env)

        elif col.kind == nnkPrefix and $col[0] == "?":
          let colname = $col[1]
          let coltype = lookup("", colname, env)
          if coltype.kind == dbUnknown:
            error "unkown column name: " & colname, col
          else:
            if params.len > 0: q.add ", "
            escIdent(q, colname)
            params.add newIdentDefs(col[1], toNimType(coltype))

          if values.len > 0: values.add ", "
          values.add "?"
        else:
          error "illformed insert statement", col

    elif b.kind == nnkCommand and $b[0] == "output":
      for i in 1..<b.len:
        let t = cond(b[i], output, params, DbType(kind: dbUnknown), env)
        retType.add toNimType(t)
    else:
      error "illformed insert statement", b

  if values.len > 0:
    q.add ")\n values ("
    q.add values
  q.add ")"
  # XXX This is postgre specific! Some DBs only allow this with an
  # API call. Maybe 'output' is just a bad idea?
  if output.len > 0:
    q.add "\nreturning "
    q.add output
  result = generateCmd(name, params, retType, q)

proc updateOrDelete(name, body: NimNode; startTok: string): NimNode =
  var params = newNimNode(nnkFormalParams)
  var retType = newNimNode(nnkPar)

  expectKind body, nnkStmtList
  expectMinLen body, 1

  var env: seq[int] = @[]
  var q = startTok
  var values = ""
  var where = ""
  for b in body:
    if b.kind in {nnkIdent, nnkAccQuoted, nnkSym} and startTok[0] == 'd':
      let tab = $b
      let tabIndex = tableNames.find(tab)
      if tabIndex < 0:
        error "unknown table name: " & tab, b
        return
      escIdent(q, tab)
      env.add tabindex
    elif b.kind == nnkCall and startTok[0] == 'u':
      let call = b
      let tab = $call[0]
      let tabIndex = tableNames.find(tab)
      if tabIndex < 0:
        error "unknown table name: " & tab, b
        return
      escIdent(q, tab)
      q.add " set "

      env.add tabindex
      for i in 1..<call.len:
        let col = call[i]
        if i > 1: q.add ", "
        if col.kind == nnkExprEqExpr:
          let colname = $col[0]
          let coltype = lookup("", colname, env)
          if coltype.kind == dbUnknown:
            error "unkown column name: " & colname, col
          else:
            escIdent(q, colname)
            q.add " = "
          discard cond(col[1], q, params, coltype, env)

        elif col.kind == nnkPrefix and $col[0] == "?":
          let colname = $col[1]
          let coltype = lookup("", colname, env)
          if coltype.kind == dbUnknown:
            error "unkown column name: " & colname, col
          else:
            escIdent(q, colname)
            params.add newIdentDefs(col[1], toNimType(coltype))
            q.add " = ?"
        else:
          error "illformed statement", col

    elif b.kind == nnkCommand and $b[0] == "where":
      expectLen b, 2
      discard cond(b[1], where, params, DbType(kind: dbBool), env)
    else:
      error "illformed statement", b

  if where.len > 0:
    q.add "\nwhere "
    q.add where
  result = generateCmd(name, params, retType, q)

macro insertq*(name, body: untyped): untyped =
  ## Example usage:
  ##
  ## .. code-block:: nim
  ##  insertq createUser:
  ##    user(?column, columnB = "value")
  ##    output id
  result = insertOrReplace(name, body, "insert into ")

macro replaceq*(name, body: untyped): untyped =
  ## Example usage:
  ##
  ## .. code-block:: nim
  ##  replaceq createOrUpdateUser:
  ##    user(?column, columnB = "value")
  when backend == DbBackend.postgre:
    error "replaceq for PostGre not yet implemented", name
    result = insertOrReplace(name, body, "replace into ")
  else:
    result = insertOrReplace(name, body, "replace into ")

macro updateq*(name, body: untyped): untyped =
  ## Example usage:
  ##
  ## .. code-block:: nim
  ##  updateq createUser:
  ##    user(?column, columnB = "value")
  ##    where id = 8
  result = updateOrDelete(name, body, "update ")

macro deleteq*(name, body: untyped): untyped =
  ## Example usage:
  ##
  ## .. code-block:: nim
  ##  updateq destroyUser:
  ##    user
  ##    where id = 8
  result = updateOrDelete(name, body, "delete from ")


when isMainModule:
  deleteq deleteUser:
    tableA
    where a == 4

  updateq modUser:
    tableA(?b)
    where a == 5

  insertq insertUser:
    tableA(?a, ?b)
    output a

  #declUpsert nameHere:
  #  user(id = ?id,  )
  #  where ...

  select q:
    tableA: (b, count(_))
    tableB: (c)
    where a == ?id or b in (select tableB(c) where aref == 9) and
      count(_) > 100
