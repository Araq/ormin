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
      let tab = $c[0]
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
      for i in 1..<c.len:
        let col = c[i]
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
  when dbBackend == DbBackend.postgre:
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


proc createInsertProc(tableName: string; cols: DbColumns): string =
  result = "proc insert" & capitalizeAscii(tableName) & "*(\L"
  var i = 0
  for c in cols:
    if i > 0: result.add ",\L"
    result.add "  "
    result.add c.name
    result.add ": "
    case c.typ.kind
    of dbInt:
      result.add c.typ.name
      if c.primaryKey: result.add " = autoIncrement"
    of dbVarchar:
      result.add "string = \"\""
    else:
      result.add c.typ.name
    inc i
  result.add "): int64 {.discardable.} = \L"
  result.add "  discard"


when false:

    iterator tokenize(s: string): string =
      const clump = {'A'..'Z', '_', 'a'..'z', '0'..'9', '\128'..'\255'}
      var i = 0
      var res = ""
      while i < s.len:
        res.setLen 0
        if s[i] in clump:
          while i < s.len and s[i] in clump:
            res.add s[i]
            inc i
        elif s[i] == '\'':
          while i < s.len:
            res.add s[i]
            if s[i] == '\'' and s[i+1] != '\'': break
            inc i
        else:
          res.add s[i]
          inc i
        yield res

    macro query(name, body: untyped): untyped =
      var params = newNimNode(nnkFormalParams)
      var retType = newNimNode(nnkPar)
      var b = if body.kind in {nnkStmtList, nnkStmtListExpr} and body.len == 1:
                body[0]
              else:
                body
      if b.kind notin {nnkStrLit..nnkTripleStrLit}:
        error "string literal as query expected", b
      else:
        let q = b.strVal
        var r = ""
        var i = 0
        let tokens = toSeq(tokenize(q))
        var isSelect
        while i < tokens.len:
          if tokens[i] == "select":
            inc i


          r.add tokens[i]
          inc i

      echo repr body

    query getUsers:
      """select a, b, c from x where ?cond"""
