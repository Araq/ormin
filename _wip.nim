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
