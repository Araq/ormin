## This module implements a little helper macro to ease
## writing the dispatching logic for JSON based servers.

import macros

proc tbody(n: NimNode): NimNode =
  # transforms::
  #   f fields(a, "b")
  # into::
  #   f  a = %args["a"], b = %args["b"]
  case n.kind
  of nnkCallKinds:
    result = copyNimNode(n)
    result.add tbody(n[0])
    for i in 1..<n.len:
      let it = n[i]
      if it.kind == nnkCall and $it[0] == "fields":
        for j in 1..<it.len:
          let field = it[j]
          let s = if field.kind in {nnkStrLit..nnkTripleStrLit}: field.strVal
                  else: $field
          let arg = newTree(nnkPrefix, ident"%",
            newTree(nnkBracketExpr, ident"arg", newLit(s)))
          result.add newTree(nnkExprEqExpr, ident(s), arg)
      else:
        result.add tbody(it)
  else:
    result = copyNimNode(n)
    for x in n: result.add tbody(x)

macro createDispatcher*(name, n: untyped): untyped =
  expectKind n, nnkStmtList
  result = newStmtList()
  let disp = newTree(nnkCaseStmt, ident"cmd")
  template cmdProc(name, body) {.dirty.} =
    proc name(arg: JsonNode): JsonNode = body
  template callCmd(name) {.dirty.} =
    result = name(arg)

  for x in n:
    if x.kind == nnkCall and x.len == 2 and
        x[0].kind == nnkIdent and x[1].kind == nnkStmtList:
      result.add getAst(cmdProc(x[0], tbody x[1]))
      disp.add newTree(nnkOfBranch, newLit($x[0]),
                       newStmtList(getAst(callCmd(x[0]))))
    else:
      # do not touch:
      result.add n
  disp.add newTree(nnkElse, newStmtList(newTree(nnkDiscardStmt, newEmptyNode())))

  template dispatchProc(name, body) {.dirty.} =
    proc dispatch(inp: JsonNode): JsonNode =
      let arg = inp["arg"]
      let cmd = inp["cmd"].getStr("")
      body
  result.add getAst(dispatchProc(name, disp))
  when defined(debugDispatcherDsl):
    echo repr result

when isMainModule:
  import json, db_sqlite

  createDispatcher(dispatch):
    insertCustomer:
      echo "insert customer", fields(a, b, c)
      result = arg
    selectCustomers:
      echo "select customer"
      result = arg

  echo dispatch(nil, %*{"cmd": "insertCustomer", "arg": [1, 2, 3]})
