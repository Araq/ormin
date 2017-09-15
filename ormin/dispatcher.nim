## This module implements a little helper macro to ease
## writing the dispatching logic for JSON based servers.

import macros

macro createDispatcher*(name, n: untyped): untyped =
  expectKind n, nnkStmtList
  result = newStmtList()
  let disp = newTree(nnkCaseStmt, ident"cmd")
  template cmdProc(name, body) {.dirty.} =
    proc name(db: DbConn; arg: JsonNode): JsonNode = body
  template callCmd(name) {.dirty.} =
    result = name(db, arg)

  for x in n:
    if x.kind == nnkCall and x.len == 2 and
        x[0].kind == nnkIdent and x[1].kind == nnkStmtList:
      result.add getAst(cmdProc(x[0], x[1]))
      disp.add newTree(nnkOfBranch, newLit($x[0]),
                       newStmtList(getAst(callCmd(x[0]))))
    else:
      # do not touch:
      result.add n
  disp.add newTree(nnkElse, newStmtList(newTree(nnkDiscardStmt, newEmptyNode())))

  template dispatchProc(name, body) {.dirty.} =
    proc dispatch*(db: DbConn; inp: JsonNode): JsonNode =
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
      echo "insert customer"
      result = arg
    selectCustomers:
      echo "select customer"
      result = arg

  echo dispatch(nil, %*{"cmd": "insertCustomer", "arg": [1, 2, 3]})
