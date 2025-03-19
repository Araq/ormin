# Package

version       = "0.2.0"
author        = "Araq"
description = "Prepared SQL statement generator. A lightweight ORM."
license       = "MIT"

# Dependencies
requires "nim >= 2.0.0"
requires "websocket >= 0.2.2"
requires "db_connector >= 0.1.0"

bin = @["tools/ormin_importer"]

skipDirs = @["examples"]
installExt = @["nim"]

task test, "Run all test suite":
  exec "nim c -r tests/tfeature"
  exec "nim c -r tests/tcommon"
  exec "nim c -r tests/tsqlite"
  # Skip PostgreSQL tests as they require a running PostgreSQL server
  # exec "nim c -r -d:postgre tests/tfeature"
  # exec "nim c -r -d:postgre tests/tcommon"
  # exec "nim c -r tests/tpostgre"

task buildexamples, "Build examples: chat and forum":
  selfExec "c examples/chat/server"
  selfExec "js examples/chat/frontend"
  selfExec "c examples/forum/forum"
  selfExec "c examples/forum/forumproto"
  selfExec "c examples/tweeter/src/tweeter"
