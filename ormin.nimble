# Package

version       = "0.1.0"
author        = "Araq"
description   = "Prepared SQL statement generator. A lightweight ORM."
license       = "MIT"

# Dependencies

requires "nim >= 0.17.2"
requires "websocket >= 0.2.2"

bin = @["tools/ormin_importer"]

skipDirs = @["examples"]
installExt = @["nim"]

task test, "Run all test suite":
  selfExec "tcommon"
  selfExec "tfunction"
  selfExec "-d:postgre tcommon"
  selfExec "-d:postgre tfunction"
