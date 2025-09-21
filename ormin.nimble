# Package

version       = "0.5.0"
author        = "Araq"
description = "Prepared SQL statement generator. A lightweight ORM."
license       = "MIT"
bin = @["tools/ormin_importer"]
skipDirs = @["examples"]
installExt = @["nim"]

# Dependencies
requires "nim >= 2.0.0"
requires "db_connector >= 0.1.0"

feature "examples":
  requires "websocket >= 0.2.2"
  requires "karax"
  requires "jester"

include "config.nims"
