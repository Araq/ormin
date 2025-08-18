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

include "config.nims"
