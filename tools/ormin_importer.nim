## Ormin -- ORM for Nim.
## (c) 2017 Andreas Rumpf
## MIT License.

import strutils, os, parseopt

import ../ormin/importer_core

proc writeHelp() =
  echo """
ormin <schema.sql> --out:<file.nim>  --db:postgre|sqlite|mysql
"""

proc writeVersion() = echo "v1.0"

var p = initOptParser()
var infile = ""
var outfile = ""
var target: ImportTarget
for kind, key, val in p.getopt():
  case kind
  of cmdArgument:
    infile = key
  of cmdLongOption, cmdShortOption:
    case key
    of "help", "h": writeHelp()
    of "version", "v": writeVersion()
    of "out", "o": outfile = val
    of "db": target = parseEnum[ImportTarget](val)
    else: discard
  of cmdEnd: assert(false) # cannot happen
if infile == "":
  # no filename has been given, so we show the help:
  writeHelp()
else:
  if outfile == "":
    outfile = changeFileExt(infile, "nim")
  writeFile(outfile, generateModelCode(readFile(infile), absolutePath(infile), target))
