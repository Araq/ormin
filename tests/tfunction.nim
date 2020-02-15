import unittest, strformat, sequtils, strutils, math
import ../ormin

include initdb

suite fmt"test common sql function of {backend}":
  test "query with count":
    let res = query:
      select person(count(_))
    check res[0] == personcount

  test "query with sum":
    let res = query:
      select person(sum(id))
    check res[0] == persondata.mapIt(it.id).sum()

  test "query with avg":
    let res = query:
      select person(avg(id))
    check res[0] == persondata.mapIt(it.id).sum() / persondata.len()

  test "query with min":
    let res = query:
      select person(min(id))
    check res[0] == 1

  test "query with max":
    let res = query:
      select person(max(id))
    check res[0] == personcount