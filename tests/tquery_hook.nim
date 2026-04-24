import unittest, strutils, json, times
import std/options

import ormin/query_hooks

proc fromQueryHook*(to: typedesc[seq[string]], x: string): seq[string] =
  x.split(",")

proc toQueryHook*(val: var string, x: seq[string]) =
  val = x.join(",")

suite "query hook helpers":
  test "maps raw DbValue primitives directly":
    check fromQueryHook(int, DbValue[int](value: 42)) == 42
    check fromQueryHook(bool, DbValue[bool](value: true)) == true
    check fromQueryHook(JsonNode, DbValue[JsonNode](value: %*{"hello": "world"})) == %*{"hello": "world"}

  test "maps null DbValue to Option":
    check fromQueryHook(Option[string], DbValue[string](isNull: true)).isNone
    check fromQueryHook(Option[string], DbValue[string](value: "hello")) == some("hello")

  test "maps null DbValue to string and JsonNode defaults":
    check fromQueryHook(string, DbValue[string](isNull: true)) == ""
    check fromQueryHook(JsonNode, DbValue[JsonNode](isNull: true)) == newJNull()

  test "applies custom hooks over raw DB values":
    let mapped = fromQueryHook(seq[string], DbValue[string](value: "alice,bob"))
    check mapped == @["alice", "bob"]

  test "writes Option.none to a null DbValue":
    var value: DbValue[string]

    toQueryHook(value, none(string))
    check value.isNull
    check value.value.len == 0

  test "round-trips custom hook types through DbValue":
    var value: DbValue[string]

    toQueryHook(value, @["carol", "dave"])
    check not value.isNull
    check value.value == "carol,dave"
    check fromQueryHook(seq[string], value) == @["carol", "dave"]

  test "round-trips DateTime through DbValue":
    let source = dateTime(2024, mJan, 2, 3, 4, 5, zone = utc())
    var value: DbValue[DateTime]

    toQueryHook(value, source)
    check not value.isNull
    check fromQueryHook(DateTime, value) == source
