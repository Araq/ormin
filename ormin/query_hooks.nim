import strutils, options, json, times

type
  DbItem* = object
    name*: string
    value*: string
    isNull*: bool

  DbRow* = seq[DbItem]

var queryHookTimeFormat* = "yyyy-MM-dd HH:mm:ss"

template fromQueryHook*[T](to: typedesc[T], x: T): T =
  ## Default conversion hook used by `query(T): ...`.
  ## Users can overload this proc to customize field/type conversions.
  x

proc dbItemIndex*(val: openArray[DbItem]; name: string): int =
  result = -1
  for i, it in val:
    if cmpIgnoreCase(it.name, name) == 0:
      return i

proc dbItemByName*(val: var DbRow; name: string): var DbItem =
  let idx = dbItemIndex(val, name)
  if idx < 0:
    raise newException(KeyError, "query(T): missing field in DbRow: " & name)
  val[idx]

proc fromQueryHook*(to: typedesc[string], val: var DbItem): string =
  if val.isNull:
    when defined(nimNoNilSeqs):
      ""
    else:
      nil
  else:
    val.value

proc fromQueryHook*(to: typedesc[int], val: var DbItem): int =
  if val.isNull:
    raise newException(ValueError, "cannot map NULL to int")
  parseInt(val.value)

proc fromQueryHook*(to: typedesc[int64], val: var DbItem): int64 =
  if val.isNull:
    raise newException(ValueError, "cannot map NULL to int64")
  parseInt(val.value).int64

proc fromQueryHook*(to: typedesc[float64], val: var DbItem): float64 =
  if val.isNull:
    raise newException(ValueError, "cannot map NULL to float64")
  parseFloat(val.value)

proc fromQueryHook*(to: typedesc[bool], val: var DbItem): bool =
  if val.isNull:
    raise newException(ValueError, "cannot map NULL to bool")
  let s = val.value.toLowerAscii()
  if s in ["t", "true", "1", "yes", "y"]:
    true
  elif s in ["f", "false", "0", "no", "n"]:
    false
  else:
    raise newException(ValueError, "invalid boolean DbItem value: " & val.value)

proc fromQueryHook*(to: typedesc[DateTime], val: var DbItem): DateTime =
  if val.isNull:
    raise newException(ValueError, "cannot map NULL to DateTime")
  let src = val.value
  let i = src.find('.')
  if i < 0:
    if src.len >= 3 and src[src.len - 3] in {'+', '-'}:
      parse(src, "yyyy-MM-dd HH:mm:sszz")
    else:
      parse(src, "yyyy-MM-dd HH:mm:ss", utc())
  else:
    if src.len >= 3 and src[src.len - 3] in {'+', '-'}:
      let itz = src.len - 3
      let dtstr = src[0..<itz] & '0'.repeat(10 - src.len + i) & src[src.len - 3 .. ^1]
      parse(dtstr, "yyyy-MM-dd HH:mm:ss\'.\'ffffffzz")
    else:
      let dtstr = src & '0'.repeat(7 - src.len + i)
      parse(dtstr, "yyyy-MM-dd HH:mm:ss\'.\'ffffff", utc())

proc fromQueryHook*(to: typedesc[JsonNode], val: var DbItem): JsonNode =
  if val.isNull:
    newJNull()
  else:
    parseJson(val.value)

proc fromQueryHook*[T](to: typedesc[Option[T]], val: var DbItem): Option[T] =
  if val.isNull:
    none(T)
  else:
    some(fromQueryHook(T, val))

proc fromQueryHook*[T](to: typedesc[T], val: var DbItem): T =
  when compiles(fromQueryHook(to, val.value)):
    if val.isNull:
      raise newException(ValueError, "cannot map NULL DbItem value")
    fromQueryHook(to, val.value)
  else:
    {.error: "No fromQueryHook for this destination type. Provide fromQueryHook(typedesc[T], val: var DbItem) or fromQueryHook(typedesc[T], val: string).".}

proc fromQueryHook*[T](to: typedesc[T], val: var DbRow): T =
  when compiles(block:
    var probe: T
    for field, value in fieldPairs(probe):
      discard field
      discard value):
    for field, value in fieldPairs(result):
      var item = dbItemByName(val, field)
      value = fromQueryHook(typeof(value), item)
  elif compiles(block:
    var probe: T
    new(probe)
    for field, value in fieldPairs(probe[]):
      discard field
      discard value):
    new(result)
    for field, value in fieldPairs(result[]):
      var item = dbItemByName(val, field)
      value = fromQueryHook(typeof(value), item)
  else:
    if val.len != 1:
      raise newException(ValueError, "query(T): expected exactly one DbItem for scalar mapping")
    var item = val[0]
    result = fromQueryHook(T, item)

proc toQueryHook*(val: var string, x: string) =
  val = x

proc toQueryHook*(val: var string, x: int) =
  val = $x

proc toQueryHook*(val: var string, x: int64) =
  val = $x

proc toQueryHook*(val: var string, x: float64) =
  val = $x

proc toQueryHook*(val: var string, x: bool) =
  val = $x

proc toQueryHook*(val: var string, x: DateTime) =
  val = format(x, queryHookTimeFormat)

proc toQueryHook*(val: var string, x: JsonNode) =
  val = $x

proc toQueryHook*[T](val: var DbItem, x: Option[T]) =
  if x.isSome:
    toQueryHook(val, x.get)
  else:
    val.isNull = true
    setLen(val.value, 0)

proc toQueryHook*[T](val: var DbItem, x: T) =
  val.isNull = false
  when compiles(toQueryHook(val.value, x)):
    toQueryHook(val.value, x)
  else:
    {.error: "No toQueryHook for this source type. Provide toQueryHook(val: var DbItem, x: T) or toQueryHook(val: var string, x: T).".}

proc toQueryHook*[T](val: var DbRow, x: T) =
  setLen(val, 0)
  when compiles(block:
    for field, value in fieldPairs(x):
      discard field
      discard value):
    for field, value in fieldPairs(x):
      var item: DbItem
      item.name = field
      toQueryHook(item, value)
      val.add item
  elif compiles(block:
    if x.isNil:
      discard
    for field, value in fieldPairs(x[]):
      discard field
      discard value):
    if x.isNil:
      raise newException(ValueError, "cannot map nil ref object to DbRow")
    for field, value in fieldPairs(x[]):
      var item: DbItem
      item.name = field
      toQueryHook(item, value)
      val.add item
  else:
    var item: DbItem
    toQueryHook(item, x)
    val.add item
