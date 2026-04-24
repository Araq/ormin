import options, json

type
  DbValue*[T] = object
    isNull*: bool
    value*: T

template fromQueryHook*[T, S](to: typedesc[T], x: S): T =
  ## Default conversion hook used by `query(T): ...`.
  ## Users can overload this proc to customize field/type conversions.
  block:
    var converted: T = x
    converted

template toQueryHook*[T, S](val: var T, x: S) =
  ## Default conversion hook used for query parameters.
  ## Users can overload this proc to customize parameter conversions.
  val = x

proc nullQueryValueError() {.noreturn.} =
  raise newException(ValueError, "cannot map NULL query result")

proc fromQueryHook*[T, S](to: typedesc[Option[T]], x: DbValue[S]): Option[T] =
  if x.isNull:
    none(T)
  else:
    some(fromQueryHook(T, x.value))

proc fromQueryHook*[T, S](to: typedesc[T], x: DbValue[S]): T =
  if x.isNull:
    when T is string:
      ""
    elif T is JsonNode:
      newJNull()
    else:
      nullQueryValueError()
  else:
    fromQueryHook(T, x.value)

proc bindFromQueryHook*[T, S](dest: var T, x: DbValue[S]) =
  dest = fromQueryHook(T, x)

proc toQueryHook*[S, T](val: var DbValue[S], x: Option[T]) =
  if x.isSome:
    val.isNull = false
    toQueryHook(val.value, x.get)
  else:
    val.isNull = true
    when compiles(val.value = default(S)):
      val.value = default(S)

proc toQueryHook*[S, T](val: var DbValue[S], x: T) =
  val.isNull = false
  toQueryHook(val.value, x)
