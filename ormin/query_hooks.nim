import options, json

type
  DbValue*[T] = object
    isNull*: bool
    value*: T

template fromQueryHook*[T, S](val: var T, x: S) =
  ## Default conversion hook used by `query(T): ...`.
  ## Users can overload this proc to customize field/type conversions.
  val = x

template toQueryHook*[T, S](val: var T, x: S) =
  ## Default conversion hook used for query parameters.
  ## Users can overload this proc to customize parameter conversions.
  val = x

proc nullQueryValueError() {.noreturn.} =
  raise newException(ValueError, "cannot map NULL query result")

proc fromQueryHook*[T, S](val: var Option[T], x: var DbValue[S]) =
  if x.isNull:
    val = none(T)
  else:
    var converted: T
    fromQueryHook(converted, move x.value)
    val = some(converted)

proc fromQueryHook*[T, S](val: var T, x: var DbValue[S]) =
  if x.isNull:
    when T is string:
      val = ""
    elif T is JsonNode:
      val = newJNull()
    else:
      nullQueryValueError()
  else:
    fromQueryHook(val, move x.value)

proc bindFromQueryHook*[T, S](dest: var T, x: var DbValue[S]) =
  fromQueryHook(dest, x)

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
