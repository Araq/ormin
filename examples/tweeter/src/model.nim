type
  User* = tuple[
    username: string,
    following: seq[string]
  ]

  Message* = tuple[
    username: string,
    time: int,
    msg: string
  ]