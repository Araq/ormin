## Frontend for our chat application example.
## You need to have 'karax' installed in order for this to work.

import karax / [kbase, karax, vdom, karaxdsl, kajax, jwebsockets, jjson,
                jstrutils, errors]

# Custom UI element with input validation:

type
  Validator* = proc (field: kstring): proc ()

proc validateNotEmpty(field: kstring): proc () =
  result = proc () =
    let x = getVNodeById(field)
    if x.text.isNil or x.text == "":
      setError(field, field & " must not be empty")
    else:
      setError(field, "")

proc loginField(desc, field, class: kstring; validator: Validator): VNode =
  result = buildHtml(tdiv):
    label(`for` = field):
      text desc
    input(class = class, id = field, onkeyuplater = validator(field))

# here we setup the connection to the server:
let conn = newWebSocket("ws://localhost:8080", "orminchat")

proc send(msg: JsonNode) =
  # The Ormin "protocol" requires us to have 'send' implementation.
  conn.send(toJson(msg))

type User* = ref object
  name*, password: kstring

# Include the 'chatclient' helper that Ormin produced for us:
include chatclient

template loggedIn(): bool = userId > 0

const
  username = kstring"username"
  password = kstring"password"

proc doLogin() =
  registerUser(User(name: getVNodeById(username).text,
                password: getVNodeById(password).text))

proc registerOnUpdate() =
  conn.onmessage =
    proc (e: MessageEvent) =
      let msg = fromJson[JsonNode](e.data)
      # 'recvMsg' was generated for us:
      recvMsg(msg)
      karax.redraw()

proc main(): VNode =
  result = buildHtml(tdiv):
    tdiv:
      if not loggedIn:
        loginField("Name :", username, "input", validateNotEmpty)
        loginField("Password: ", password, "password", validateNotEmpty)
        button(onclick = doLogin, disabled = disableOnError()):
          text "Login"
        p:
          text getError(username)
        p:
          text getError(password)

    table:
      for m in allMessages:
        tr:
          td:
            bold:
              text m.name
          td:
            text m.content

registerOnUpdate()
runLater proc() =
  getRecentMessages()

setRenderer(main)
