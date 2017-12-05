## Frontend for our chat application example.
## You need to have 'karax' installed in order for this to work.

import karax / [kbase, karax, vdom, karaxdsl, kajax, jwebsockets, jjson,
                jstrutils, errors]

# Custom UI element with input validation:

proc validateNotEmpty(field: kstring): proc () =
  result = proc () =
    let x = getVNodeById(field)
    if x.text.isNil or x.text == "":
      setError(field, field & " must not be empty")
    else:
      setError(field, "")

proc loginField(desc, field, class: kstring;
                validator: proc (field: kstring): proc ()): VNode =
  result = buildHtml(tdiv):
    label(`for` = field):
      text desc
    input(`type` = class, id = field, onchange = validator(field))

# here we setup the connection to the server:
let conn = newWebSocket("ws://localhost:8080", "orminchat")

proc send(msg: JsonNode) =
  # The Ormin "protocol" requires us to have 'send' implementation.
  conn.send(toJson(msg))

type User* = ref object
  name*, password: kstring

const
  username = kstring"username"
  password = kstring"password"
  message = kstring"message"

# Include the 'chatclient' helper that Ormin produced for us:
include chatclient

template loggedIn(): bool = userId > 0

proc doLogin() =
  registerUser(User(name: getVNodeById(username).text,
                password: getVNodeById(password).text))

proc doSendMessage() =
  let inputField = getVNodeById(message)
  sendMessage(TextMessage(author: userId, content: inputField.text))
  inputField.setInputText ""

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
    tdiv:
      table:
        for m in allMessages:
          tr:
            td:
              bold:
                text m.name
            td:
              text m.content
    tdiv:
      if loggedIn:
        label(`for` = message):
          text "Message: "
        input(class = "input", id = message, onkeyupenter = doSendMessage)

registerOnUpdate()
runLater proc() =
  getRecentMessages()

setRenderer(main)
