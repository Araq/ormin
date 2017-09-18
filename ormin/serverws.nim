
import asynchttpserver, asyncdispatch, asyncnet, "../../websocket/websocket", json,
  strutils, times

when false:
  type
    MessageId = distinct int
    MessageKind = enum # serialized so add new values to the end!
      msgSelect,
      msgUpdate,
      msgInsert,
      msgDelete,
      msgDisconnect
    Message = object
      kind: MessageKind
      id: MessageId
      data: JsonNode
      version: int

type
  ReqHandler* = proc (msg: JsonNode; broadcast: var bool): JsonNode {.
    closure, gcsafe.}

when false:
  proc `%`(id: MessageId): JsonNode = %BiggestInt(id)
  proc `%`(k: MessageKind): JsonNode = %BiggestInt(k)

  proc messageFromJson(j: JsonNode): Message =
    Message(kind: MessageKind(j["kind"].getNum), id: MessageId(j["id"].num),
            data: j["data"], version: j["version"].num.int)

proc error(msg: string) = echo "[Error] ", msg
proc warn(msg: string) = echo "[Warning] ", msg
proc hint(msg: string) = echo "[Hint] ", msg

type
  Client = ref object
    socket: AsyncSocket
    connected: bool
    hostname: string
    id: int
    lastMessage: float
    rapidMessageCount: int

  Server = ref object
    clients: seq[Client]
    needsUpdate: bool
    msgs: seq[(string, int)]
    gid: int
    handler: ReqHandler

proc newClient(s: Server; socket: AsyncSocket, hostname: string): Client =
  inc s.gid
  result = Client(socket: socket, connected: true, hostname: hostname, id: s.gid)

proc `$`(client: Client): string =
  "Client(ip: $1)" % [client.hostname]

proc updateClients(server: Server) {.async.} =
  while true:
    var needsUpdate = false
    for client in server.clients:
      if not client.connected:
        needsUpdate = true
        break

    server.needsUpdate = server.needsUpdate or needsUpdate
    if server.needsUpdate and server.msgs.len != 0:
      var someDead = false
      # perform a copy to prevent the race condition:
      var msgs = server.msgs
      setLen(server.msgs, 0)
      for m in msgs:
        for c in server.clients:
          if c.connected:
            # do not send a broadcast message to the one which sent it:
            if c.id != m[1]:
              await c.socket.sendText(m[0], false)
          else:
            someDead = true
      if someDead:
        var i = 0
        while i < server.clients.len:
          if not server.clients[i].connected: del(server.clients, i)
          else: inc i
      server.needsUpdate = false
    # let other stuff in the main loop run:
    await sleepAsync(10)

proc processMessage(server: Server, client: Client, data: string) {.async.} =
  # Check if last message was relatively recent. If so, kick the user.
  echo "processMessage ", data
  if epochTime() - client.lastMessage < 0.1: # 100ms
    client.rapidMessageCount.inc
  else:
    client.rapidMessageCount = 0

  client.lastMessage = epochTime()
  if client.rapidMessageCount > 10:
    warn("Client ($1) is firing messages too rapidly. Killing." % $client)
    client.connected = false
  let msgj = parseJson(data)
  var broadcast = false
  if msgj.hasKey("msg") and msgj["msg"].getStr("") == "disconnect":
    client.connected = false
    server.needsUpdate = true
  else:
    let resp = server.handler(msgj, broadcast)
    if broadcast:
      server.msgs.add(($resp, client.id))
      server.needsUpdate = true
    else:
      await client.socket.sendText($resp, false)

proc processClient(server: Server, client: Client) {.async.} =
  while client.connected:
    var frameFut = client.socket.readData(false)
    yield frameFut
    if frameFut.failed:
      error("Error occurred handling client messages.\n" &
            frameFut.error.msg)
      client.connected = false
      break

    let frame = frameFut.read()
    if frame.opcode == Opcode.Text:
      let processFut = processMessage(server, client, frame.data)
      if processFut.failed:
        error("Client ($1) attempted to send bad JSON? " % $client & "\n" &
              processFut.error.msg)
        client.connected = false

  client.socket.close()

proc onRequest(server: Server, req: Request; key: string) {.async.} =
  let (success, error) = await verifyWebsocketRequest(req, key)
  if success:
    hint("Client connected from " & req.hostname)
    server.clients.add(newClient(server, req.client, req.hostname))
    asyncCheck processClient(server, server.clients[^1])
  else:
    warn("WS negotiation failed: " & error)
    await req.respond(Http400, "WebSocket negotiation failed: " & error)
    req.client.close()

proc serve*(key: string; handler: ReqHandler) =
  let httpServer = newAsyncHttpServer()
  let server = Server(clients: @[], msgs: @[], handler: handler, gid: 0)

  proc cb(req: Request): Future[void] {.async, gcsafe.} =
    await onRequest(server, req, key)

  asyncCheck updateClients(server)
  waitFor httpServer.serve(Port(8080), cb)
