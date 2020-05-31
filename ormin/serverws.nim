
import asynchttpserver, asyncdispatch, asyncnet, json,
  strutils, times

import websocket

type
  Receivers* {.pure} = enum
    sender, allExceptSender, all
  ReqHandler* = proc (msg: JsonNode; receivers: var Receivers): JsonNode {.
    closure, gcsafe.}

proc error(msg: string) = echo "[Error] ", msg
proc warn(msg: string) = echo "[Warning] ", msg
proc hint(msg: string) = echo "[Hint] ", msg

type
  Client = ref object
    socket: AsyncWebSocket
    connected: bool
    hostname: string
    id: int
    lastMessage: float
    rapidMessageCount: int

  Server = ref object
    clients: seq[Client]
    needsUpdate: bool
    receivers: Receivers
    msgs: seq[(string, int)]
    gid: int
    handler: ReqHandler

proc newClient(s: Server; socket: AsyncWebSocket, hostname: string): Client =
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
            if server.receivers != Receivers.allExceptSender or c.id != m[1]:
              await c.socket.sendText(m[0]) #, false)
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
  let now = epochTime()
  if now - client.lastMessage < 0.1: # 100ms
    client.rapidMessageCount.inc
  else:
    client.rapidMessageCount = 0

  client.lastMessage = now
  if client.rapidMessageCount > 50:
    warn("Client ($1) is firing messages too rapidly. Killing." % $client)
    client.connected = false
  let msgj = parseJson(data)
  server.receivers = Receivers.sender
  if msgj.hasKey("msg") and msgj["msg"].getStr("") == "disconnect":
    client.connected = false
    server.needsUpdate = true
  else:
    let resp = server.handler(msgj, server.receivers)
    if not resp.isNil:
      if server.receivers == sender:
        await client.socket.sendText($resp) #, false)
      else:
        server.msgs.add(($resp, client.id))
        server.needsUpdate = true

proc processClient(server: Server, client: Client) {.async.} =
  while client.connected:
    var frameFut = client.socket.readData()
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
        echo processFut.error.getStackTrace()
        error("Client ($1) attempted to send bad JSON? " % $client & "\n" &
              processFut.error.msg)
        #client.connected = false
  await client.socket.close()

proc onRequest(server: Server, req: Request; key: string) {.async.} =
  let (ws, error) = await verifyWebsocketRequest(req, key)
  if ws != nil:
    hint("Client connected from " & req.hostname)
    ws.protocol = key
    server.clients.add(newClient(server, ws, req.hostname))
    asyncCheck processClient(server, server.clients[^1])
  else:
    warn("WS negotiation failed: " & $error)
    await req.respond(Http400, "WebSocket negotiation failed: " & $error)
    req.client.close()

proc serve*(key: string; handler: ReqHandler) =
  let httpServer = newAsyncHttpServer()
  let server = Server(clients: @[], msgs: @[], handler: handler, gid: 0)

  proc cb(req: Request): Future[void] {.async, gcsafe.} =
    await onRequest(server, req, key)

  asyncCheck updateClients(server)
  waitFor httpServer.serve(Port(8080), cb)
