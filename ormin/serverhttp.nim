

import asynchttpserver, asyncdispatch, json

type
  Receivers* {.pure} = enum
    sender, allExceptSender, all
  ReqHandler* = proc (msg: JsonNode; receivers: var Receivers): JsonNode {.
    closure, gcsafe.}

proc error(msg: string) = echo "[Error] ", msg
proc warn(msg: string) = echo "[Warning] ", msg
proc hint(msg: string) = echo "[Hint] ", msg

proc serve*(key: string; handler: ReqHandler) =
  let httpServer = newAsyncHttpServer()

  proc reqhandler(req: Request) {.async.} =
    if req.url.path == "/" & key:
      let headers = newHttpHeaders([("Content-Type","application/json"),
                                    ("Access-Control-Allow-Origin", "*")])

      var receivers = Receivers.sender
      let b = req.body
      hint "Got raw data " & b
      let msgj = parseJson(b)
      let resp = handler(msgj, receivers)
      await req.respond(Http200, $resp, headers)
    else:
      await req.respond(Http404, "Not Found")

  waitFor httpServer.serve(Port(8080), reqhandler)
