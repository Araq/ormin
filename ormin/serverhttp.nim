

import asynchttpserver, asyncdispatch, asyncfile, mimetypes, os, json

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
    let path = req.url.path
    if path == "/" & key:
      let headers = newHttpHeaders([("Content-Type","application/json")])
      var receivers = Receivers.sender
      let b = req.body
      hint "Got raw data " & b
      let msgj = parseJson(b)
      let resp = handler(msgj, receivers)
      if not resp.isNil:
        await req.respond(Http200, $resp, headers)
    else:
      let contentType = getMimetype(newMimetypes(), splitFile(path).ext)
      let headers = newHttpHeaders([("Content-Type", contentType)])

      var file = openAsync("frontend" / path, fmReadWrite)
      let data = await file.readAll()

      await req.respond(Http200, data, headers)
      file.close()
    #else:
    #  await req.respond(Http404, "Not Found")

  waitFor httpServer.serve(Port(8080), reqhandler)
