import asyncdispatch, times
import jester
import database, model, views/[user, general]

proc userLogin(request: Request, user: var User): bool =
  if request.cookies.hasKey("username"):
    let username = request.cookies["username"]
    if not findUser(username, user):
      user = (username: username, following: @[])
      create(user)
    return true
  else:
    return false

routes:
  get "/":
    var user: User
    if userLogin(request, user):
      let messages = findMessage(user.following & user.username)
      resp renderMain(renderTimeline(user.username, messages))
    else:
      resp renderMain(renderLogin())

  get "/@name":
    cond '.' notin @"name"
    var user: User
    if not findUser(@"name", user):
      halt "User not found"
    let messages = findMessage([user.username])

    var currentUser: User
    if userLogin(request, currentUser):
      resp renderMain(renderUser(user, currentUser) & renderMessages(messages))
    else:
      resp renderMain(renderUser(user) & renderMessages(messages))

  post "/follow":
    var follower, target: User
    if not findUser(@"follower", follower):
      halt "Follower not found"
    if not findUser(@"target", target):
      halt "Follow target not found"
    follow(follower, target)
    redirect uri("/" & @"target")

  post "/login":
    setCookie("username", @"username", getTime().utc() + 2.hours)
    redirect "/"

  post "/createMessage":
    let message = (
      username: @"username",
      time: getTime().toUnix().int,
      msg: @"message"
    )
    post(message)
    redirect "/"

runForever()