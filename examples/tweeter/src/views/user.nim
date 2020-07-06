#? stdtmpl(subsChar = '$', metaChar = '#', toString = "xmltree.escape")
#import xmltree
#import times
#import "../model"
#
#proc renderUser*(user: User): string =
#  result = ""
<div id="user">
  <h1>${user.username}</h1>
  <span>Following: ${$user.following.len}</span>
</div>
#end proc
#
#proc renderUser*(user, currentUser: User): string =
#  result = ""
<div id="user">
  <h1>${user.username}</h1>
  <span>Following: ${$user.following.len}</span>
  #if user.username notin currentUser.following and user.username != currentUser.username:
  <form action="follow" method="post">
    <input type="hidden" name="follower" value="${currentUser.username}">
    <input type="hidden" name="target" value="${user.username}">
    <input type="submit" value="Follow">
  </form>
  #end if
</div>
#
#end proc
#
#proc renderMessages*(messages: openArray[Message]): string =
#  result = ""
<div id="messages">
  #for message in messages:
    <div>
      <a href="/${message.username}">${message.username}</a>
      <span>${message.time.fromUnix().format("HH:mm MMMM d',' yyyy")}</span>
      <h3>${message.msg}</h3>
    </div>
  #end for
</div>
#end proc