from bottle import route, run, static_file, request, post
import json

@route('/static/<filename>')
def server_static(filename):
    return static_file(filename, root="./static")

@route("/<name:re:.*\.html>")
def server_page(name):
    return static_file(name, root=".")

@route("/")
def index():
    return static_file("login.html", root=".")

@post("/login")
def login():
    user = request.forms.get("user")
    pwd = request.forms.get("pwd")
    print(user)
    print(pwd)
    if user == "yangren" and pwd == "123456":
        return "True"
    else:
        return "False"


run(host="0.0.0.0", port=80)
