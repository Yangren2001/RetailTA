/*
登录事件
 */

var login = document.getElementById("login");
login.addEventListener("click", function () {
    var user = document.getElementsByName("user")[0].value;
    var pwd = document.getElementsByName("password")[0].value;
    $.post("/login", data={
        "user":user,
        "pwd":pwd
    }, function (data)
    {
        if(data === "True")
        {
            window.location.href = "/index0.html";
        }
        else
        {
            alert("账号或密码错误！");
        }

    });
});
