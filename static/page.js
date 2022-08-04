var bt1 = document.getElementById("customer_num");
var bt2 = document.getElementById("country");
var bt3 = document.getElementById("goods");
var bt4 = document.getElementById("word");
var bt5 = document.getElementById("mouth");
var bt6 = document.getElementById("day");
var bt7 = document.getElementById("return");
var bt8 = document.getElementById("buy");

var bt = [bt1, bt2, bt3, bt4, bt5, bt6, bt7, bt8];
for (var i = 1; i <=8; i++)
{
    bt[i - 1].addEventListener("click", function ()
    {
        window.location.href = "/index" + bt.indexOf(this).toString() + ".html";
    })
}