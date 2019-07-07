var http = require("http")
var request = require("request")
var express = require("express")
var redis = require("redis")
var util = require("util")
var app =  express()

var client = redis.createClient(6379, "10.154.10.18")
var registerStr = "http://localhost:9000/register?service-name=adder_msvc&service-host=localhost&service-port=9002&health-check=health&queue-name=Q:adder_msvc";
request({uri:registerStr,method:"GET"}, function(err,response, body){
    console.log(body)
})

app.get("/health", function(req, res){
    res.json({data:"OK"});
})

app.listen(9002, function(){
    console.log("Server is listening")
})

getRequest()
function getRequest(){
    client.on("connect", function(){
     setInterval(function(){
        client.rpop("Q:adder_msvc", function(err, reply){
            if(err){
                console.log(err)
            }else if(reply != null){
                console.log("popped data:" + reply)
                replyObj  = JSON.parse(reply)
                replyQueueGw = replyObj.replyQueueGw
                responseStr = getData(reply)
                client.lpush(replyQueueGw,responseStr, function(err,reply){
                    if(err){
                        console.log("Error occured: " + err)
                    }else{
                        console.log("Successfully Inserted into Queue")
                    }
                })
            }else{
                console.log("Q:adder_msvc is empty")
            }
          })
     },10000)
    })

}

function getData(data){
    dataObj = JSON.parse(data);
    srcReplyQueue = dataObj.srcReplyQueue;
    sum = Number(dataObj.params.numberOne) + Number(dataObj.params.numberTwo);
    responseStr = util.format(`{"timestamp":"%s","serviceName":"adder_msvc", "result":{"sum":%d},"replyQueue":"%s"}`,
                   new Date(), sum, srcReplyQueue);

    console.log("sum is: " + sum);

    return responseStr;
}