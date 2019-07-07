package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/gjson"

	"github.com/buger/jsonparser"
	"github.com/magiconair/properties"

	"github.com/garyburd/redigo/redis"
)

var pool redis.Pool
var config = properties.MustLoadFile("../conf/application.properties", properties.UTF8)

type Microservice struct {
	ServiceName string `json:"serviceName"`
	QName       string `json:"qName"`
	ServicePort string `json:"servicePort"`
	ServiceHost string `json:"serviceHost"`
	HealthCheck string `json:"healthCheck"`
}

func init() {

	pool = redis.Pool{
		MaxActive:   config.MustGetInt("redis.pool.maxActive"),
		MaxIdle:     config.MustGetInt("redis.pool.maxIdle"),
		IdleTimeout: 60 * time.Second,
		Wait:        true,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", fmt.Sprintf("%s:%s", config.MustGetString("redis.host"), config.MustGetString("redis.port")))
			if err != nil {
				return nil, err
			}

			return c, nil
		},
		TestOnBorrow: func(conn redis.Conn, t time.Time) error {
			_, err := conn.Do("PING")

			return err

		},
	}

}
func main() {
	wg := sync.WaitGroup{}
	mutex := sync.Mutex{}
	readQueueGoroutines := config.MustGetInt("go.readQueue.goroutines")
	//Start non-blocking http server
	startHttpServer()
	for {
		for i := 0; i < readQueueGoroutines; i++ {
			wg.Add(1)
			go readQueue(&wg, &mutex, i)
			wg.Add(1)
			go readReplyQueue(&wg)

		}
		time.Sleep(50 * time.Millisecond)

	}
	//wg.Wait()
}

func readQueue(wg *sync.WaitGroup, mutex *sync.Mutex, id int) {
	conn := pool.Get()
	defer conn.Close()
	defer wg.Done()

	mutex.Lock()
	resultSvcCall, err := redis.String(conn.Do("rpop", "Q:comms"))
	mutex.Unlock()

	if err != nil {
		if err.Error() == "redigo: nil returned" {
			log.Println("Q:Comms is empty")
		} else {
			log.Println(err)
		}
	}

	if resultSvcCall != "" {
		fmt.Println("Received service call ", resultSvcCall)
		dataFromMsvc := []byte(resultSvcCall)
		//serviceName, _ := jsonparser.GetString(dataFromMsvc, "serviceName")
		var timestamp, serviceName, params, replyQueue, ttl string

		jsonparser.ObjectEach(dataFromMsvc, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
			switch string(key) {
			case "timestamp":
				timestamp = string(value)
			case "serviceName":
				serviceName = string(value)
			case "ttl":
				ttl = string(value)
			case "params":
				params = string(value)
			case "replyQueue":
				replyQueue = string(value)

			}

			return nil
		})
		fmt.Println(timestamp, serviceName, ttl, params, replyQueue)

		//Check if the service is registered
		resultMsvcCheck, err := redis.String(conn.Do("hget", "microservices", serviceName))
		if err != nil {
			fmt.Println("Error in checking service registration", err)
		}
		fmt.Println("Got the service: ", resultMsvcCheck)

		// Extract Queue name to push requests
		serviceQName := strings.Split(resultMsvcCheck, "|")[0]
		fmt.Println("QName: ", serviceQName)

		// Microservice Call
		msvcQueryStr := fmt.Sprintf(`
		{
			"timestamp":"%s",
			"serviceName":"%s",
			"srcReplyQueue":"%s",
			"ttl":"%s",
			"params":%s,
			"replyQueueGw":"QR:apigw:%s"
		   }`, timestamp, serviceName, replyQueue, ttl, params, serviceName)

		fmt.Println("Push request into msvc queue")
		_, err = conn.Do("lpush", serviceQName, msvcQueryStr)
		if err != nil {
			fmt.Println("Error during writing to msvc queue: ", serviceQName, err)
		}

		//fmt.Println("result:", serviceName, "Thread:", id)
	}

}

func registerMsvc(w http.ResponseWriter, r *http.Request) {
	conn := pool.Get()
	defer conn.Close()

	//e.g http://localhost:9000/register?service-name=http_client&service-host=localhost&service-port=90001&health-check=health&queue-name=Q:http_codec
	params := r.URL.Query()
	serviceName := params.Get("service-name")
	qName := params.Get("queue-name")
	healthCheck := params.Get("health-check")
	serviceHost := params.Get("service-host")
	servicePort := params.Get("service-port")

	//Save into REDIS
	_, err := conn.Do("hset", "microservices", serviceName, fmt.Sprintf("%s|%s|%s|%s", qName, serviceHost, servicePort, healthCheck))
	if err != nil {
		log.Println("Error during saving microservice details to redis", err)
	}

	//Save into file [persistance]
	microservice := Microservice{
		ServiceName: serviceName,
		ServiceHost: serviceHost,
		ServicePort: servicePort,
		QName:       qName,
		HealthCheck: healthCheck,
	}

	byteJson, err := json.Marshal(&microservice)
	if err != nil {
		fmt.Println("Error during Marshalling: ", err)
	}

	err = ioutil.WriteFile("../services/"+serviceName, byteJson, 0644)

	if err != nil {
		fmt.Println("Error during saving service to snapshot file: ", err)
	}
	fmt.Fprintln(w, params.Get("service-name"))
	//fmt.Print(params)

}

// Non-Blocking web-server
func startHttpServer() {
	http.HandleFunc("/register", registerMsvc)

	server := &http.Server{}
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: config.MustGetInt("http.port")})
	if err != nil {
		log.Fatal("Error in creating listener", err)
	}
	go server.Serve(listener)

}

func readReplyQueue(wg *sync.WaitGroup) {
	conn := pool.Get()
	defer conn.Close()
	defer wg.Done()
	ms_list, err := redis.StringMap(conn.Do("hgetall", "microservices"))
	if err != nil {
		fmt.Println("Error in getting service list", err)
	}

	for k, _ := range ms_list {

		ms_result, err := redis.String(conn.Do("rpop", "QR:apigw:"+k))

		if err != nil {
			if err.Error() == "redigo: nil returned" {
				log.Println("QR:apigw:" + k + " is empty")
			} else {
				fmt.Println("Error during reading Reply Queue", "QR:apigw:"+k, err)
			}
		} else {
			fmt.Println("Ms Result ", ms_result)
			replyQueue := gjson.Get(ms_result, "replyQueue").String()

			fmt.Println("Push Result into msvc result queue")
			_, err = conn.Do("lpush", replyQueue, ms_result)
			if err != nil {
				fmt.Println("Error during writing to msvc result queue: ", replyQueue, err)
			}
		}

	}

}
