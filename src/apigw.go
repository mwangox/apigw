package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/magiconair/properties"

	"github.com/garyburd/redigo/redis"
)

var pool redis.Pool
var config = properties.MustLoadFile("conf/application.properties", properties.UTF8)

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
		IdleTimeout: 240 * time.Millisecond,
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
	startHttpServer()
	for {
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go readQueue(&wg, &mutex, i)

		}
		time.Sleep(5 * time.Second)

	}
	wg.Wait()
}

func readQueue(wg *sync.WaitGroup, mutex *sync.Mutex, id int) {
	conn := pool.Get()
	defer conn.Close()
	defer wg.Done()
	mutex.Lock()
	result, err := redis.String(conn.Do("rpop", "queue"))
	mutex.Unlock()
	if err != nil {
		log.Println(err)
	}
	fmt.Println("result:", result, "Thread:", id)

}

func registerMsvc(w http.ResponseWriter, r *http.Request) {
	conn := pool.Get()
	defer conn.Close()

	//e.g http://localhost:9000/register?service-name=http_client&service-host=localhost&service-port=90001&health-check=health
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

	//Save into file
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

	err = ioutil.WriteFile("services/"+serviceName, byteJson, 0644)

	if err != nil {
		fmt.Println("Error during saving service to snapshot file: ", err)
	}
	fmt.Fprintln(w, params.Get("service-name"))
	//fmt.Print(params)

}

func startHttpServer() {
	http.HandleFunc("/register", registerMsvc)

	server := &http.Server{}
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 12000})
	if err != nil {
		log.Fatal("Error in creating listener", err)
	}
	go server.Serve(listener)

}
