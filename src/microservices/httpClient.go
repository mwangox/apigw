package main

import (
	"fmt"
	"log"
	"net/http"
	"sync"
)

var urls = []string{
	"https://google.com",
	"https://tutorialedge.net",
	"https://twitter.com",
}

func home(w http.ResponseWriter, r *http.Request) {

	wg := sync.WaitGroup{}

	for _, url := range urls {
		wg.Add(1)
		go fetch(url, &wg)
	}
	wg.Wait()
	fmt.Fprintf(w, "All responses received")
}

func fetch(url string, wg *sync.WaitGroup) {
	resp, err := http.Get(url)
	if err != nil {
		log.Println(err)
	}
	fmt.Println(resp.Status, url)
	defer wg.Done()
}

func main() {

	http.HandleFunc("/", home)
	http.ListenAndServe(":9090", nil)

}
