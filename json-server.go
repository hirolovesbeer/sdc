package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
	"container/list"
)

var (
	cmdQueue = list.New()
)

type Message struct {
	Message string
}

func newMessage(msg string) *Message {
	return &Message{msg}
}

type Request struct {
	Message []string
}

func jsonHandleFunc(rw http.ResponseWriter, req *http.Request) {
	output := newMessage("OK")

	defer func() {
		outjson, e := json.Marshal(output)
		if e != nil {
			fmt.Println(e)
		}
		rw.Header().Set("Content-Type", "application/json")
		fmt.Fprint(rw, string(outjson))
	}()

	if req.Method != "POST" {
		output.Message = "Not POST Method. Post only."
		return
	}

	body, e := ioutil.ReadAll(req.Body)
	if e != nil {
		output.Message = e.Error()
		fmt.Println(e.Error())
		return
	}

	// input := newMessage("fuga")
	input := Request{}
	e = json.Unmarshal(body, &input)
	if e != nil {
		output.Message = e.Error()
		// output.Out = e.Error()
		fmt.Println(e.Error())
		return
	}
	cmdQueue.PushBack(input.Message)

	fmt.Printf("%#v\n", input)
}

func main() {
	go func() {
		fs := http.FileServer(http.Dir("static"))
		http.Handle("/", fs)
		http.HandleFunc("/json", jsonHandleFunc)
		http.ListenAndServe(":8888", nil)
	}()

	fmt.Println("hogehoge")
	time.Sleep(30 * time.Second)

	fmt.Printf("cmdQueue.Len() = %d\n", cmdQueue.Len())
}
