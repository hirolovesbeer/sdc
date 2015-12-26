package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

type Message struct {
	Message []string `json:"Message"`
}

type Response struct {
	Message string
}

func main() {
	args := []string{"/bin/cat /var/tmp/1.txt >> /var/tmp/intermediate.txt", "/bin/cat /var/tmp/2.txt >> /var/tmp/intermediate.txt", "/bin/cat /var/tmp/3.txt >> /var/tmp/intermediate.txt", "/bin/cat /var/tmp/intermediate.txt | /bin/grep abe > /var/tmp/grep-result.txt", "/bin/rm /var/tmp/intermediate.txt"}

	msg := &Message{Message: args}

	intput, err := json.Marshal(msg)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	res, err := http.Post("http://127.0.0.1:8888/json", "application/json", bytes.NewBuffer(intput))
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	if res.StatusCode != http.StatusOK {
		fmt.Printf("Status code = %s", res.StatusCode)
		return
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		fmt.Printf("%s", body)
		fmt.Println(err.Error())
		return
	}

	output := Response{}
	err = json.Unmarshal(body, &output)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	fmt.Printf("%#v\n", output)
}
