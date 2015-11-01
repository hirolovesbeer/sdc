package main

import (
	"fmt"
	"os/exec"
	"net"
	"bufio"
)

func main() {
	path := "./hoge.txt"
	catCmd := exec.Command("/bin/cat", path)

    catOut, err := catCmd.Output()
	if err != nil {
		panic(err)
	}
	fmt.Println("> cat")
	fmt.Println(string(catOut))

	// conn, _ :=net.Dial("tcp", "127.0.0.1:8081")
	conn, _ :=net.Dial("tcp", "10.141.141.10:8081")

	fmt.Print("Text to send: ")
	fmt.Fprintf(conn, string(catOut))

	message, _ := bufio.NewReader(conn).ReadString('\n')
	fmt.Print("Message from server: " + message)
}
