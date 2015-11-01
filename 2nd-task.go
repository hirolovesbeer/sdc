package main

import (
	"fmt"
	"os/exec"
	"net"
	"bufio"
	"io"
	"strings"
	"bytes"
	"log"
)

func main() {
	fmt.Println("Launching server...")

	ln, err := net.Listen("tcp", ":8081")
	if err != nil {
		log.Fatal(err)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}

		go server(conn)
	}
}

func server(conn net.Conn) {
	// send to 3rd task
	conn2, _ :=net.Dial("tcp", "127.0.0.1:8082")
	defer conn2.Close()

	r := bufio.NewReader(conn)
	for {
		grepCmd := exec.Command("/usr/bin/grep", "abe")

		message, _, err := r.ReadLine()
		fmt.Println("Message Received:", string(message))

		if err == io.EOF {
			break
		}

		grepCmd.Stdin = strings.NewReader(string(message))
		var out bytes.Buffer
		grepCmd.Stdout = &out
		err2 := grepCmd.Run()
		if err2 != nil {
			continue
			// fmt.Println("err2: ", err2)
		}
		fmt.Printf("in all caps: %q\n", out.String())

		// conn.Write([]byte("from server\n"))
		// send message to client
		conn.Write([]byte(out.String() + "\n"))

		// send message to 3rd-task
		fmt.Print("Text to send to 3rd-task\n")
		// conn2.Write([]byte(out.String() + "\n"))
		conn2.Write([]byte(out.String()))
	}

	defer conn.Close()
}