package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
)

var (
	port string
	conns = make(map[string]net.Conn)
)

func init() {
	if len(os.Args) < 2 {
		log.Fatal("第1引数にポート番号を指定してください")
	}
	port = os.Args[1]
}

func main() {
	l, err := net.Listen("tcp", "localhost:" + port)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("ポート番号%sでListenしています¥n", port)

	for {
		c, err := l.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go serve(c)
	}
}

func serve(c net.Conn) {
	addr := c.RemoteAddr().String()

	log.Printf("クライアントから接続されました。[%s]¥n", addr)

	conns[addr] = c

	showConns()

	fmt.Fprintf(c, "Go研チャットサーバへようこそ[%s]¥n", addr)

	r := bufio.NewReader(c)
	for {
		b, _, err := r.ReadLine()
		if err != nil {
			if err == io.EOF {
				log.Printf("クライアントからの接続が切断されました。[%s]¥n", addr)
			} else {
				log.Printf("クライアントからのメッセージ読み込みでエラーが発生しました。[%s][%s]¥n", addr, err.Error())
			}
			close(c)
			return
		}

		handleInput(c, string(b))
	}
}

func close(c net.Conn) {
	addr := c.RemoteAddr().String()
	log.Printf("クライアントとの接続を切断します。[%s]¥n", addr)
	err := c.Close()
	if err != nil {
		log.Printf("クライアントとの接続を切断に失敗しました。[%s][%s]¥n", addr, err.Error())
	} else {
		log.Printf("クライアントとの接続を切断しました。[%s]¥n", addr)
		delete(conns, addr)
	}
	showConns()
}

func showConns() {
	if len(conns) > 0 {
		log.Println("現在接続中のクライアント:")
		i := 0
		for addr, _ := range conns {
			i++
			space := ""
			if i < 10 {
				space = " "
			}
			log.Printf("%s %d %¥n", space, i, addr)
		}
	} else {
		log.Println("現在接続中のクライアントはありません。")
	}
}

func handleInput(c net.Conn, s string) {
	tokens := strings.Split(s, ":")
	if len(tokens) < 2 {
		fmt.Fprintln(c, "入力内容の形式が不正です。「名前：メッセージ」という形式で入力してください。")
		return
	}
	name :=strings.TrimSpace(tokens[0])
	message := strings.TrimSpace(strings.Join(tokens[1:], ":"))
	log.Printf("クライアントからメッセージが送信されました。[%s][%s][%s]", c.RemoteAddr().String(), name, message)
	go broadcast(name, message)
}

func broadcast(name string, message string) {
	for _, c := range conns {
		fmt.Fprintln(c, name + ": " + message)
	}
}
