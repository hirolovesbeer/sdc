package main

import (
	"fmt"
	"container/list"
)

var (
	cmdQueue = list.New()
)

func main() {
	args1 := []string{"/bin/echo 'hogehoge'", "/bin/date"}
	args2 := []string{"/usr/bin/id", "/bin/date"}

	cmdQueue.PushBack(args1)
	cmdQueue.PushBack(args2)

	fmt.Printf("cmdQueue.Len() = %d\n", cmdQueue.Len())
}
