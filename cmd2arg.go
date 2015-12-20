package main

import (
	"fmt"
	"strings"
	"time"
	"math/rand"
)

func main() {
        // [/bin/cat /var/tmp/1.txt /var/tmp/2.txt /var/tmp/3.txt | /bin/grep abe > /var/tmp/grep-result.txt]
        //
        // rebuild args
        // 1. /bin/cat /var/tmp/1.txt >> /var/tmp/intermediate.txt
        // 2. /bin/cat /var/tmp/2.txt >> /var/tmp/intermediate.txt
        // 3. /bin/cat /var/tmp/3.txt >> /var/tmp/intermediate.txt
        // 4. /bin/cat /var/tmp/intermediate.txt | /bin/grep abe > /var/tmp/grep-result.txt
        // 5. /bin/rm /var/tmp/intermediate.txt
	
	rand.Seed(time.Now().UnixNano()) // Seed
	inter_file := fmt.Sprintf("/var/tmp/%010d.txt", rand.Intn(1000000000))
	fmt.Println("inter_file = ", inter_file)
	inter_file = "/var/tmp/intermediate.txt"

        args := []string{"/bin/cat", "/var/tmp/1.txt", "/var/tmp/2.txt", "/var/tmp/3.txt", "|", "/bin/grep", "abe", ">", "/var/tmp/grep-result.txt", "|", "date"}
        // args := []string{"/bin/cat", "/var/tmp/1.txt", "/var/tmp/2.txt", "/var/tmp/3.txt", "|", "/bin/grep", "abe", ">", "/var/tmp/grep-result.txt"}

	pipe_indexes := make([]int, 0)

	// find pipe
	for i := range args {
		// fmt.Printf("arg = %s\n", args[i])

		if args[i] == "|" {
			// fmt.Println("Find pipe")
			pipe_indexes = append(pipe_indexes, i)
		}
	}

	// fmt.Println("Pipe = ", pipe_indexes)

	cmds := make([]string, 0)

	start := 0
	for i := range pipe_indexes {
		// fmt.Printf("start = %d, end = %d\n", start, pipe_indexes[i])
		// fmt.Println(args[start:pipe_indexes[i]])
		// fmt.Println("join = ", strings.Join(args[start:pipe_indexes[i]], " "))
		if start == 0 {
			for ii :=1; ii<pipe_indexes[i]; ii++ {
				fmt.Println(args[start], args[ii])
				cmd := []string{args[start], args[ii], ">>", inter_file}
		  		cmds = append(cmds, strings.Join(cmd, " "))
			}
		} else {
			cmd := []string{"/bin/cat", inter_file, "|"}
			cmd = append(cmd, args[start:pipe_indexes[i]]...)
			// cmd := args[start:pipe_indexes[i]]
	  		cmds = append(cmds, strings.Join(cmd, " "))
			// cmds = append(cmds, strings.Join(args[start:pipe_indexes[i]], " "))
		}
		start = pipe_indexes[i]+1
	}

	// fmt.Println(start)
	// fmt.Println(len(args))
	// fmt.Println(args[start:len(args)])
	cmds = append(cmds, strings.Join(args[start:len(args)], " "))

	// remove intermediate file
	cmd := []string{"/bin/rm", inter_file}
	cmds = append(cmds, strings.Join(cmd, " "))
	
	fmt.Println("cmds = %s", cmds)

	for _, v := range cmds {
		fmt.Println("v = ", v)
	}
}
