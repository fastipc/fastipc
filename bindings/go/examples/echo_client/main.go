// Sends three words to examples/echo_server and prints what comes back.

// echo_client.go
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/fastipc/fastipc/bindings/go/fipc"
)

func main() {
	conn, err := fipc.Connect("demo", 5*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	for _, word := range []string{"hello", "shared", "memory"} {
		if err := conn.Send([]byte(word), fipc.Forever); err != nil {
			log.Fatal(err)
		}
		echo, err := conn.Receive(fipc.Forever)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(echo))
	}
}
