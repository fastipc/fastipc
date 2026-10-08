// Echoes every message back until its client's end. Its client is examples/echo_client or examples/game_loop, or a
// client in any other language.

// echo_server.go
package main

import (
	"errors"
	"fmt"
	"log"

	"github.com/fastipc/fastipc/bindings/go/fipc"
)

func main() {
	// rings of 1 MiB each way: a power of two, 1 KiB to 2 GiB
	listener, err := fipc.Listen("demo", 1<<20)
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	conn, err := listener.Accept(fipc.Forever) // waits for a client
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	for {
		msg, err := conn.Receive(fipc.Forever)
		if errors.Is(err, fipc.ErrDisconnected) {
			break
		} else if err != nil {
			log.Fatal(err)
		}
		if err := conn.Send(msg, fipc.Forever); err != nil { // echo, any size
			log.Fatal(err)
		}
	}
	fmt.Println("client gone")
}
