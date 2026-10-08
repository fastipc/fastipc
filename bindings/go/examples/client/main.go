// The client of the repository README's example: one RPC request to examples/server (or any other language's
// server).

// client.go
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/fastipc/fastipc/bindings/go/fipc"
)

func main() {
	conn, err := fipc.Connect("my_channel", 5*time.Second) // waits up to 5 s for the server
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.RPCSubmit(1, []byte("ping"), fipc.Forever); err != nil { // opcode 1
		log.Fatal(err)
	}
	reply, err := conn.RPCReceive(fipc.Forever)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(reply.Payload)) // PING
}
