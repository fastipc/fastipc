// The server of the repository README's example: answers one RPC request with its payload in upper case. Its client
// is examples/client, or any other language's client.

// server.go
package main

import (
	"bytes"
	"log"

	"github.com/fastipc/fastipc/bindings/go/fipc"
)

func main() {
	listener, err := fipc.Listen("my_channel", 1<<20) // rings of 1 MiB each way
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	conn, err := listener.Accept(fipc.Forever) // waits for a client
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	request, err := conn.RPCReceive(fipc.Forever)
	if err != nil {
		log.Fatal(err)
	}
	err = conn.RPCRespond(request.ID, request.Opcode, 0, bytes.ToUpper(request.Payload), fipc.Forever)
	if err != nil {
		log.Fatal(err)
	}
}
