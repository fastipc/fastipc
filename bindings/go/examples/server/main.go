// The server of the repository README's example: answers one message with it in upper case. Its client is
// examples/client, or any other language's client.

// server.go
package main

import (
	"bytes"
	"log"

	"github.com/fastipc/fastipc/bindings/go/fipc"
)

func main() {
	listener, err := fipc.CreateStream("my_channel", 1<<20) // rings of 1 MiB each way
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	conn, err := listener.Accept(fipc.Forever) // waits for a client
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	message, err := conn.Receive(fipc.Forever)
	if err != nil {
		log.Fatal(err)
	}
	if err := conn.Send(bytes.ToUpper(message), fipc.Forever); err != nil {
		log.Fatal(err)
	}
}
