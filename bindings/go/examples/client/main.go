// The client of the repository README's example: sends ping to examples/server (or any other language's server) and
// prints the answer.

// client.go
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/fastipc/fastipc/bindings/go/fipc"
)

func main() {
	conn, err := fipc.ConnectStream("my_channel", 5*time.Second) // waits up to 5 s for the server
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	if err := conn.Send([]byte("ping"), fipc.Forever); err != nil {
		log.Fatal(err)
	}
	reply, err := conn.Receive(fipc.Forever)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(reply)) // PING
}
