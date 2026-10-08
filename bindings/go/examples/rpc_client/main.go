// One UPPER request to examples/rpc_server (or an RPC server in any other language).

// rpc_client.go
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/fastipc/fastipc/bindings/go/fipc"
)

const upper = 1

func main() {
	conn, err := fipc.Connect("my_channel", 5*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	id, err := conn.RPCSubmit(upper, []byte("ping"), fipc.Forever)
	if err != nil {
		log.Fatal(err)
	}
	reply, err := conn.RPCReceive(fipc.Forever)
	if err != nil {
		log.Fatal(err)
	}
	if reply.Kind != fipc.RPCResponse || reply.ID != id {
		log.Fatalf("not the response to request %d: %+v", id, reply)
	}
	fmt.Println(reply.Status, string(reply.Payload)) // 0 PING
}
