// An RPC server: answers each UPPER request with its payload in upper case, until its client's end. Its client is
// examples/rpc_client, or a client in any other language.

// rpc_server.go
package main

import (
	"bytes"
	"errors"
	"log"

	"github.com/fastipc/fastipc/bindings/go/fipc"
)

const upper = 1

func main() {
	listener, err := fipc.Listen("my_channel", 1<<20)
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	conn, err := listener.Accept(fipc.Forever)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	for {
		req, err := conn.RPCReceive(fipc.Forever)
		if errors.Is(err, fipc.ErrDisconnected) {
			return
		} else if err != nil {
			log.Fatal(err)
		}
		if req.Kind == fipc.RPCRequest && req.Opcode == upper {
			if err := conn.RPCRespond(req.ID, req.Opcode, 0, bytes.ToUpper(req.Payload), fipc.Forever); err != nil {
				log.Fatal(err)
			}
		}
	}
}
