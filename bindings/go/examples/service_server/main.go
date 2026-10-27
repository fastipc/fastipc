// The server of the service example: owns the service my_service and is its server, answers three requests, each
// with its uppercase, then ends the service. Its client is examples/service_client, or any other language's service
// client.

// service_server.go
package main

import (
	"bytes"
	"log"
	"time"

	"github.com/fastipc/fastipc/bindings/go/fipc"
)

func main() {
	// 4 KiB per client, requests and replies of up to 256 bytes, 1 client and 1 server: this process owns the service
	// and is its server
	service, err := fipc.CreateService("my_service", 4096, 256, 1, 1)
	if err != nil {
		log.Fatal(err)
	}
	server, err := fipc.JoinService("my_service", fipc.Server, 5*time.Second) // this process admits it, as any member
	if err != nil {
		log.Fatal(err)
	}
	for range 3 { // three requests, then the end
		request, err := server.Receive(fipc.Forever) // the next request of any client
		if err != nil {
			log.Fatal(err)
		}
		// the reply goes to its client, and never waits
		if err := server.Send(bytes.ToUpper(request), fipc.Forever); err != nil {
			log.Fatal(err)
		}
	}
	server.Close()
	service.Close() // ends the service: the client still receives the replies sent
}
