// The client of the service example: joins my_service, sends "ping 1" to "ping 3" and prints each reply, "PING 1" to
// "PING 3", from examples/service_server (or any other language's service server).

// service_client.go
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/fastipc/fastipc/bindings/go/fipc"
)

func main() {
	client, err := fipc.JoinService("my_service", fipc.Client, 5*time.Second) // waits up to 5 s for the service
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	for i := 1; i <= 3; i++ {
		// sends the request and returns its reply; fipc.ErrAbandoned if the server ended before it replied
		reply, err := client.Call(fmt.Appendf(nil, "ping %d", i), fipc.Forever)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(reply)) // PING 1, PING 2, PING 3
	}
}
