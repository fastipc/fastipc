// The publisher of the broadcast example: waits for a subscriber, sends "tick 1" to "tick 3" on the broadcast
// my_broadcast, then ends the broadcast. Its subscriber is examples/subscriber, or any other language's subscriber.

// publisher.go
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/fastipc/fastipc/bindings/go/fipc"
)

func main() {
	// 64 KiB of messages, up to 8 subscribers; one that holds the publisher back 5 s is dropped
	publisher, err := fipc.CreateBroadcast("my_broadcast", 1<<16, 8, 5*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := publisher.WaitForMembers(1, fipc.Forever); err != nil { // waits for the first subscriber
		log.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		if err := publisher.Send(fmt.Appendf(nil, "tick %d", i), fipc.Forever); err != nil {
			log.Fatal(err)
		}
	}
	publisher.Close() // ends the broadcast: subscribers get fipc.ErrDisconnected after the ticks
}
