// The subscriber of the broadcast example: joins my_broadcast gated and prints "tick 1" to "tick 3" from
// examples/publisher (or any other language's publisher), until the broadcast ends.

// subscriber.go
package main

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/fastipc/fastipc/bindings/go/fipc"
)

func main() {
	// gated: misses nothing (the publisher waits for it)
	subscriber, err := fipc.JoinBroadcast("my_broadcast", fipc.Gated, 5*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	defer subscriber.Close()
	for {
		tick, err := subscriber.Receive(fipc.Forever)
		if errors.Is(err, fipc.ErrDisconnected) {
			return // the publisher closed
		} else if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(tick)) // tick 1, tick 2, tick 3
	}
}
