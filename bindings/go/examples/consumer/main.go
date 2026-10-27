// The consumer of the work queue example: joins my_queue and prints "job 1" to "job 3" from examples/producer (or any
// other language's producer), until the queue ends.

// consumer.go
package main

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/fastipc/fastipc/bindings/go/fipc"
)

func main() {
	consumer, err := fipc.JoinQueue("my_queue", fipc.Consumer, 5*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	for {
		job, err := consumer.Receive(fipc.Forever)
		if errors.Is(err, fipc.ErrDisconnected) {
			return // the queue ended
		} else if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(job)) // job 1, job 2, job 3
	}
}
