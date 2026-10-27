// The producer of the work queue example: owns the queue my_queue and is its producer, sends "job 1" to "job 3",
// waits for a consumer to join, then ends the queue. Its consumer is examples/consumer, or any other language's
// consumer.

// producer.go
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/fastipc/fastipc/bindings/go/fipc"
)

func main() {
	// 4 KiB of messages per producer, 1 producer and 1 consumer: this process owns the queue and is its producer
	queue, err := fipc.CreateQueue("my_queue", 4096, 1, 1)
	if err != nil {
		log.Fatal(err)
	}
	producer, err := fipc.JoinQueue("my_queue", fipc.Producer, 5*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	for i := 1; i <= 3; i++ { // each job waits in the queue until a consumer takes it
		if err := producer.Send(fmt.Appendf(nil, "job %d", i), fipc.Forever); err != nil {
			log.Fatal(err)
		}
	}
	if _, err := queue.WaitForMembers(fipc.Consumer, 1, fipc.Forever); err != nil { // waits for the consumer to join
		log.Fatal(err)
	}
	producer.Close()
	queue.Close() // ends the queue: the consumer gets fipc.ErrDisconnected after the jobs
}
