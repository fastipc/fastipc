// A frame loop that never blocks on the connection: each frame sends its update and takes whatever has arrived,
// without waiting. Run it against examples/echo_server.

// game_loop.go
package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/fastipc/fastipc/bindings/go/fipc"
)

func main() {
	conn, err := fipc.Connect("demo", 5*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	echoes := 0
	buf := make([]byte, 64)
	for frame := range uint32(60) {
		update := binary.LittleEndian.AppendUint32(nil, frame)
		if err := conn.Send(update, fipc.NoWait); err != nil { // ErrTimeout only if the ring is full
			log.Fatal(err)
		}
		for {
			_, err := conn.ReceiveInto(buf, fipc.NoWait)
			if errors.Is(err, fipc.ErrTimeout) {
				break // nothing more this frame
			} else if err != nil {
				log.Fatal(err) // ErrDisconnected: the peer is gone
			}
			echoes++
		}
		time.Sleep(16 * time.Millisecond) // the rest of the frame
	}
	fmt.Printf("%d echoes in 60 frames\n", echoes)
}
