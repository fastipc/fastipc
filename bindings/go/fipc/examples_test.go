package fipc_test

// The examples (../examples/*) run in pairs, as the README runs them: a server, then its client (a publisher, then
// its subscriber, and the other way round; a producer, then its consumer, and the other way round; a service's server,
// then its client, and the other way round), each in a process of its own. They are built with the go command that runs the tests (go test puts its GOROOT/bin first on PATH).

import (
	"bytes"
	"context"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// buildExamples builds every example into a folder of the test's and returns it.
func buildExamples(t *testing.T) string {
	t.Helper()
	gocmd, err := exec.LookPath("go")
	if err != nil {
		t.Skip("no go command on PATH to build the examples with")
	}
	dir := t.TempDir()
	cmd := exec.Command(gocmd, "build", "-o", dir+string(filepath.Separator), "./examples/...")
	cmd.Dir = ".."
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build ./examples/...: %v\n%s", err, out)
	}
	return dir
}

// runPair runs server, then client, within 30 seconds; both must succeed. It returns their outputs.
func runPair(t *testing.T, dir, server, client string) (string, string) {
	t.Helper()
	exe := func(name string) string {
		if runtime.GOOS == "windows" {
			name += ".exe"
		}
		return filepath.Join(dir, name)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	var serverOut, serverErr bytes.Buffer
	serverCmd := exec.CommandContext(ctx, exe(server))
	serverCmd.Stdout, serverCmd.Stderr = &serverOut, &serverErr
	must(t, serverCmd.Start())
	clientCmd := exec.CommandContext(ctx, exe(client))
	var clientErr bytes.Buffer
	clientCmd.Stderr = &clientErr
	clientOut, err := clientCmd.Output()
	if err != nil {
		serverCmd.Process.Kill()
		serverCmd.Wait()
		t.Fatalf("%s: %v\n%s", client, err, clientErr.String())
	}
	if err := serverCmd.Wait(); err != nil {
		t.Fatalf("%s: %v\n%s", server, err, serverErr.String())
	}
	return serverOut.String(), string(clientOut)
}

// One test, so that no two pairs use a name at once.
func TestExamplesRunInPairs(t *testing.T) {
	guard(t, 300*time.Second)
	dir := buildExamples(t)

	if _, client := runPair(t, dir, "server", "client"); strings.TrimSpace(client) != "PING" {
		t.Errorf("client: %q", client)
	}

	server, client := runPair(t, dir, "echo_server", "echo_client")
	if strings.Join(strings.Fields(client), " ") != "hello shared memory" {
		t.Errorf("echo_client: %q", client)
	}
	if strings.TrimSpace(server) != "client gone" {
		t.Errorf("echo_server: %q", server)
	}

	server, client = runPair(t, dir, "echo_server", "game_loop")
	words := strings.Fields(client)
	if !strings.HasSuffix(strings.TrimSpace(client), "echoes in 60 frames") {
		t.Errorf("game_loop: %q", client)
	} else if echoes, err := strconv.Atoi(words[0]); err != nil || echoes < 50 || echoes > 60 {
		t.Errorf("game_loop: %q", client)
	}
	if strings.TrimSpace(server) != "client gone" {
		t.Errorf("echo_server: %q", server)
	}

	// Either may start first: the subscriber's JoinBroadcast waits for the broadcast, and the publisher for a
	// subscriber
	if _, subscriber := runPair(t, dir, "publisher", "subscriber"); strings.Join(strings.Fields(subscriber), " ") !=
		"tick 1 tick 2 tick 3" {
		t.Errorf("subscriber, after the publisher: %q", subscriber)
	}
	if subscriber, _ := runPair(t, dir, "subscriber", "publisher"); strings.Join(strings.Fields(subscriber), " ") !=
		"tick 1 tick 2 tick 3" {
		t.Errorf("subscriber, before the publisher: %q", subscriber)
	}

	// Either may start first: the consumer's JoinQueue waits for the queue, and the jobs wait in it for the consumer.
	// The queue promises no order: the jobs may come in any.
	if _, consumer := runPair(t, dir, "producer", "consumer"); jobsOf(consumer) != "job 1, job 2, job 3" {
		t.Errorf("consumer, after the producer: %q", consumer)
	}
	if consumer, _ := runPair(t, dir, "consumer", "producer"); jobsOf(consumer) != "job 1, job 2, job 3" {
		t.Errorf("consumer, before the producer: %q", consumer)
	}

	// Either may start first: the client's JoinService waits for the service, and the server's Receive for a request.
	// The replies come in the order of the requests.
	if _, client := runPair(t, dir, "service_server", "service_client"); strings.Join(lines(client), ", ") !=
		"PING 1, PING 2, PING 3" {
		t.Errorf("service_client, after the server: %q", client)
	}
	if client, _ := runPair(t, dir, "service_client", "service_server"); strings.Join(lines(client), ", ") !=
		"PING 1, PING 2, PING 3" {
		t.Errorf("service_client, before the server: %q", client)
	}
}

// jobsOf is the consumer example's output lines, sorted, joined by ", ".
func jobsOf(output string) string {
	jobs := lines(output)
	slices.Sort(jobs)
	return strings.Join(jobs, ", ")
}
