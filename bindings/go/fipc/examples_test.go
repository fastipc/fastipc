package fipc_test

// The examples (../examples/*) run in pairs, as the README runs them: a server, then its client, each in a process of
// its own. They are built with the go command that runs the tests (go test puts its GOROOT/bin first on PATH).

import (
	"bytes"
	"context"
	"os/exec"
	"path/filepath"
	"runtime"
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

	if _, client := runPair(t, dir, "rpc_server", "rpc_client"); strings.TrimSpace(client) != "0 PING" {
		t.Errorf("rpc_client: %q", client)
	}
}
