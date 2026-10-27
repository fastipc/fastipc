package fipc

// What only the package itself can see: timeouts as milliseconds, the names of the results, the library's search, a
// receive that grows its buffer.

import (
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestTimeoutsInMilliseconds(t *testing.T) {
	cases := []struct {
		timeout time.Duration
		ms      int32
	}{
		{Forever, -1},
		{-time.Second, -1}, // any negative duration waits for ever
		{NoWait, 0},
		{time.Nanosecond, 1}, // a partial millisecond waits a whole one
		{1500 * time.Microsecond, 2},
		{5 * time.Second, 5000},
		{(math.MaxInt32 - 1) * time.Millisecond, math.MaxInt32 - 1},
		{(math.MaxInt32-1)*time.Millisecond + 1, -1}, // rounds up to MaxInt32
		{math.MaxInt32 * time.Millisecond, -1},
		{math.MaxInt64, -1},
	}
	for _, c := range cases {
		if got := millis(c.timeout); got != c.ms {
			t.Errorf("millis(%v) = %d, want %d", c.timeout, got, c.ms)
		}
	}
}

func TestResultNamesAreTheLibrarys(t *testing.T) {
	if err := load(); err != nil {
		t.Fatal(err)
	}
	for code := int32(1); code <= 11; code++ { // 11: no result, FIPC_UNKNOWN on both sides
		e := &Error{code: code}
		if got := lib.resultStr(code); got != e.Name() {
			t.Errorf("code %d: the library says %s, the binding %s", code, got, e.Name())
		}
	}
	if got := lib.resultStr(0); got != "FIPC_OK" {
		t.Errorf("code 0: %s", got)
	}
}

func TestAnOverrunSaysHowManyMessagesWereLost(t *testing.T) {
	for _, c := range []struct {
		lost uint64
		text string
	}{
		{0, "FIPC_OVERRUN: messages were lost"},
		{1, "FIPC_OVERRUN: 1 message was lost"},
		{2, "FIPC_OVERRUN: 2 messages were lost"},
		{math.MaxUint64, "FIPC_OVERRUN: 18446744073709551615 messages were lost"},
	} {
		if got := (&Error{code: codeOverrun, lost: c.lost}).Error(); got != c.text {
			t.Errorf("lost %d: %q, want %q", c.lost, got, c.text)
		}
	}
}

func TestTheLibraryIsFoundInTheCheckout(t *testing.T) {
	source := sourceDir()
	if source == "" {
		t.Skip("built with -trimpath: no source folder")
	}
	abs, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(abs, "build.zig")); err != nil {
		t.Skip("not in a checkout of the repository")
	}
	if root := repository(source); root != abs {
		t.Fatalf("repository(%s) = %q, want %q", source, root, abs)
	}
	if repository(t.TempDir()) != "" {
		t.Fatal("a folder outside a checkout is no repository")
	}
	_, file, ok := platform()
	if !ok {
		t.Skip("an unsupported platform")
	}
	dir := t.TempDir()
	if libraryIn(dir, file) != "" {
		t.Fatal("an empty folder holds no library")
	}
	if err := os.WriteFile(filepath.Join(dir, file), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if libraryIn(dir, file) != filepath.Join(dir, file) {
		t.Fatal("libraryIn doesn't find the file")
	}
}

// A receive that grows its buffer (Receive after FIPC_TOO_LARGE) on a work queue, where another consumer may take the
// message first: each retry waits only for what is left of the timeout, grows again for a longer message, and returns
// exactly the length of a shorter one. The fake receive stands for the library: it reports a longer message, then
// returns a shorter one.
func TestAReceiveThatGrowsItsBufferWaitsOnlyForWhatIsLeftOfItsTimeout(t *testing.T) {
	type call struct{ room, ms int }
	for _, c := range []struct {
		ms      int32
		elapsed time.Duration // since the receive began
		check   func(ms int) bool
	}{
		{1000, 300 * time.Millisecond, func(ms int) bool { return ms > 0 && ms <= 700 }},
		{1000, 2 * time.Second, func(ms int) bool { return ms == 0 }}, // run out: the retry polls
		{-1, time.Second, func(ms int) bool { return ms == -1 }},      // for ever stays for ever
		{0, time.Second, func(ms int) bool { return ms == 0 }},        // no wait stays no wait
	} {
		var calls []call
		recv := func(buf []byte, ms int32) (uintptr, int) {
			calls = append(calls, call{len(buf), int(ms)})
			if len(calls) == 1 {
				return codeTooLarge, 8000 // another consumer took the 5000-byte message; the next is longer
			}
			copy(buf, "a short one")
			return codeOK, len("a short one")
		}
		msg, r, n := receiveGrowing(codeTooLarge, 5000, c.ms, now()-instant(c.elapsed), recv)
		if r != codeOK || n != len("a short one") || string(msg) != "a short one" {
			t.Fatalf("ms %d: %q, result %d, length %d", c.ms, msg, r, n)
		}
		if len(calls) != 2 || calls[0].room != 5000 || calls[1].room != 8000 {
			t.Fatalf("ms %d: calls %v", c.ms, calls)
		}
		for _, call := range calls {
			if !c.check(call.ms) {
				t.Errorf("ms %d, %v elapsed: a retry waits %d ms", c.ms, c.elapsed, call.ms)
			}
		}
	}

	// A result other than FIPC_TOO_LARGE ends it, with that call's result and length
	msg, r, n := receiveGrowing(codeTooLarge, 100, 1000, now(), func([]byte, int32) (uintptr, int) {
		return codeTimeout, 0
	})
	if msg != nil || r != codeTimeout || n != 0 {
		t.Fatalf("a retry that timed out: %q, result %d, length %d", msg, r, n)
	}
}
