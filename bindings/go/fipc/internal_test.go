package fipc

// What only the package itself can see: timeouts as milliseconds, the names of the results, the library's search.

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
	for code := int32(1); code <= 8; code++ {
		e := &Error{code: code}
		if got := lib.resultStr(code); got != e.Name() {
			t.Errorf("code %d: the library says %s, the binding %s", code, got, e.Name())
		}
	}
	if got := lib.resultStr(0); got != "FIPC_OK" {
		t.Errorf("code 0: %s", got)
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
