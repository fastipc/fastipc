package fipc_test

// Every Go block of the documentation is code that runs: each block of the READMEs and of the website's pages must
// appear, line for line, in ../examples or in snippets_test.go (which run). Blank lines and indentation don't count.
// Outside a checkout of the repository only the module's README is there, and the other documents are skipped.

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// lines are the lines that count: trimmed, without blank ones.
func lines(text string) []string {
	var out []string
	for line := range strings.Lines(text) {
		if line = strings.TrimSpace(line); line != "" {
			out = append(out, line)
		}
	}
	return out
}

var htmlEntities = strings.NewReplacer("&lt;", "<", "&gt;", ">", "&quot;", `"`, "&#39;", "'", "&amp;", "&")

// goBlocks are the Go blocks of a Markdown file (```go) or an HTML page (<code class="language-go">); none if the file
// doesn't exist.
func goBlocks(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	text := strings.ReplaceAll(string(data), "\r\n", "\n")
	open, end := "```go\n", "\n```"
	if strings.HasSuffix(path, ".html") {
		open, end = `<code class="language-go">`, "</code>"
	}
	var blocks []string
	for {
		_, after, found := strings.Cut(text, open)
		if !found {
			return blocks
		}
		block, rest, closed := strings.Cut(after, end)
		if !closed {
			t.Fatalf("%s: an unclosed block", path)
		}
		blocks = append(blocks, htmlEntities.Replace(block))
		text = rest
	}
}

// contains reports whether block's lines appear, one after another, in source's.
func contains(source, block []string) bool {
	for i := 0; i+len(block) <= len(source); i++ {
		if slices.Equal(source[i:i+len(block)], block) {
			return true
		}
	}
	return false
}

func TestEveryGoBlockOfTheDocsRuns(t *testing.T) {
	var sources [][]string
	paths, err := filepath.Glob(filepath.Join("..", "examples", "*", "*.go"))
	must(t, err)
	for _, path := range append(paths, "snippets_test.go") {
		data, err := os.ReadFile(path)
		must(t, err)
		sources = append(sources, lines(string(data)))
	}

	root := repository(t)
	page := filepath.Join(root, "website", "go.html")
	docs := []string{
		filepath.Join("..", "README.md"),
		filepath.Join(root, "README.md"),
		page,
		filepath.Join(root, "website", "index.html"),
	}
	checked := 0
	for _, doc := range docs {
		for _, block := range goBlocks(t, doc) {
			want := lines(block)
			if !slices.ContainsFunc(sources, func(source []string) bool { return contains(source, want) }) {
				t.Errorf("%s: this block runs nowhere (../examples, snippets_test.go):\n%s", doc, strings.Join(want, "\n"))
			}
			checked++
		}
	}
	if checked < 7 {
		t.Errorf("only %d Go blocks found", checked)
	}
	// In a checkout, the website's Go page shows the binding call by call: at least ten Go blocks, as the other
	// bindings' pages do
	if _, err := os.Stat(filepath.Join(root, "website", "index.html")); err == nil {
		if blocks := goBlocks(t, page); len(blocks) < 10 {
			t.Errorf("%s: %d Go blocks, fewer than ten (is the page missing?)", page, len(blocks))
		}
	}
}
