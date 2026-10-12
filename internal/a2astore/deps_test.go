package a2astore

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// forbiddenImports is the dependency boundary of this package: the store
// never reaches for the HTTP stack, a subprocess, the Herdr transport,
// the CLI, the A2A server or the SDK client.
var forbiddenImports = []string{
	"net/http",
	"os/exec",
	"github.com/djalmajr/nau/internal/soho",
	"github.com/djalmajr/nau/internal/cli",
	"github.com/djalmajr/nau/internal/a2aserver",
	"github.com/a2aproject/a2a-go/v2/a2aclient",
}

// TestForbiddenImports parses the package's non-test files (imports only)
// and fails when one imports a forbidden package or a package below it.
func TestForbiddenImports(t *testing.T) {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	found := false
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		found = true
		path := filepath.Join(dir, name)
		f, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		for _, imp := range f.Imports {
			imported := strings.Trim(imp.Path.Value, `"`)
			for _, forbid := range forbiddenImports {
				if imported == forbid || strings.HasPrefix(imported, forbid+"/") {
					t.Errorf("%s imports %q (forbidden for this package)", name, imported)
				}
			}
		}
	}
	if !found {
		t.Fatal("no non-test .go files in the package")
	}
}
