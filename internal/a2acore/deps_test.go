package a2acore

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// forbiddenImports is the import boundary of the non-test files of this
// package: no A2A SDK, no HTTP, no database, no subprocess, and no
// Nau package outside the allowed set (stdlib, notify, jobapi).
func forbiddenImport(path string) bool {
	if strings.HasPrefix(path, "github.com/a2aproject/") {
		return true
	}
	switch path {
	case "net/http",
		"database/sql",
		"os/exec",
		"github.com/djalmajr/nau/internal/soho",
		"github.com/djalmajr/nau/internal/registry",
		"github.com/djalmajr/nau/internal/cli",
		"github.com/djalmajr/nau/internal/a2astore",
		"github.com/djalmajr/nau/internal/a2aserver":
		return true
	}
	return false
}

func TestNonTestImportsStayWithinBoundary(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading the package directory: %v", err)
	}
	checked := 0
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		checked++
		path := filepath.Join(".", e.Name())
		fset := token.NewFileSet()
		f, perr := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if perr != nil {
			t.Fatalf("parsing %s: %v", e.Name(), perr)
		}
		for _, imp := range f.Imports {
			p := strings.Trim(imp.Path.Value, `"`)
			if forbiddenImport(p) {
				t.Errorf("%s imports forbidden package %q", e.Name(), p)
			}
		}
	}
	if checked == 0 {
		t.Fatal("no non-test Go files found in the package directory")
	}
}
