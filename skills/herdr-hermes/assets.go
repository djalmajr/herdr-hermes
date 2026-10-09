// Package skill contains the agent skill bundled with herdr-hermes.
package skill

import (
	"embed"
	"io/fs"
)

// The skill is the only embedded content of this package: SKILL.md plus the
// references directory. The Go sources of the package itself are not part
// of the bundle.
//
//go:embed SKILL.md references
var assets embed.FS

// Files returns every embedded skill file keyed by its slash path relative
// to the skill root (for example "SKILL.md" or "references/protocol.md").
func Files() (map[string][]byte, error) {
	files := make(map[string][]byte)
	err := fs.WalkDir(assets, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		data, err := assets.ReadFile(p)
		if err != nil {
			return err
		}
		files[p] = data
		return nil
	})
	if err != nil {
		return nil, err
	}
	return files, nil
}
