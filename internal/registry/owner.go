package registry

import (
	"regexp"
	"strings"
)

// WorkspaceName is the owner parsed from an hm-<agent>-<type>-<ref>
// workspace label.
type WorkspaceName struct {
	Agente   string
	Trabalho string
	Ref      string
}

var (
	workspaceTokenPattern = regexp.MustCompile(`^[a-z0-9]+$`)
	workspaceTypeTokens   = map[string]bool{"impl": true, "review": true, "fix": true, "plan": true}
)

// ParseWorkspaceName resolves an hm-<agent>-<type>-<ref> label: every
// token must be non-empty lower-case alphanumeric, and exactly one type
// token with tokens on both sides is accepted; anything else is
// unresolved, never guessed.
func ParseWorkspaceName(label string) (WorkspaceName, bool) {
	if !strings.HasPrefix(label, "hm-") {
		return WorkspaceName{}, false
	}
	tokens := strings.Split(label[len("hm-"):], "-")
	for _, token := range tokens {
		if !workspaceTokenPattern.MatchString(token) {
			return WorkspaceName{}, false
		}
	}
	typeIndex := -1
	for i, token := range tokens {
		if !workspaceTypeTokens[token] || i == 0 || i == len(tokens)-1 {
			continue
		}
		if typeIndex != -1 {
			return WorkspaceName{}, false
		}
		typeIndex = i
	}
	if typeIndex == -1 {
		return WorkspaceName{}, false
	}
	return WorkspaceName{
		Agente:   strings.Join(tokens[:typeIndex], "-"),
		Trabalho: tokens[typeIndex],
		Ref:      strings.Join(tokens[typeIndex+1:], "-"),
	}, true
}

// JobIDFromWorkspace returns the contract id of a job-<id> workspace.
func JobIDFromWorkspace(label string) (string, bool) {
	const prefix = "job-"
	if !strings.HasPrefix(label, prefix) {
		return "", false
	}
	id := label[len(prefix):]
	if !jobIDPattern.MatchString(id) {
		return "", false
	}
	return id, true
}
