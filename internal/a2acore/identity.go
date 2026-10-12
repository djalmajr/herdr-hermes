package a2acore

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"
)

// ErrIdentityUnavailable reports that the injected identity provider or
// grant source is not configured, failed, or answered something invalid.
// It is distinct from ErrUnauthenticated (a credential that is missing,
// malformed, unknown, expired or revoked). Error text never carries the
// credential, the provider's own error text or any provider metadata.
var ErrIdentityUnavailable = errors.New("a2a: identity provider unavailable")

// LocalSubjectID is the subject id reserved for the loopback local
// operator. A provider identity can never claim it.
const LocalSubjectID = "local"

// Bounds of the bearer credential and of the values a provider returns.
const (
	maxBearerTokenBytes = 4096
	maxIdentityRefBytes = 256
	maxGrantProjectLen  = 256
)

// Identity is what the existing identity source asserts for one bearer
// token: the stable subject id (SubjectPattern, never LocalSubjectID)
// and the herdr-soho reference the subject speaks for ("" when none).
type Identity struct {
	Subject string
	Ref     string
}

// IdentityProvider is the injected existing identity source. Verify is
// called on EVERY request; revocation and expiry live in the source and
// nothing in Nau caches its answer. An error wrapping ErrUnauthenticated
// means the token is unknown, expired or revoked; any other error means
// the provider is unavailable.
type IdentityProvider interface {
	Verify(ctx context.Context, token string) (Identity, error)
}

// Grant is the project authorization of one identity.
type Grant struct {
	Projects []string // "<org>/<repo>" values, or AnyProject
	Actions  []Action
}

// GrantSource maps a verified identity to its grant, read on every
// request (never cached).
type GrantSource interface {
	Grant(ctx context.Context, id Identity) (Grant, error)
}

// ProjectRoutes is the reverse view of the existing owner/guardian
// routing that RoutingGrants needs: the projects whose registered owner
// is ref, and the guardian reference ("" when none).
type ProjectRoutes interface {
	OwnedProjects(ref string) ([]string, error)
	GuardianRef() (string, error)
}

// RoutingGrants is the GrantSource over the existing owner/guardian
// routing, re-read on every call: a Ref of "" is an empty grant (no
// projects, no actions); a Ref equal to the guardian reference is
// AnyProject with AllActions; a Ref that owns projects is those projects
// (sorted, deduplicated) with AllActions; any other Ref is an empty
// grant. A routes error is an error wrapping ErrIdentityUnavailable with
// a fixed text.
type RoutingGrants struct{ Routes ProjectRoutes }

// Grant implements GrantSource.
func (g RoutingGrants) Grant(_ context.Context, id Identity) (Grant, error) {
	if g.Routes == nil {
		return Grant{}, fmt.Errorf("%w: routes not configured", ErrIdentityUnavailable)
	}
	if id.Ref == "" {
		return Grant{}, nil
	}
	guardian, err := g.Routes.GuardianRef()
	if err != nil {
		return Grant{}, fmt.Errorf("%w: routes unavailable", ErrIdentityUnavailable)
	}
	if guardian != "" && guardian == id.Ref {
		return Grant{Projects: []string{AnyProject}, Actions: slices.Clone(AllActions)}, nil
	}
	owned, err := g.Routes.OwnedProjects(id.Ref)
	if err != nil {
		return Grant{}, fmt.Errorf("%w: routes unavailable", ErrIdentityUnavailable)
	}
	projects := slices.Clone(owned)
	slices.Sort(projects)
	projects = slices.Compact(projects)
	if len(projects) == 0 {
		return Grant{}, nil
	}
	return Grant{Projects: projects, Actions: slices.Clone(AllActions)}, nil
}

// BearerAuthenticator is the Authenticator of the injected identity
// provider. It keeps no state between calls (no cache): the provider
// and the grant source are asked on every request, so a revoked token
// or a changed grant takes effect on the next call. It fails closed:
// with no provider or no grant source it authenticates nobody.
type BearerAuthenticator struct {
	Identity IdentityProvider
	Grants   GrantSource
}

// Authenticate implements Authenticator. Every error text is fixed: the
// token, the error text of the provider or of the grant source and any
// provider metadata never reach a returned error.
func (b BearerAuthenticator) Authenticate(ctx context.Context, c Credential) (Subject, error) {
	if b.Identity == nil || b.Grants == nil {
		return Subject{}, fmt.Errorf("%w: identity provider not configured", ErrIdentityUnavailable)
	}
	if c.Authorization == "" {
		return Subject{}, fmt.Errorf("%w: missing bearer credential", ErrUnauthenticated)
	}
	token, ok := parseBearer(c.Authorization)
	if !ok {
		return Subject{}, fmt.Errorf("%w: malformed bearer credential", ErrUnauthenticated)
	}
	id, err := b.Identity.Verify(ctx, token)
	if err != nil {
		if errors.Is(err, ErrUnauthenticated) {
			return Subject{}, fmt.Errorf("%w: credential rejected", ErrUnauthenticated)
		}
		return Subject{}, fmt.Errorf("%w: identity provider failed", ErrIdentityUnavailable)
	}
	if !validIdentity(id) {
		return Subject{}, fmt.Errorf("%w: identity provider returned an invalid identity", ErrIdentityUnavailable)
	}
	grant, err := b.Grants.Grant(ctx, id)
	if err != nil {
		return Subject{}, fmt.Errorf("%w: grant source failed", ErrIdentityUnavailable)
	}
	if !validGrant(grant) {
		return Subject{}, fmt.Errorf("%w: grant source returned an invalid grant", ErrIdentityUnavailable)
	}
	return Subject{
		ID:       id.Subject,
		Ref:      id.Ref,
		Projects: slices.Clone(grant.Projects),
		Actions:  slices.Clone(grant.Actions),
	}, nil
}

// parseBearer extracts the token of an Authorization value: the scheme
// "Bearer" (case-insensitive), exactly one space and an RFC 6750 b64token
// ([A-Za-z0-9._~+/-]+ followed by any number of "=") of at most
// maxBearerTokenBytes bytes.
func parseBearer(h string) (string, bool) {
	const scheme = "Bearer"
	if len(h) < len(scheme)+2 || !strings.EqualFold(h[:len(scheme)], scheme) || h[len(scheme)] != ' ' {
		return "", false
	}
	token := h[len(scheme)+1:]
	if len(token) == 0 || len(token) > maxBearerTokenBytes {
		return "", false
	}
	body := strings.TrimRight(token, "=")
	if body == "" {
		return "", false
	}
	for i := 0; i < len(body); i++ {
		if !b64tokenByte(body[i]) {
			return "", false
		}
	}
	return token, true
}

// b64tokenByte reports whether c is in [A-Za-z0-9._~+/-].
func b64tokenByte(c byte) bool {
	switch {
	case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		return true
	}
	return strings.IndexByte("._~+/-", c) >= 0
}

// validIdentity reports whether a provider identity is acceptable: a
// subject that matches SubjectPattern and is not the reserved local
// subject, and a reference of at most maxIdentityRefBytes bytes that is
// valid UTF-8 and has no control character.
func validIdentity(id Identity) bool {
	if !SubjectPattern.MatchString(id.Subject) || id.Subject == LocalSubjectID {
		return false
	}
	if len(id.Ref) > maxIdentityRefBytes || !utf8.ValidString(id.Ref) {
		return false
	}
	return !hasControl(id.Ref)
}

// validGrant reports whether a grant is acceptable: every project is
// AnyProject or a non-empty value of at most maxGrantProjectLen bytes
// with no control character, and every action is one of AllActions.
func validGrant(g Grant) bool {
	for _, p := range g.Projects {
		if p == AnyProject {
			continue
		}
		if p == "" || len(p) > maxGrantProjectLen || hasControl(p) {
			return false
		}
	}
	for _, a := range g.Actions {
		if !slices.Contains(AllActions, a) {
			return false
		}
	}
	return true
}

// hasControl reports whether s has a Unicode control character.
func hasControl(s string) bool {
	return strings.IndexFunc(s, unicode.IsControl) >= 0
}
