package a2acore

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
)

// canaryToken is a generated-looking fixture token (not a real
// credential); canaryProviderText is a provider error text that must
// never reach a returned error.
const (
	canaryToken        = "fixture-token-4f9a1c7e22b0"
	canaryProviderText = "provider-secret-text-7d31aa90"
)

// fakeIdentity is a counting IdentityProvider fixture.
type fakeIdentity struct {
	mu    sync.Mutex
	calls int
	last  string
	id    Identity
	err   error
}

// Verify implements IdentityProvider.
func (f *fakeIdentity) Verify(_ context.Context, token string) (Identity, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.last = token
	return f.id, f.err
}

// fakeGrants is a counting GrantSource fixture.
type fakeGrants struct {
	mu    sync.Mutex
	calls int
	got   Identity
	grant Grant
	err   error
}

// Grant implements GrantSource.
func (f *fakeGrants) Grant(_ context.Context, id Identity) (Grant, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.got = id
	return f.grant, f.err
}

// fakeProjectRoutes is a ProjectRoutes fixture.
type fakeProjectRoutes struct {
	owned       map[string][]string
	guardian    string
	ownedErr    error
	guardianErr error
	ownedCalls  int
}

// OwnedProjects implements ProjectRoutes.
func (f *fakeProjectRoutes) OwnedProjects(ref string) ([]string, error) {
	f.ownedCalls++
	return f.owned[ref], f.ownedErr
}

// GuardianRef implements ProjectRoutes.
func (f *fakeProjectRoutes) GuardianRef() (string, error) { return f.guardian, f.guardianErr }

func okIdentity() *fakeIdentity {
	return &fakeIdentity{id: Identity{Subject: "svc-1", Ref: "ref-1"}}
}

func okGrants() *fakeGrants {
	return &fakeGrants{grant: Grant{Projects: []string{"org/repo"}, Actions: slices.Clone(AllActions)}}
}

func TestBearerAuthenticatorSteps(t *testing.T) {
	longToken := strings.Repeat("a", maxBearerTokenBytes+1)
	maxToken := strings.Repeat("a", maxBearerTokenBytes)
	cases := []struct {
		name string
		auth func() BearerAuthenticator
		hdr  string
		// wantIs is the sentinel the error must wrap; "" means success.
		wantIs     error
		wantAbsent error
		wantText   string
		wantVerify int
		wantGrant  int
		wantToken  string
	}{
		{
			name:     "nil identity",
			auth:     func() BearerAuthenticator { return BearerAuthenticator{Grants: okGrants()} },
			hdr:      "Bearer " + canaryToken,
			wantIs:   ErrIdentityUnavailable,
			wantText: "identity provider not configured",
		},
		{
			name:     "nil grants",
			auth:     func() BearerAuthenticator { return BearerAuthenticator{Identity: okIdentity()} },
			hdr:      "Bearer " + canaryToken,
			wantIs:   ErrIdentityUnavailable,
			wantText: "identity provider not configured",
		},
		{
			name:     "both nil",
			auth:     func() BearerAuthenticator { return BearerAuthenticator{} },
			hdr:      "Bearer " + canaryToken,
			wantIs:   ErrIdentityUnavailable,
			wantText: "identity provider not configured",
		},
		{
			name:     "empty header",
			auth:     func() BearerAuthenticator { return BearerAuthenticator{Identity: okIdentity(), Grants: okGrants()} },
			hdr:      "",
			wantIs:   ErrUnauthenticated,
			wantText: "missing bearer credential",
		},
		{name: "wrong scheme", hdr: "Basic " + canaryToken, wantIs: ErrUnauthenticated, wantText: "malformed bearer credential"},
		{name: "scheme only", hdr: "Bearer", wantIs: ErrUnauthenticated, wantText: "malformed bearer credential"},
		{name: "scheme and space only", hdr: "Bearer ", wantIs: ErrUnauthenticated, wantText: "malformed bearer credential"},
		{name: "no space", hdr: "Bearer" + canaryToken, wantIs: ErrUnauthenticated, wantText: "malformed bearer credential"},
		{name: "two spaces", hdr: "Bearer  " + canaryToken, wantIs: ErrUnauthenticated, wantText: "malformed bearer credential"},
		{name: "tab separator", hdr: "Bearer\t" + canaryToken, wantIs: ErrUnauthenticated, wantText: "malformed bearer credential"},
		{name: "token with space", hdr: "Bearer abc def", wantIs: ErrUnauthenticated, wantText: "malformed bearer credential"},
		{name: "token with control", hdr: "Bearer abc\x01def", wantIs: ErrUnauthenticated, wantText: "malformed bearer credential"},
		{name: "token with newline", hdr: "Bearer abc\ndef", wantIs: ErrUnauthenticated, wantText: "malformed bearer credential"},
		{name: "token with non-ascii", hdr: "Bearer abc\u00e9def", wantIs: ErrUnauthenticated, wantText: "malformed bearer credential"},
		{name: "token with equals in the middle", hdr: "Bearer abc=def", wantIs: ErrUnauthenticated, wantText: "malformed bearer credential"},
		{name: "token only padding", hdr: "Bearer ===", wantIs: ErrUnauthenticated, wantText: "malformed bearer credential"},
		{name: "token over 4096 bytes", hdr: "Bearer " + longToken, wantIs: ErrUnauthenticated, wantText: "malformed bearer credential"},
		{name: "token of exactly 4096 bytes", hdr: "Bearer " + maxToken, wantVerify: 1, wantGrant: 1, wantToken: maxToken},
		{name: "lower case scheme", hdr: "bearer " + canaryToken, wantVerify: 1, wantGrant: 1, wantToken: canaryToken},
		{name: "upper case scheme", hdr: "BEARER " + canaryToken, wantVerify: 1, wantGrant: 1, wantToken: canaryToken},
		{name: "b64token alphabet with padding", hdr: "Bearer aB0._~+/-xyz==", wantVerify: 1, wantGrant: 1, wantToken: "aB0._~+/-xyz=="},
		{
			name: "provider unauthenticated",
			auth: func() BearerAuthenticator {
				p := okIdentity()
				p.err = fmt.Errorf("%w: %s", ErrUnauthenticated, canaryProviderText)
				return BearerAuthenticator{Identity: p, Grants: okGrants()}
			},
			hdr: "Bearer " + canaryToken, wantIs: ErrUnauthenticated, wantAbsent: ErrIdentityUnavailable,
			wantText: "credential rejected", wantVerify: 1, wantToken: canaryToken,
		},
		{
			name: "provider other error",
			auth: func() BearerAuthenticator {
				p := okIdentity()
				p.err = errors.New("dial failed for " + canaryToken + ": " + canaryProviderText)
				return BearerAuthenticator{Identity: p, Grants: okGrants()}
			},
			hdr: "Bearer " + canaryToken, wantIs: ErrIdentityUnavailable, wantAbsent: ErrUnauthenticated,
			wantText: "identity provider failed", wantVerify: 1, wantToken: canaryToken,
		},
		{
			name: "invalid subject",
			auth: func() BearerAuthenticator {
				p := okIdentity()
				p.id = Identity{Subject: "bad subject!", Ref: "r"}
				return BearerAuthenticator{Identity: p, Grants: okGrants()}
			},
			hdr: "Bearer " + canaryToken, wantIs: ErrIdentityUnavailable,
			wantText: "invalid identity", wantVerify: 1, wantToken: canaryToken,
		},
		{
			name: "empty subject",
			auth: func() BearerAuthenticator {
				p := okIdentity()
				p.id = Identity{}
				return BearerAuthenticator{Identity: p, Grants: okGrants()}
			},
			hdr: "Bearer " + canaryToken, wantIs: ErrIdentityUnavailable,
			wantText: "invalid identity", wantVerify: 1, wantToken: canaryToken,
		},
		{
			name: "subject local refused",
			auth: func() BearerAuthenticator {
				p := okIdentity()
				p.id = Identity{Subject: LocalSubjectID, Ref: "r"}
				return BearerAuthenticator{Identity: p, Grants: okGrants()}
			},
			hdr: "Bearer " + canaryToken, wantIs: ErrIdentityUnavailable,
			wantText: "invalid identity", wantVerify: 1, wantToken: canaryToken,
		},
		{
			name: "ref with control character",
			auth: func() BearerAuthenticator {
				p := okIdentity()
				p.id = Identity{Subject: "svc-1", Ref: "ref\x07-1"}
				return BearerAuthenticator{Identity: p, Grants: okGrants()}
			},
			hdr: "Bearer " + canaryToken, wantIs: ErrIdentityUnavailable,
			wantText: "invalid identity", wantVerify: 1, wantToken: canaryToken,
		},
		{
			name: "ref with newline",
			auth: func() BearerAuthenticator {
				p := okIdentity()
				p.id = Identity{Subject: "svc-1", Ref: "ref\n1"}
				return BearerAuthenticator{Identity: p, Grants: okGrants()}
			},
			hdr: "Bearer " + canaryToken, wantIs: ErrIdentityUnavailable,
			wantText: "invalid identity", wantVerify: 1, wantToken: canaryToken,
		},
		{
			name: "ref over 256 bytes",
			auth: func() BearerAuthenticator {
				p := okIdentity()
				p.id = Identity{Subject: "svc-1", Ref: strings.Repeat("r", maxIdentityRefBytes+1)}
				return BearerAuthenticator{Identity: p, Grants: okGrants()}
			},
			hdr: "Bearer " + canaryToken, wantIs: ErrIdentityUnavailable,
			wantText: "invalid identity", wantVerify: 1, wantToken: canaryToken,
		},
		{
			name: "ref not utf-8",
			auth: func() BearerAuthenticator {
				p := okIdentity()
				p.id = Identity{Subject: "svc-1", Ref: "ref\xff"}
				return BearerAuthenticator{Identity: p, Grants: okGrants()}
			},
			hdr: "Bearer " + canaryToken, wantIs: ErrIdentityUnavailable,
			wantText: "invalid identity", wantVerify: 1, wantToken: canaryToken,
		},
		{
			name: "ref of exactly 256 bytes and empty ref are valid",
			auth: func() BearerAuthenticator {
				p := okIdentity()
				p.id = Identity{Subject: "svc-1", Ref: strings.Repeat("r", maxIdentityRefBytes)}
				return BearerAuthenticator{Identity: p, Grants: okGrants()}
			},
			hdr: "Bearer " + canaryToken, wantVerify: 1, wantGrant: 1, wantToken: canaryToken,
		},
		{
			name: "grant error",
			auth: func() BearerAuthenticator {
				g := okGrants()
				g.err = errors.New("routes broke: " + canaryProviderText)
				return BearerAuthenticator{Identity: okIdentity(), Grants: g}
			},
			hdr: "Bearer " + canaryToken, wantIs: ErrIdentityUnavailable,
			wantText: "grant source failed", wantVerify: 1, wantGrant: 1, wantToken: canaryToken,
		},
		{
			name: "grant error wrapping unauthenticated stays unavailable",
			auth: func() BearerAuthenticator {
				g := okGrants()
				g.err = fmt.Errorf("%w: %s", ErrUnauthenticated, canaryProviderText)
				return BearerAuthenticator{Identity: okIdentity(), Grants: g}
			},
			hdr: "Bearer " + canaryToken, wantIs: ErrIdentityUnavailable, wantAbsent: ErrUnauthenticated,
			wantText: "grant source failed", wantVerify: 1, wantGrant: 1, wantToken: canaryToken,
		},
		{
			name: "invalid project empty",
			auth: func() BearerAuthenticator {
				g := okGrants()
				g.grant.Projects = []string{"org/repo", ""}
				return BearerAuthenticator{Identity: okIdentity(), Grants: g}
			},
			hdr: "Bearer " + canaryToken, wantIs: ErrIdentityUnavailable,
			wantText: "invalid grant", wantVerify: 1, wantGrant: 1, wantToken: canaryToken,
		},
		{
			name: "invalid project with control character",
			auth: func() BearerAuthenticator {
				g := okGrants()
				g.grant.Projects = []string{"org/re\npo"}
				return BearerAuthenticator{Identity: okIdentity(), Grants: g}
			},
			hdr: "Bearer " + canaryToken, wantIs: ErrIdentityUnavailable,
			wantText: "invalid grant", wantVerify: 1, wantGrant: 1, wantToken: canaryToken,
		},
		{
			name: "invalid project over 256 bytes",
			auth: func() BearerAuthenticator {
				g := okGrants()
				g.grant.Projects = []string{"org/" + strings.Repeat("p", maxGrantProjectLen)}
				return BearerAuthenticator{Identity: okIdentity(), Grants: g}
			},
			hdr: "Bearer " + canaryToken, wantIs: ErrIdentityUnavailable,
			wantText: "invalid grant", wantVerify: 1, wantGrant: 1, wantToken: canaryToken,
		},
		{
			name: "unknown action",
			auth: func() BearerAuthenticator {
				g := okGrants()
				g.grant.Actions = []Action{ActionSend, Action("admin:all")}
				return BearerAuthenticator{Identity: okIdentity(), Grants: g}
			},
			hdr: "Bearer " + canaryToken, wantIs: ErrIdentityUnavailable,
			wantText: "invalid grant", wantVerify: 1, wantGrant: 1, wantToken: canaryToken,
		},
		{
			name: "any project grant is valid",
			auth: func() BearerAuthenticator {
				g := okGrants()
				g.grant.Projects = []string{AnyProject}
				return BearerAuthenticator{Identity: okIdentity(), Grants: g}
			},
			hdr: "Bearer " + canaryToken, wantVerify: 1, wantGrant: 1, wantToken: canaryToken,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var ident *fakeIdentity
			var grants *fakeGrants
			var auth BearerAuthenticator
			if tc.auth != nil {
				auth = tc.auth()
			} else {
				auth = BearerAuthenticator{Identity: okIdentity(), Grants: okGrants()}
			}
			ident, _ = auth.Identity.(*fakeIdentity)
			grants, _ = auth.Grants.(*fakeGrants)
			s, err := auth.Authenticate(context.Background(), Credential{Authorization: tc.hdr, RemoteAddr: "127.0.0.1:1"})
			if tc.wantIs == nil {
				if err != nil {
					t.Fatalf("err = %v, want success", err)
				}
				if s.ID != "svc-1" {
					t.Errorf("subject id = %q, want svc-1", s.ID)
				}
			} else {
				if !errors.Is(err, tc.wantIs) {
					t.Fatalf("err = %v, want it to wrap %v", err, tc.wantIs)
				}
				if tc.wantAbsent != nil && errors.Is(err, tc.wantAbsent) {
					t.Errorf("err = %v, must not wrap %v", err, tc.wantAbsent)
				}
				if !strings.Contains(err.Error(), tc.wantText) {
					t.Errorf("err = %q, want it to contain %q", err.Error(), tc.wantText)
				}
				if s.ID != "" || s.Projects != nil || s.Actions != nil {
					t.Errorf("subject = %+v, want the zero subject on error", s)
				}
				for _, out := range []string{err.Error(), fmt.Sprintf("%v", err), fmt.Sprintf("%+v", err), fmt.Sprintf("%#v", err)} {
					for _, secret := range []string{canaryToken, canaryProviderText} {
						if strings.Contains(out, secret) {
							t.Errorf("error output carries %q: %s", secret, out)
						}
					}
				}
			}
			if ident != nil {
				if ident.calls != tc.wantVerify {
					t.Errorf("Verify calls = %d, want %d", ident.calls, tc.wantVerify)
				}
				if tc.wantVerify > 0 && ident.last != tc.wantToken {
					t.Errorf("Verify token = %q, want %q", ident.last, tc.wantToken)
				}
			}
			if grants != nil && grants.calls != tc.wantGrant {
				t.Errorf("Grant calls = %d, want %d", grants.calls, tc.wantGrant)
			}
		})
	}
}

func TestBearerAuthenticatorSuccessClonesSlices(t *testing.T) {
	grants := okGrants()
	auth := BearerAuthenticator{Identity: okIdentity(), Grants: grants}
	cred := Credential{Authorization: "Bearer " + canaryToken}

	first, err := auth.Authenticate(context.Background(), cred)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	if first.ID != "svc-1" || first.Ref != "ref-1" {
		t.Fatalf("subject = %+v, want svc-1/ref-1", first)
	}
	if !slices.Equal(first.Projects, []string{"org/repo"}) || !slices.Equal(first.Actions, AllActions) {
		t.Fatalf("subject grant = %v / %v", first.Projects, first.Actions)
	}
	// Mutating the returned slices changes neither the grant source's
	// own slices nor the next result.
	first.Projects[0] = "evil/repo"
	first.Actions[0] = Action("admin:all")
	if grants.grant.Projects[0] != "org/repo" || grants.grant.Actions[0] != ActionSend {
		t.Errorf("the grant source slices were aliased: %v / %v", grants.grant.Projects, grants.grant.Actions)
	}
	second, err := auth.Authenticate(context.Background(), cred)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if !slices.Equal(second.Projects, []string{"org/repo"}) || !slices.Equal(second.Actions, AllActions) {
		t.Errorf("second subject grant = %v / %v, want the unmodified grant", second.Projects, second.Actions)
	}
	if AllActions[0] != ActionSend {
		t.Errorf("AllActions was modified: %v", AllActions)
	}
}

func TestBearerAuthenticatorNoCache(t *testing.T) {
	ident := okIdentity()
	grants := okGrants()
	auth := BearerAuthenticator{Identity: ident, Grants: grants}
	cred := Credential{Authorization: "Bearer " + canaryToken}
	for i := 1; i <= 3; i++ {
		if _, err := auth.Authenticate(context.Background(), cred); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		if ident.calls != i || grants.calls != i {
			t.Fatalf("after call %d: Verify=%d Grant=%d, want %d each (no cache)", i, ident.calls, grants.calls, i)
		}
	}
	// The identity handed to the grant source is the provider's one.
	if grants.got != (Identity{Subject: "svc-1", Ref: "ref-1"}) {
		t.Errorf("grant source got %+v", grants.got)
	}
	// A revocation between two calls takes effect on the next one.
	ident.mu.Lock()
	ident.err = fmt.Errorf("%w: revoked", ErrUnauthenticated)
	ident.mu.Unlock()
	if _, err := auth.Authenticate(context.Background(), cred); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("after revocation err = %v, want ErrUnauthenticated", err)
	}
	if ident.calls != 4 {
		t.Errorf("Verify calls = %d, want 4", ident.calls)
	}
	if grants.calls != 3 {
		t.Errorf("Grant calls = %d, want 3 (not asked for a rejected token)", grants.calls)
	}
}

func TestRoutingGrants(t *testing.T) {
	cases := []struct {
		name      string
		routes    *fakeProjectRoutes
		ref       string
		wantProj  []string
		wantAct   []Action
		wantErr   bool
		wantOwned int
	}{
		{name: "empty ref", routes: &fakeProjectRoutes{owned: map[string][]string{"": {"org/a"}}, guardian: ""}, ref: ""},
		{name: "empty ref with empty guardian is not the guardian", routes: &fakeProjectRoutes{guardian: ""}, ref: ""},
		{
			name: "guardian ref", routes: &fakeProjectRoutes{guardian: "guardian-ref", owned: map[string][]string{"guardian-ref": {"org/a"}}},
			ref: "guardian-ref", wantProj: []string{AnyProject}, wantAct: AllActions,
		},
		{
			name: "owner ref sorted and deduplicated", routes: &fakeProjectRoutes{guardian: "guardian-ref", owned: map[string][]string{"owner-1": {"org/z", "org/a", "org/z", "org/m"}}},
			ref: "owner-1", wantProj: []string{"org/a", "org/m", "org/z"}, wantAct: AllActions, wantOwned: 1,
		},
		{
			name: "owner ref without guardian", routes: &fakeProjectRoutes{owned: map[string][]string{"owner-1": {"org/a"}}},
			ref: "owner-1", wantProj: []string{"org/a"}, wantAct: AllActions, wantOwned: 1,
		},
		{name: "unknown ref", routes: &fakeProjectRoutes{guardian: "guardian-ref", owned: map[string][]string{"owner-1": {"org/a"}}}, ref: "stranger", wantOwned: 1},
		{
			name:   "owned projects error",
			routes: &fakeProjectRoutes{ownedErr: errors.New("registry broke: " + canaryProviderText)},
			ref:    "owner-1", wantErr: true, wantOwned: 1,
		},
		{
			name:   "guardian error",
			routes: &fakeProjectRoutes{guardianErr: errors.New("registry broke: " + canaryProviderText), owned: map[string][]string{"owner-1": {"org/a"}}},
			ref:    "owner-1", wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g, err := RoutingGrants{Routes: tc.routes}.Grant(context.Background(), Identity{Subject: "svc-1", Ref: tc.ref})
			if tc.wantErr {
				if !errors.Is(err, ErrIdentityUnavailable) {
					t.Fatalf("err = %v, want ErrIdentityUnavailable", err)
				}
				if errors.Is(err, ErrUnauthenticated) {
					t.Errorf("err = %v, must not wrap ErrUnauthenticated", err)
				}
				for _, out := range []string{err.Error(), fmt.Sprintf("%+v", err)} {
					if strings.Contains(out, canaryProviderText) {
						t.Errorf("error output carries the routes error text: %s", out)
					}
				}
				if len(g.Projects) != 0 || len(g.Actions) != 0 {
					t.Errorf("grant = %+v, want empty on error", g)
				}
			} else if err != nil {
				t.Fatalf("err = %v, want nil", err)
			}
			if !slices.Equal(g.Projects, tc.wantProj) {
				t.Errorf("projects = %v, want %v", g.Projects, tc.wantProj)
			}
			if !slices.Equal(g.Actions, tc.wantAct) {
				t.Errorf("actions = %v, want %v", g.Actions, tc.wantAct)
			}
			if tc.routes.ownedCalls != tc.wantOwned {
				t.Errorf("OwnedProjects calls = %d, want %d", tc.routes.ownedCalls, tc.wantOwned)
			}
		})
	}
}

func TestRoutingGrantsNilRoutesAndFreshRead(t *testing.T) {
	if _, err := (RoutingGrants{}).Grant(context.Background(), Identity{Subject: "svc-1", Ref: "r"}); !errors.Is(err, ErrIdentityUnavailable) {
		t.Errorf("nil routes err = %v, want ErrIdentityUnavailable", err)
	}
	routes := &fakeProjectRoutes{owned: map[string][]string{"owner-1": {"org/a"}}}
	rg := RoutingGrants{Routes: routes}
	id := Identity{Subject: "svc-1", Ref: "owner-1"}
	g1, _ := rg.Grant(context.Background(), id)
	routes.owned["owner-1"] = []string{"org/b"}
	g2, _ := rg.Grant(context.Background(), id)
	if !slices.Equal(g1.Projects, []string{"org/a"}) || !slices.Equal(g2.Projects, []string{"org/b"}) {
		t.Errorf("grants = %v then %v, want a fresh read per call", g1.Projects, g2.Projects)
	}
	// The result never aliases the routes' own slice.
	routes.owned["owner-1"] = []string{"org/b", "org/a"}
	g3, _ := rg.Grant(context.Background(), id)
	g3.Projects[0] = "evil/repo"
	if routes.owned["owner-1"][0] != "org/b" || routes.owned["owner-1"][1] != "org/a" {
		t.Errorf("routes slice was modified: %v", routes.owned["owner-1"])
	}
}

// TestBearerOverRoutingGrants wires the two real implementations: the
// grant of a guardian reference is wide, an owner's is its projects, and
// a subject without a reference has no authorization at all.
func TestBearerOverRoutingGrants(t *testing.T) {
	routes := &fakeProjectRoutes{guardian: "guardian-ref", owned: map[string][]string{"owner-1": {"org/a"}}}
	cases := []struct {
		ref      string
		wantProj []string
		wantAct  int
	}{
		{ref: "guardian-ref", wantProj: []string{AnyProject}, wantAct: len(AllActions)},
		{ref: "owner-1", wantProj: []string{"org/a"}, wantAct: len(AllActions)},
		{ref: "", wantProj: nil, wantAct: 0},
		{ref: "stranger", wantProj: nil, wantAct: 0},
	}
	for _, tc := range cases {
		ident := okIdentity()
		ident.id = Identity{Subject: "svc-1", Ref: tc.ref}
		auth := BearerAuthenticator{Identity: ident, Grants: RoutingGrants{Routes: routes}}
		s, err := auth.Authenticate(context.Background(), Credential{Authorization: "Bearer " + canaryToken})
		if err != nil {
			t.Fatalf("ref %q: %v", tc.ref, err)
		}
		if !slices.Equal(s.Projects, tc.wantProj) || len(s.Actions) != tc.wantAct {
			t.Errorf("ref %q: subject grant = %v / %v", tc.ref, s.Projects, s.Actions)
		}
		if tc.wantAct == 0 {
			if err := (SubjectPolicy{}).Authorize(s, ActionSend, "org/a"); !errors.Is(err, ErrForbidden) {
				t.Errorf("ref %q: authorize err = %v, want ErrForbidden", tc.ref, err)
			}
		}
	}
	// A routes failure surfaces as an unavailable identity provider.
	routes.ownedErr = errors.New(canaryProviderText)
	ident := okIdentity()
	ident.id = Identity{Subject: "svc-1", Ref: "owner-1"}
	auth := BearerAuthenticator{Identity: ident, Grants: RoutingGrants{Routes: routes}}
	_, err := auth.Authenticate(context.Background(), Credential{Authorization: "Bearer " + canaryToken})
	if !errors.Is(err, ErrIdentityUnavailable) || strings.Contains(fmt.Sprintf("%+v", err), canaryProviderText) {
		t.Errorf("routes failure err = %v, want ErrIdentityUnavailable without the routes text", err)
	}
}
