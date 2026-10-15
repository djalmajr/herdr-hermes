package a2aserver

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient/agentcard"
)

func TestCardServedAndResolved(t *testing.T) {
	store, _ := newTestStore(t)
	auth := newTableAuth(testSubject("caller-1"))
	s := newTestServer(t, store, auth, fakeRoutes{owner: "owner-ref-1"})
	ts := serveServer(t, s)
	client := &http.Client{Timeout: testHTTPClientTimeout}

	// The raw JSON over 127.0.0.1: the protocol version is "1.0" on the
	// single HTTP+JSON interface, streaming is off and push is on.
	resp, err := client.Get(ts.URL + "/.well-known/agent-card.json")
	if err != nil {
		t.Fatalf("get card: %v", err)
	}
	raw, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read card: %v", err)
	}
	var envelope map[string]any
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatalf("card is not JSON: %v", err)
	}
	ifaces, ok := envelope["supportedInterfaces"].([]any)
	if !ok || len(ifaces) != 1 {
		t.Fatalf("supportedInterfaces = %v, want exactly one", envelope["supportedInterfaces"])
	}
	iface, _ := ifaces[0].(map[string]any)
	if string(a2a.Version) != "1.0" {
		t.Fatalf("SDK version = %s, want 1.0", a2a.Version)
	}
	if iface["protocolVersion"] != "1.0" {
		t.Errorf("protocolVersion = %v, want 1.0", iface["protocolVersion"])
	}
	if iface["protocolBinding"] != string(a2a.TransportProtocolHTTPJSON) {
		t.Errorf("protocolBinding = %v, want HTTP+JSON", iface["protocolBinding"])
	}
	caps, _ := envelope["capabilities"].(map[string]any)
	if caps["streaming"] != false {
		t.Errorf("streaming = %v, want false", caps["streaming"])
	}
	if caps["pushNotifications"] != true {
		t.Errorf("pushNotifications = %v, want true", caps["pushNotifications"])
	}

	// The resolved card carries the same shape.
	card, err := agentcard.NewResolver(client).Resolve(ctxT(t), ts.URL)
	if err != nil {
		t.Fatalf("resolve card: %v", err)
	}
	if card.Name != cardName || card.Version != "test-1" {
		t.Errorf("card = name %s version %s, want %s / test-1", card.Name, card.Version, cardName)
	}
	if len(card.SupportedInterfaces) != 1 {
		t.Fatalf("interfaces = %d, want 1", len(card.SupportedInterfaces))
	}
	if got := card.SupportedInterfaces[0].URL; got != "http://127.0.0.1:7430" {
		t.Errorf("interface url = %s, want the advertised base url", got)
	}
	if card.Capabilities.Streaming || !card.Capabilities.PushNotifications {
		t.Errorf("capabilities = %+v, want streaming off, push on", card.Capabilities)
	}
	if len(card.Skills) != 1 || card.Skills[0].ID != skillID {
		t.Errorf("skills = %+v, want the single %s skill", card.Skills, skillID)
	}
	if len(card.DefaultInputModes) != 1 || card.DefaultInputModes[0] != "text/plain" {
		t.Errorf("input modes = %v, want text/plain", card.DefaultInputModes)
	}
	if len(card.DefaultOutputModes) != 1 || card.DefaultOutputModes[0] != "text/plain" {
		t.Errorf("output modes = %v, want text/plain", card.DefaultOutputModes)
	}

	// The card is public: the resolver call above sent no credential.
	if s.Card() == nil {
		t.Fatal("server has no card")
	}
}
