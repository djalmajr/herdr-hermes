package a2aserver

import (
	"context"
	"net/http"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/a2aproject/a2a-go/v2/a2aclient/agentcard"

	"github.com/djalmajr/nau/internal/a2acore"
)

// CredentialSource provides the Authorization header value of one
// outgoing call. Its value is never logged.
type CredentialSource interface {
	Authorization(ctx context.Context) (string, error)
}

// credentialInterceptor sets the Authorization service parameter (sent
// as the HTTP header by the REST transport) from the CredentialSource.
type credentialInterceptor struct {
	creds CredentialSource
}

// Before implements a2aclient.CallInterceptor.
func (i credentialInterceptor) Before(ctx context.Context, req *a2aclient.Request) (context.Context, any, error) {
	value, err := i.creds.Authorization(ctx)
	if err != nil {
		return ctx, nil, err
	}
	if value != "" {
		req.ServiceParams["authorization"] = []string{value}
	}
	return ctx, nil, nil
}

// After implements a2aclient.CallInterceptor.
func (credentialInterceptor) After(_ context.Context, _ *a2aclient.Response) error {
	return nil
}

// Client is the REST-only SDK client wrapper used by the CLI: the agent
// card is resolved from the base URL, only the HTTP+JSON transport is
// used, the credential source sets the Authorization header and every
// send asks to return immediately.
type Client struct {
	client *a2aclient.Client
	card   *a2a.AgentCard
}

// SendInput is one message the client sends: the correlation metadata,
// the target task or context and the text as data.
type SendInput struct {
	Project   string
	Job       string
	TaskID    string
	ContextID string
	Session   string
	MessageID string
	Text      string
	Epoch     int64
	Seq       int64
}

// Dial resolves the agent card at baseURL and opens the REST client.
// The client dials the given baseURL (the card interface URL is
// rewritten to it: the caller chooses the endpoint). creds may be nil
// (no header is sent) and hc may be nil (the SDK default client is
// used).
func Dial(ctx context.Context, baseURL string, creds CredentialSource, hc *http.Client) (*Client, error) {
	card, err := agentcard.NewResolver(hc).Resolve(ctx, baseURL)
	if err != nil {
		return nil, err
	}
	card = rewriteCardURL(card, baseURL)
	opts := []a2aclient.FactoryOption{
		a2aclient.WithConfig(a2aclient.Config{
			PreferredTransports: []a2a.TransportProtocol{a2a.TransportProtocolHTTPJSON},
		}),
		a2aclient.WithRESTTransport(hc),
	}
	if creds != nil {
		opts = append(opts, a2aclient.WithCallInterceptors(credentialInterceptor{creds: creds}))
	}
	c, err := a2aclient.NewFromCard(ctx, card, opts...)
	if err != nil {
		return nil, err
	}
	return &Client{client: c, card: card}, nil
}

// Card returns the resolved agent card.
func (c *Client) Card() *a2a.AgentCard { return c.card }

// rewriteCardURL points the card interfaces at baseURL (a copy: the
// resolved card is not mutated).
func rewriteCardURL(card *a2a.AgentCard, baseURL string) *a2a.AgentCard {
	if card == nil {
		return card
	}
	c := *card
	ifaces := make([]*a2a.AgentInterface, 0, len(card.SupportedInterfaces))
	for _, i := range card.SupportedInterfaces {
		if i == nil {
			ifaces = append(ifaces, nil)
			continue
		}
		ci := *i
		ci.URL = baseURL
		ifaces = append(ifaces, &ci)
	}
	c.SupportedInterfaces = ifaces
	return &c
}

// Send sends one user message with ReturnImmediately forced: it creates
// a new task or continues the one in TaskID. An empty MessageID is
// generated.
func (c *Client) Send(ctx context.Context, in SendInput) (a2a.SendMessageResult, error) {
	id := in.MessageID
	if id == "" {
		id = a2a.NewMessageID()
	}
	msg := &a2a.Message{
		ID:        id,
		Role:      a2a.MessageRoleUser,
		Parts:     a2a.ContentParts{a2a.NewTextPart(in.Text)},
		TaskID:    a2a.TaskID(in.TaskID),
		ContextID: in.ContextID,
		Metadata: map[string]any{
			a2acore.MetaSession: in.Session,
			a2acore.MetaEpoch:   float64(in.Epoch),
			a2acore.MetaSeq:     float64(in.Seq),
		},
	}
	if in.Project != "" {
		msg.Metadata[a2acore.MetaProject] = in.Project
	}
	if in.Job != "" {
		msg.Metadata[a2acore.MetaJob] = in.Job
	}
	return c.client.SendMessage(ctx, &a2a.SendMessageRequest{
		Message: msg,
		Config:  &a2a.SendMessageConfig{ReturnImmediately: true},
	})
}

// Get fetches one task by id.
func (c *Client) Get(ctx context.Context, taskID string) (*a2a.Task, error) {
	return c.client.GetTask(ctx, &a2a.GetTaskRequest{ID: a2a.TaskID(taskID)})
}

// Cancel cancels one task by id.
func (c *Client) Cancel(ctx context.Context, taskID string) (*a2a.Task, error) {
	return c.client.CancelTask(ctx, &a2a.CancelTaskRequest{ID: a2a.TaskID(taskID)})
}

// Close releases the client resources.
func (c *Client) Close() error { return c.client.Destroy() }
