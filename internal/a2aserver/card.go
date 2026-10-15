package a2aserver

import (
	"github.com/a2aproject/a2a-go/v2/a2a"
)

// The fixed identity text of the agent card.
const (
	cardName         = "Nau"
	cardDescription  = "Nau evaluates messages from other agents as data, stores them durably and resolves each task when the project owner or the guardian records a decision: an answer completes it, a question puts it back to the caller, a decline rejects it."
	skillID          = "owner-evaluation"
	skillName        = "Owner evaluation"
	skillDescription = "Accepts a message about a project, keeps it in the task history and resolves the task when the owner or guardian records an answer, a question or a decline."
	textMode         = "text/plain"
)

// buildCard builds the fixed agent card: one HTTP+JSON interface at
// cfg.BaseURL, streaming off, push notifications on, text/plain in and
// out and the single owner-evaluation skill.
func buildCard(cfg Config) *a2a.AgentCard {
	return &a2a.AgentCard{
		Name:        cardName,
		Description: cardDescription,
		Version:     cfg.Version,
		SupportedInterfaces: []*a2a.AgentInterface{
			a2a.NewAgentInterface(cfg.BaseURL, a2a.TransportProtocolHTTPJSON),
		},
		Capabilities: a2a.AgentCapabilities{
			Streaming:         false,
			PushNotifications: true,
		},
		DefaultInputModes:  []string{textMode},
		DefaultOutputModes: []string{textMode},
		Skills: []a2a.AgentSkill{{
			ID:          skillID,
			Name:        skillName,
			Description: skillDescription,
			InputModes:  []string{textMode},
			OutputModes: []string{textMode},
		}},
	}
}
