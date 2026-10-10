// Package registry is the current-state registry behind `herdr-hermes serve`
// (DJA-229). It keeps, in one SQLite database, only what is true now across
// the nodes: the active jobs with their owner, card, workspace, branch, pull
// request, phase, idle time and human waits; the nodes with their versions,
// runners and maintenance flag; the panes waiting for a person; the folders
// awaiting a deletion decision; the holds and priorities; the claims of the
// pre-dispatch check; and the last events of each job. History and decisions
// live elsewhere.
//
// State reaches the registry from two producers: the bridge push (one outbox
// record per request, the format of docs/protocol.md, applied by Ingest) and
// the read-only Herdr inventory poll (`herdr [--machine <label>] workspace
// list`, `agent list` and `status server`, applied by ApplyPoll). Nothing is
// inferred that no producer reports: an unknown value stays null.
//
// Check is the mandatory pre-dispatch check: it refuses a request that
// overlaps active work, an unexpired claim or a hold, and otherwise records a
// claim, atomically in one immediate SQLite transaction.
package registry

import (
	"errors"
	"time"
)

// Defaults and bounds.
const (
	// SchemaVersion is the SQLite user_version of the registry schema.
	SchemaVersion = 2
	// DefaultEventsPerJob is the number of most recent events kept per job.
	DefaultEventsPerJob = 20
	// DefaultOnlineWindow: a node is online when its last contact (a
	// successful poll or an ingested record) is at most this old.
	DefaultOnlineWindow = 3 * time.Minute
	// DefaultClaimTTL, MinClaimTTL and MaxClaimTTL bound a claim's life.
	DefaultClaimTTL = time.Hour
	MinClaimTTL     = time.Minute
	MaxClaimTTL     = 24 * time.Hour
	// TombstoneTTL is how long a closed job or session is remembered so a
	// late or replayed record cannot bring it back.
	TombstoneTTL = 7 * 24 * time.Hour
	// MaxResumo is the cut of an event resumo, in code points.
	MaxResumo = 280

	// Admission caps: what the registry admits before it refuses.
	MaxNodes    = 64
	MaxJobs     = 1000
	MaxClaims   = 1000
	MaxPolicies = 1000
	MaxFolders  = 1000

	// MaxInventoryBytes bounds a full inventory answer: with every table
	// at its cap and every kept value at its length bound and fully
	// escaped, the JSON stays under it. The jobs client reads up to it.
	MaxInventoryBytes = 256 << 20
)

// Job origins.
const (
	// OrigemJob: a herdr-soho contract job, keyed by its job id.
	OrigemJob = "job"
	// OrigemWorkspace: an hm-<agent>-<type>-<ref> Herdr workspace seen by
	// the poll, keyed by "ws:" + its workspace id.
	OrigemWorkspace = "workspace"
	// OrigemSessao: an open session record, keyed by
	// "sessao:" + projeto + "#" + branch.
	OrigemSessao = "sessao"
)

// Owner sources.
const (
	AgenteFonteNome  = "hm_name"
	AgenteFonteClaim = "claim"
)

// Wait types.
const (
	// EsperaPane: Herdr reports the pane's agent_status as blocked (a
	// trust, permission or question dialog).
	EsperaPane = "pane_blocked"
	// EsperaQuestion and EsperaBlocked: the job's latest event says it
	// waits for a person (contract question, blocked, or a terminal exit
	// 7, 11 or 14).
	EsperaQuestion = "question"
	EsperaBlocked  = "blocked"
)

// Check statuses.
const (
	CheckOK        = "ok"
	CheckDuplicate = "duplicate"
	CheckHold      = "hold"
)

// Conflict kinds and fields.
const (
	ConflitoJob   = "job"
	ConflitoClaim = "claim"
	ConflitoHold  = "hold"

	CampoCard      = "card"
	CampoWorkspace = "workspace"
	CampoRef       = "ref"
	CampoBranch    = "branch"
	CampoPR        = "pr"
	CampoProjeto   = "projeto"
	CampoAgente    = "agente"
)

// Policy scopes.
const (
	EscopoProjeto = "projeto"
	EscopoAgente  = "agente"
	EscopoCard    = "card"
)

// ErrInvalid is wrapped by every validation error; the message after the
// prefix is safe to show to the caller.
var ErrInvalid = errors.New("registry: invalid input")

// ErrNotFound: the claim, policy or folder does not exist.
var ErrNotFound = errors.New("registry: not found")

// ErrSchema: the database was written by a newer schema.
var ErrSchema = errors.New("registry: unsupported schema version")

// Options configures a Store.
type Options struct {
	// Now is the clock; time.Now when nil.
	Now func() time.Time
	// EventsPerJob caps the events kept per job; DefaultEventsPerJob when
	// zero or negative.
	EventsPerJob int
	// OnlineWindow; DefaultOnlineWindow when zero or negative.
	OnlineWindow time.Duration
}

// Event is one stored job event. Refs keeps only the allowlisted keys
// (EventRefsKeys); local paths (report, brief) are never stored.
type Event struct {
	Seq    int64          `json:"seq"`
	TS     string         `json:"ts"`
	Tipo   string         `json:"tipo"`
	Resumo string         `json:"resumo"`
	Refs   map[string]any `json:"refs,omitempty"`
}

// EventRefsKeys are the contract refs keys the registry keeps.
var EventRefsKeys = []string{"agente", "papel", "pane", "sha", "pr", "motivo", "exit", "seq_ref"}

// Job is one active unit of work on one node. Pointer fields are null when
// no producer reported them.
type Job struct {
	Maquina         string  `json:"maquina"`
	Chave           string  `json:"chave"`
	Origem          string  `json:"origem"`
	JobID           *string `json:"job_id"`
	Workspace       *string `json:"workspace"`
	WorkspaceID     *string `json:"workspace_id"`
	Agente          *string `json:"agente"`
	AgenteFonte     *string `json:"agente_fonte"`
	Trabalho        *string `json:"trabalho"`
	Ref             *string `json:"ref"`
	Card            *string `json:"card"`
	Projeto         *string `json:"projeto"`
	Branch          *string `json:"branch"`
	PR              *string `json:"pr"`
	Fase            *string `json:"fase"`
	HerdrStatus     *string `json:"herdr_status"`
	Terminal        bool    `json:"terminal"`
	Saida           *int    `json:"saida"`
	EsperandoHumano bool    `json:"esperando_humano"`
	EsperaMotivo    *string `json:"espera_motivo"`
	UltimoSeq       int64   `json:"ultimo_seq"`
	UltimaAtividade *string `json:"ultima_atividade"`
	OciosoS         *int64  `json:"ocioso_s"`
	Obsoleto        bool    `json:"obsoleto"`
	Eventos         []Event `json:"eventos"`
}

// Node is one machine.
type Node struct {
	Maquina          string   `json:"maquina"`
	Online           bool     `json:"online"`
	UltimoContato    *string  `json:"ultimo_contato"`
	UltimoPoll       *string  `json:"ultimo_poll"`
	PollStatus       *string  `json:"poll_status"`
	HerdrVersion     *string  `json:"herdr_version"`
	SohoVersion      *string  `json:"soho_version"`
	Runners          []string `json:"runners"`
	Manutencao       bool     `json:"manutencao"`
	ManutencaoMotivo *string  `json:"manutencao_motivo"`
}

// Wait is one pane or job waiting for a person.
type Wait struct {
	Maquina string  `json:"maquina"`
	Tipo    string  `json:"tipo"`
	Chave   *string `json:"chave"`
	Pane    *string `json:"pane"`
	Agente  *string `json:"agente"`
	Resumo  *string `json:"resumo"`
	Desde   string  `json:"desde"`
}

// Claim is a reservation made by a successful Check.
type Claim struct {
	ID        string  `json:"id"`
	Agente    string  `json:"agente"`
	Maquina   *string `json:"maquina"`
	Card      *string `json:"card"`
	Projeto   *string `json:"projeto"`
	Branch    *string `json:"branch"`
	PR        *string `json:"pr"`
	Workspace *string `json:"workspace"`
	Ref       *string `json:"ref"`
	Criado    string  `json:"criado"`
	Expira    string  `json:"expira"`
}

// Policy is a hold or a priority for a project, an agent or a card.
type Policy struct {
	Escopo     string  `json:"escopo"`
	Valor      string  `json:"valor"`
	Hold       bool    `json:"hold"`
	Prioridade *int    `json:"prioridade"`
	Motivo     *string `json:"motivo"`
	Atualizado string  `json:"atualizado"`
}

// Folder is a folder retained on a node, awaiting a person's decision. It is
// a record only: the registry never deletes anything and a record is never
// a permission to delete.
type Folder struct {
	Maquina      string  `json:"maquina"`
	Caminho      string  `json:"caminho"`
	TamanhoBytes *int64  `json:"tamanho_bytes"`
	Pedido       string  `json:"pedido"`
	Job          *string `json:"job"`
	Criado       string  `json:"criado"`
}

// Inventory is the whole current state, as `GET /v1/jobs` returns it.
type Inventory struct {
	Gerado    string   `json:"gerado"`
	Nodes     []Node   `json:"nodes"`
	Jobs      []Job    `json:"jobs"`
	Esperas   []Wait   `json:"esperas"`
	Claims    []Claim  `json:"claims"`
	Politicas []Policy `json:"politicas"`
	Pastas    []Folder `json:"pastas"`
}

// Filter narrows an Inventory; empty fields match everything.
type Filter struct {
	Maquina string
	Agente  string
}

// CheckRequest is the input of the pre-dispatch check.
type CheckRequest struct {
	Agente    string `json:"agente"`
	Maquina   string `json:"maquina,omitempty"`
	Card      string `json:"card,omitempty"`
	Projeto   string `json:"projeto,omitempty"`
	Branch    string `json:"branch,omitempty"`
	PR        string `json:"pr,omitempty"`
	Workspace string `json:"workspace,omitempty"`
	TTLS      int    `json:"ttl_s,omitempty"`
}

// Conflict is one reason a check was refused.
type Conflict struct {
	Tipo    string  `json:"tipo"`
	Campo   string  `json:"campo"`
	Valor   string  `json:"valor"`
	Maquina *string `json:"maquina,omitempty"`
	Chave   *string `json:"chave,omitempty"`
	Agente  *string `json:"agente,omitempty"`
	Motivo  *string `json:"motivo,omitempty"`
}

// CheckResult is the outcome of the pre-dispatch check.
type CheckResult struct {
	Status    string     `json:"status"`
	Claim     *Claim     `json:"claim,omitempty"`
	Conflitos []Conflict `json:"conflitos,omitempty"`
}

// PolicyInput sets a hold or a priority.
type PolicyInput struct {
	Escopo     string `json:"escopo"`
	Valor      string `json:"valor"`
	Hold       bool   `json:"hold"`
	Prioridade *int   `json:"prioridade,omitempty"`
	Motivo     string `json:"motivo,omitempty"`
}

// FolderInput records a retained folder.
type FolderInput struct {
	Maquina      string `json:"maquina"`
	Caminho      string `json:"caminho"`
	TamanhoBytes *int64 `json:"tamanho_bytes,omitempty"`
	Pedido       string `json:"pedido"`
	Job          string `json:"job,omitempty"`
}

// NodeUpdate changes the operator-reported facts of a node; a nil field is
// left unchanged.
type NodeUpdate struct {
	Maquina          string   `json:"-"`
	Manutencao       *bool    `json:"manutencao,omitempty"`
	ManutencaoMotivo *string  `json:"manutencao_motivo,omitempty"`
	SohoVersion      *string  `json:"soho_version,omitempty"`
	Runners          []string `json:"runners,omitempty"`
}

// PolledWorkspace is one workspace of a `workspace list` answer.
type PolledWorkspace struct {
	ID          string
	Label       string
	AgentStatus string
}

// PolledPane is one agent of an `agent list` answer, reduced to the
// allowlisted fields (never cwd, titles or terminal content).
type PolledPane struct {
	PaneID      string
	WorkspaceID string
	Name        string
	AgentStatus string
}

// PollResult is the outcome of polling one machine.
type PollResult struct {
	Maquina string
	At      time.Time
	// OK: both the workspace list and the agent list were valid. When
	// false, Motivo is a router reason code (probe_timeout, probe_failed,
	// probe_malformed, herdr_unavailable) and the lists are empty.
	OK     bool
	Motivo string
	// HerdrVersion is empty when `status server` failed or printed no
	// version.
	HerdrVersion string
	Workspaces   []PolledWorkspace
	Panes        []PolledPane
}
