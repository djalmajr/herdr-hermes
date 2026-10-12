package a2astore

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv/taskstore"

	"github.com/djalmajr/nau/internal/a2acore"
)

// Create stores a new task for the authenticated subject. A duplicate id
// is taskstore.ErrTaskAlreadyExists; a nil task, an empty id or an empty
// context id is a2a.ErrInvalidParams. The project and job come from the
// newest admission of the task (the SDK admits before it creates), or
// from the task metadata when the admission never committed; a task
// without a resolvable project is a2a.ErrInvalidParams. The held
// deliveries of the task become pending in the same transaction, so the
// notice is only ever sent after the task is durable.
func (s *Store) Create(ctx context.Context, task *a2a.Task) (taskstore.TaskVersion, error) {
	owner, err := s.owner(ctx)
	if err != nil {
		return 0, err
	}
	if task == nil || string(task.ID) == "" || task.ContextID == "" {
		return 0, a2a.ErrInvalidParams
	}
	err = s.tx(ctx, func(tx *sql.Tx) error {
		var n int
		if err := tx.QueryRow(`SELECT COUNT(*) FROM a2a_tasks WHERE task_id = ?`, string(task.ID)).Scan(&n); err != nil {
			return err
		}
		if n > 0 {
			return taskstore.ErrTaskAlreadyExists
		}
		project, job, err := s.projectJob(tx, string(task.ID), task)
		if err != nil {
			return err
		}
		body, err := json.Marshal(task)
		if err != nil {
			return fmt.Errorf("a2astore: task: %w", err)
		}
		now := s.now()
		created, createdUnix := stamp(now)
		if _, err := tx.Exec(`INSERT INTO a2a_tasks(task_id, context_id, owner, project, job, state, version, messages, task_json, created, created_unix, updated, updated_nano)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			string(task.ID), task.ContextID, owner, project, nullStr(job),
			NeutralState(task.Status.State), 1, len(task.History), string(body),
			created, createdUnix, created, now.UnixNano()); err != nil {
			return err
		}
		_, err = tx.Exec(`UPDATE a2a_deliveries SET state = ?, next_attempt_unix = ? WHERE task_id = ? AND state = ?`,
			a2acore.DeliveryPending, now.Unix(), string(task.ID), a2acore.DeliveryHeld)
		return err
	})
	if err != nil {
		return 0, err
	}
	return 1, nil
}

// projectJob resolves the task's project and job: the newest admission of
// the task when one committed, else the task metadata ("" project is a
// refusal, the new-task rule).
func (s *Store) projectJob(tx *sql.Tx, taskID string, task *a2a.Task) (string, string, error) {
	var project string
	var job sql.NullString
	err := tx.QueryRow(`SELECT project, job FROM a2a_ingress WHERE task_id = ? ORDER BY ingress_id DESC LIMIT 1`, taskID).
		Scan(&project, &job)
	if err == nil {
		return project, job.String, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", "", err
	}
	project = metadataString(task.Metadata, a2acore.MetaProject)
	if project == "" {
		return "", "", a2a.ErrInvalidParams
	}
	return project, metadataString(task.Metadata, a2acore.MetaJob), nil
}

// metadataString is the metadata value of key when it is a string.
func metadataString(meta map[string]any, key string) string {
	v, _ := meta[key].(string)
	return v
}

// Update stores a new version of the caller's task: the optimistic check
// on req.PrevVersion (TaskVersionMissing skips it), then, in the same
// transaction, the decision marked applied by the event and the terminal
// side effects. A missing task or another subject's task is
// a2a.ErrTaskNotFound; a stale version is taskstore.ErrConcurrentModification.
func (s *Store) Update(ctx context.Context, req *taskstore.UpdateRequest) (taskstore.TaskVersion, error) {
	if req == nil || req.Task == nil {
		return 0, a2a.ErrInvalidParams
	}
	owner, err := s.owner(ctx)
	if err != nil {
		return 0, err
	}
	var version taskstore.TaskVersion
	err = s.tx(ctx, func(tx *sql.Tx) error {
		var storedOwner string
		var storedVersion int
		err := tx.QueryRow(`SELECT owner, version FROM a2a_tasks WHERE task_id = ?`, string(req.Task.ID)).
			Scan(&storedOwner, &storedVersion)
		if errors.Is(err, sql.ErrNoRows) {
			return a2a.ErrTaskNotFound
		}
		if err != nil {
			return err
		}
		if storedOwner != owner {
			return a2a.ErrTaskNotFound
		}
		if req.PrevVersion != taskstore.TaskVersionMissing && int64(req.PrevVersion) != int64(storedVersion) {
			return taskstore.ErrConcurrentModification
		}
		body, err := json.Marshal(req.Task)
		if err != nil {
			return fmt.Errorf("a2astore: task: %w", err)
		}
		now := s.now()
		state := NeutralState(req.Task.Status.State)
		if _, err := tx.Exec(`UPDATE a2a_tasks SET task_json = ?, state = ?, version = ?, messages = ?, updated = ?, updated_nano = ? WHERE task_id = ?`,
			string(body), state, storedVersion+1, len(req.Task.History), now.Format(tsLayout), now.UnixNano(), string(req.Task.ID)); err != nil {
			return err
		}
		// The status event that applies a recorded decision marks it
		// applied in the same transaction as the task update.
		if ev, ok := req.Event.(*a2a.TaskStatusUpdateEvent); ok {
			if id, ok := ev.Metadata[a2acore.MetaDecision].(string); ok && id != "" {
				if _, err := tx.Exec(`UPDATE a2a_decisions SET state = ?, applied = ? WHERE decision_id = ? AND task_id = ? AND state = ?`,
					a2acore.DecisionApplied, now.Format(tsLayout), id, string(req.Task.ID), a2acore.DecisionPending); err != nil {
					return err
				}
			}
		}
		if a2acore.Terminal(state) {
			if err := s.terminalSideEffects(tx, string(req.Task.ID)); err != nil {
				return err
			}
		}
		version = taskstore.TaskVersion(storedVersion + 1)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return version, nil
}

// terminalSideEffects is the terminal task update of one transaction:
// the task's held or pending deliveries become obsolete and its other
// pending decisions are refused with terminal.
func (s *Store) terminalSideEffects(tx *sql.Tx, taskID string) error {
	if _, err := tx.Exec(`UPDATE a2a_deliveries SET state = ? WHERE task_id = ? AND state IN (?, ?)`,
		a2acore.DeliveryObsolete, taskID, a2acore.DeliveryHeld, a2acore.DeliveryPending); err != nil {
		return err
	}
	_, err := tx.Exec(`UPDATE a2a_decisions SET state = ?, refusal = ? WHERE task_id = ? AND state = ?`,
		a2acore.DecisionRefused, a2acore.RefuseTerminal, taskID, a2acore.DecisionPending)
	return err
}

// Get retrieves the caller's task as a deep copy: another subject's task
// and a missing one are a2a.ErrTaskNotFound.
func (s *Store) Get(ctx context.Context, id a2a.TaskID) (*taskstore.StoredTask, error) {
	return s.GetVisible(ctx, id, nil)
}

// GetVisible is Get restricted to the projects visible accepts: the
// project is the stored immutable a2a_tasks.project column, never the
// task JSON metadata. Owner masking comes first and is unchanged; a row
// whose project is not visible is the same a2a.ErrTaskNotFound as a
// missing or another subject's task. A nil visible is no project
// filter.
func (s *Store) GetVisible(ctx context.Context, id a2a.TaskID, visible func(project string) bool) (*taskstore.StoredTask, error) {
	owner, err := s.owner(ctx)
	if err != nil {
		return nil, err
	}
	var storedOwner, project string
	var version int
	var body []byte
	err = s.db.QueryRow(`SELECT owner, project, version, task_json FROM a2a_tasks WHERE task_id = ?`, string(id)).
		Scan(&storedOwner, &project, &version, &body)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, a2a.ErrTaskNotFound
	}
	if err != nil {
		return nil, err
	}
	if storedOwner != owner {
		return nil, a2a.ErrTaskNotFound
	}
	if visible != nil && !visible(project) {
		return nil, a2a.ErrTaskNotFound
	}
	task, err := unmarshalTask(body)
	if err != nil {
		return nil, err
	}
	return &taskstore.StoredTask{Task: task, Version: taskstore.TaskVersion(version), User: owner}, nil
}

// List retrieves the caller's tasks, newest update first (updated_nano,
// then task id, both descending): the filters are ContextID, Status (the
// SDK state) and StatusTimestampAfter (status timestamp at or after it);
// the page size is 1..100 (50 when zero, a2a.ErrInvalidRequest outside),
// the page token is the base64url of "<updated_nano>_<task_id>"
// (a2a.ErrParseError when it does not decode to that). HistoryLength nil
// keeps the last 100 messages, a value at or below zero keeps none,
// artifacts are dropped unless IncludeArtifacts. TotalSize is the
// filtered count.
func (s *Store) List(ctx context.Context, req *a2a.ListTasksRequest) (*a2a.ListTasksResponse, error) {
	return s.ListVisible(ctx, req, nil)
}

// ListVisible is List restricted to the projects visible accepts: the
// project is the stored immutable a2a_tasks.project column, never the
// task JSON metadata. Rows whose project is not visible are dropped
// before the StatusTimestampAfter filter, the TotalSize count and the
// page-token cut, so TotalSize, NextPageToken, history and artifacts
// never reflect them. A nil visible is no project filter.
func (s *Store) ListVisible(ctx context.Context, req *a2a.ListTasksRequest, visible func(project string) bool) (*a2a.ListTasksResponse, error) {
	if req == nil {
		return nil, a2a.ErrInvalidRequest
	}
	owner, err := s.owner(ctx)
	if err != nil || owner == "" {
		return nil, a2a.ErrUnauthenticated
	}
	pageSize := req.PageSize
	if pageSize == 0 {
		pageSize = 50
	}
	if pageSize < 1 || pageSize > 100 {
		return nil, fmt.Errorf("a2astore: page size must be between 1 and 100 inclusive, got %d: %w", pageSize, a2a.ErrInvalidRequest)
	}
	// A status with no neutral form (auth required) matches no stored
	// task: the neutral "" state is the unspecified one, not it.
	statusFilter := ""
	if req.Status != a2a.TaskStateUnspecified {
		statusFilter = NeutralState(req.Status)
		if statusFilter == "" {
			return &a2a.ListTasksResponse{Tasks: []*a2a.Task{}, PageSize: pageSize}, nil
		}
	}
	query := `SELECT task_id, task_json, updated_nano, project FROM a2a_tasks WHERE owner = ?`
	args := []any{owner}
	if req.ContextID != "" {
		query += ` AND context_id = ?`
		args = append(args, req.ContextID)
	}
	if statusFilter != "" {
		query += ` AND state = ?`
		args = append(args, statusFilter)
	}
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type storedRow struct {
		id   a2a.TaskID
		nano int64
		task *a2a.Task
	}
	var filtered []storedRow
	for rows.Next() {
		var id, project string
		var nano int64
		var body []byte
		if err := rows.Scan(&id, &body, &nano, &project); err != nil {
			return nil, err
		}
		if visible != nil && !visible(project) {
			continue
		}
		task, err := unmarshalTask(body)
		if err != nil {
			return nil, err
		}
		if req.StatusTimestampAfter != nil && task.Status.Timestamp != nil {
			if task.Status.Timestamp.Before(*req.StatusTimestampAfter) {
				continue
			}
		}
		filtered = append(filtered, storedRow{id: a2a.TaskID(id), nano: nano, task: task})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].nano != filtered[j].nano {
			return filtered[i].nano > filtered[j].nano
		}
		return filtered[i].id > filtered[j].id
	})
	totalSize := len(filtered)
	// The page starts strictly after the token's row in the sort order.
	if req.PageToken != "" {
		nano, cursorID, err := decodePageToken(req.PageToken)
		if err != nil {
			return nil, err
		}
		start := sort.Search(len(filtered), func(i int) bool {
			if filtered[i].nano != nano {
				return filtered[i].nano < nano
			}
			return filtered[i].id < cursorID
		})
		filtered = filtered[start:]
	}
	var nextPageToken string
	if pageSize < len(filtered) {
		last := filtered[pageSize-1]
		nextPageToken = encodePageToken(last.nano, string(last.id))
	}
	page := filtered
	if pageSize < len(page) {
		page = page[:pageSize]
	}
	tasks := make([]*a2a.Task, 0, len(page))
	for _, row := range page {
		task := row.task
		historyLength := 100
		if req.HistoryLength != nil {
			historyLength = *req.HistoryLength
		}
		if historyLength <= 0 {
			task.History = []*a2a.Message{}
		} else if len(task.History) > historyLength {
			task.History = task.History[len(task.History)-historyLength:]
		}
		if !req.IncludeArtifacts {
			task.Artifacts = nil
		}
		tasks = append(tasks, task)
	}
	return &a2a.ListTasksResponse{Tasks: tasks, TotalSize: totalSize, PageSize: pageSize, NextPageToken: nextPageToken}, nil
}

// encodePageToken is the base64url of "<updated_nano>_<task_id>".
func encodePageToken(nano int64, taskID string) string {
	return base64.URLEncoding.EncodeToString([]byte(fmt.Sprintf("%d_%s", nano, taskID)))
}

// decodePageToken is the inverse of encodePageToken; a token that does
// not decode to "<integer>_<task id>" is a2a.ErrParseError. The task id
// itself may carry underscores, so only the first one splits.
func decodePageToken(token string) (int64, a2a.TaskID, error) {
	decoded, err := base64.URLEncoding.DecodeString(token)
	if err != nil {
		return 0, "", a2a.ErrParseError
	}
	s := string(decoded)
	i := strings.IndexByte(s, '_')
	if i <= 0 || i == len(s)-1 {
		return 0, "", a2a.ErrParseError
	}
	nano, err := strconv.ParseInt(s[:i], 10, 64)
	if err != nil {
		return 0, "", a2a.ErrParseError
	}
	return nano, a2a.TaskID(s[i+1:]), nil
}
