package registry

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	_ "modernc.org/sqlite" // driver "sqlite"

	"github.com/djalmajr/herdr-hermes/internal/outbox"
)

// Store is one open registry database. All access goes through a single
// connection (SetMaxOpenConns(1)), so every in-process transaction is
// BEGIN IMMEDIATE and serialized inside the process; a second process on
// the same file is excluded the same way and waits out busy_timeout.
type Store struct {
	db           *sql.DB
	now          func() time.Time
	eventsPerJob int
	onlineWindow time.Duration
}

// ErrUnsafeState: the database or one of its state files is a symlink or
// non-regular file, or (outside Windows) carries group or other mode bits,
// or its parent directory allows group or other writes.
var ErrUnsafeState = errors.New("registry: unsafe state file")

// schemaDDL is the v1 schema, created in one transaction. Every *_unix
// column is the Unix seconds of its text column; comparisons use the unix
// columns, the text columns are what the API returns.
const schemaDDL = `
CREATE TABLE nodes(maquina TEXT PRIMARY KEY, ultimo_contato TEXT, ultimo_contato_unix INTEGER, ultimo_poll TEXT, poll_status TEXT, herdr_version TEXT, soho_version TEXT, runners TEXT, manutencao INTEGER NOT NULL DEFAULT 0, manutencao_motivo TEXT);
CREATE TABLE jobs(maquina TEXT NOT NULL, chave TEXT NOT NULL, origem TEXT NOT NULL, job_id TEXT, workspace TEXT, workspace_id TEXT, agente TEXT, agente_fonte TEXT, trabalho TEXT, ref TEXT, card TEXT, projeto TEXT, branch TEXT, pr TEXT, fase TEXT, herdr_status TEXT, terminal INTEGER NOT NULL DEFAULT 0, saida INTEGER, esperando_humano INTEGER NOT NULL DEFAULT 0, espera_motivo TEXT, ultimo_seq INTEGER NOT NULL DEFAULT 0, ultima_atividade TEXT, ultima_atividade_unix INTEGER, obsoleto INTEGER NOT NULL DEFAULT 0, fonte_push INTEGER NOT NULL DEFAULT 0, fonte_poll INTEGER NOT NULL DEFAULT 0, visto_poll TEXT, sessao_ts_unix INTEGER, encerrado INTEGER NOT NULL DEFAULT 0, encerrado_unix INTEGER, PRIMARY KEY(maquina, chave));
CREATE TABLE events(maquina TEXT NOT NULL, chave TEXT NOT NULL, seq INTEGER NOT NULL, ts TEXT NOT NULL, tipo TEXT NOT NULL, resumo TEXT NOT NULL, refs TEXT, PRIMARY KEY(maquina, chave, seq));
CREATE TABLE waits(maquina TEXT NOT NULL, pane TEXT NOT NULL, workspace_id TEXT, agente TEXT, desde TEXT NOT NULL, desde_unix INTEGER NOT NULL, PRIMARY KEY(maquina, pane));
CREATE TABLE claims(id TEXT PRIMARY KEY, agente TEXT NOT NULL, maquina TEXT, card TEXT, projeto TEXT, branch TEXT, pr TEXT, workspace TEXT, ref TEXT, criado TEXT NOT NULL, criado_unix INTEGER NOT NULL, expira TEXT NOT NULL, expira_unix INTEGER NOT NULL);
CREATE TABLE policies(escopo TEXT NOT NULL, valor TEXT NOT NULL, hold INTEGER NOT NULL, prioridade INTEGER, motivo TEXT, atualizado TEXT NOT NULL, PRIMARY KEY(escopo, valor));
CREATE TABLE folders(maquina TEXT NOT NULL, caminho TEXT NOT NULL, tamanho_bytes INTEGER, pedido TEXT NOT NULL, job TEXT, criado TEXT NOT NULL, PRIMARY KEY(maquina, caminho));
`

// dsnFor builds the file: URI of the path, percent-escaped per segment so
// a space or a ? in the path survives, with the fixed pragma set and the
// immediate transaction lock.
func dsnFor(path string) string {
	segments := strings.Split(filepath.ToSlash(path), "/")
	for i, seg := range segments {
		segments[i] = url.PathEscape(seg)
	}
	return "file:" + strings.Join(segments, "/") +
		"?_pragma=busy_timeout(10000)" +
		"&_pragma=journal_mode(WAL)" +
		"&_pragma=foreign_keys(ON)" +
		"&_pragma=synchronous(FULL)" +
		"&_txlock=immediate"
}

// Open opens (creating the parent dir, the file and the schema when
// missing) the registry database at path. A database written by a newer
// schema is refused with ErrSchema, and a database or sidecar that is a
// symlink or non-regular file, that carries group or other mode bits,
// or that sits in a parent directory writable by group or other is
// refused with ErrUnsafeState.
func Open(path string, opts Options) (*Store, error) {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	eventsPerJob := opts.EventsPerJob
	if eventsPerJob <= 0 {
		eventsPerJob = DefaultEventsPerJob
	}
	onlineWindow := opts.OnlineWindow
	if onlineWindow <= 0 {
		onlineWindow = DefaultOnlineWindow
	}

	if err := unsafeStateCheck(path); err != nil {
		return nil, err
	}
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, err
		}
	}
	// Pre-create the file at 0600 so the WAL and SHM files SQLite opens
	// alongside it inherit the same mode.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	f.Close()

	db, err := sql.Open("sqlite", dsnFor(path))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)

	var version int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		db.Close()
		return nil, err
	}
	switch {
	case version > SchemaVersion:
		db.Close()
		return nil, fmt.Errorf("%w: user_version %d", ErrSchema, version)
	case version == SchemaVersion:
		// Existing schema: use it as is.
	default:
		// A fresh database: create the whole schema, then bump the
		// version, in one transaction.
		txn, err := db.BeginTx(context.Background(), nil)
		if err != nil {
			db.Close()
			return nil, err
		}
		// Another process may have created the schema between the read
		// above and the immediate BEGIN; read the version again under
		// the write lock.
		if err := txn.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
			txn.Rollback()
			db.Close()
			return nil, err
		}
		if version != 0 {
			txn.Rollback()
			if version > SchemaVersion {
				db.Close()
				return nil, fmt.Errorf("%w: user_version %d", ErrSchema, version)
			}
			return &Store{db: db, now: now, eventsPerJob: eventsPerJob, onlineWindow: onlineWindow}, nil
		}
		if _, err := txn.Exec(schemaDDL); err != nil {
			txn.Rollback()
			db.Close()
			return nil, err
		}
		if _, err := txn.Exec(fmt.Sprintf("PRAGMA user_version = %d", SchemaVersion)); err != nil {
			txn.Rollback()
			db.Close()
			return nil, err
		}
		if err := txn.Commit(); err != nil {
			db.Close()
			return nil, err
		}
	}
	return &Store{db: db, now: now, eventsPerJob: eventsPerJob, onlineWindow: onlineWindow}, nil
}

// Close closes the database.
func (s *Store) Close() error {
	return s.db.Close()
}

// tx runs fn in one transaction, committing when fn returns nil and
// rolling back otherwise. Every mutation goes through it; the DSN's
// _txlock=immediate makes the BEGIN immediate.
func (s *Store) tx(fn func(*sql.Tx) error) error {
	txn, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	err = fn(txn)
	if err != nil {
		txn.Rollback()
		return err
	}
	return txn.Commit()
}

// admitNewNode refuses a node that would be the MaxNodes+1-th one.
func admitNewNode(tx *sql.Tx, maquina string) error {
	var exists int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM nodes WHERE maquina = ?`, maquina).Scan(&exists); err != nil {
		return err
	}
	if exists > 0 {
		return nil
	}
	var total int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM nodes`).Scan(&total); err != nil {
		return err
	}
	if total >= MaxNodes {
		return invalidErr("nodes", "limit reached")
	}
	return nil
}

// admitNewLiveJob refuses a live job row that would be the MaxJobs+1-th
// one. Polls are not capped.
func admitNewLiveJob(tx *sql.Tx) error {
	var live int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM jobs WHERE encerrado = 0`).Scan(&live); err != nil {
		return err
	}
	if live >= MaxJobs {
		return invalidErr("jobs", "limit reached")
	}
	return nil
}

// admitNewClaim refuses a claim that would be the MaxClaims+1-th live
// one, called after the expired claims are deleted.
func admitNewClaim(tx *sql.Tx) error {
	var live int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM claims`).Scan(&live); err != nil {
		return err
	}
	if live >= MaxClaims {
		return invalidErr("claims", "limit reached")
	}
	return nil
}

// admitNewPolicy refuses a policy that would be the MaxPolicies+1-th one;
// an update of an existing policy is not capped.
func admitNewPolicy(tx *sql.Tx, escopo, valor string) error {
	var exists int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM policies WHERE escopo = ? AND valor = ?`, escopo, valor).Scan(&exists); err != nil {
		return err
	}
	if exists > 0 {
		return nil
	}
	var total int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM policies`).Scan(&total); err != nil {
		return err
	}
	if total >= MaxPolicies {
		return invalidErr("policies", "limit reached")
	}
	return nil
}

// admitNewFolder refuses a folder record that would be the MaxFolders+1-th
// one; an update of an existing record is not capped.
func admitNewFolder(tx *sql.Tx, maquina, caminho string) error {
	var exists int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM folders WHERE maquina = ? AND caminho = ?`, maquina, caminho).Scan(&exists); err != nil {
		return err
	}
	if exists > 0 {
		return nil
	}
	var total int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM folders`).Scan(&total); err != nil {
		return err
	}
	if total >= MaxFolders {
		return invalidErr("folders", "limit reached")
	}
	return nil
}

// stamp is the store clock as a TSLayout string in the clock's own
// location (always a numeric offset, never Z) and its Unix seconds: the
// text column and its *_unix companion of one instant.
func (s *Store) stamp() (string, int64) {
	t := s.now()
	return t.Format(outbox.TSLayout), t.Unix()
}

// unsafeStateCheck runs before SQLite touches the path: the database and
// each existing sidecar must be regular files, and outside Windows none
// of them may carry group or other mode bits, nor may the parent
// directory allow group or other writes. The errors name the file and
// the reason, never its content.
func unsafeStateCheck(path string) error {
	type state struct {
		path string
		fi   os.FileInfo
	}
	var existing []state
	for _, p := range []string{path, path + "-wal", path + "-shm", path + "-journal"} {
		st, err := os.Lstat(p)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return err
		}
		if st.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: %s is a symlink", ErrUnsafeState, p)
		}
		if !st.Mode().IsRegular() {
			return fmt.Errorf("%w: %s is not a regular file", ErrUnsafeState, p)
		}
		existing = append(existing, state{path: p, fi: st})
	}
	if runtime.GOOS != "windows" {
		for _, st := range existing {
			if perm := st.fi.Mode().Perm() & 0o077; perm != 0 {
				return fmt.Errorf("%w: %s has group or other mode bits (%o)", ErrUnsafeState, st.path, perm)
			}
		}
		dir := filepath.Dir(path)
		if dir != "" {
			dst, err := os.Lstat(dir)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					return nil // MkdirAll creates it at 0700
				}
				return err
			}
			if perm := dst.Mode().Perm() & 0o022; perm != 0 {
				return fmt.Errorf("%w: %s allows group or other writes (%o)", ErrUnsafeState, dir, perm)
			}
		}
	}
	return nil
}
